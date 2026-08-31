//! Shared helper for running a child process that needs both piped stdin and
//! captured stdout/stderr.
//!
//! `Command::spawn()` followed by `write_all(stdin)` and then
//! `wait_with_output()` deadlocks once the child writes enough to stdout or
//! stderr to fill the OS pipe buffer (commonly 64KB) before it has consumed
//! all of stdin: the child blocks writing its output, so it stops reading
//! stdin, so the parent's `write_all` blocks on the full stdin pipe, and
//! neither side can make progress. `run_with_input` avoids this by writing
//! stdin from a dedicated thread while the caller's thread drains stdout and
//! stderr concurrently via `wait_with_output`.
//!
//! This module is the one sanctioned caller of `wait_with_output`; every
//! other call site should go through [`run_with_input`] instead (enforced by
//! the workspace's `disallowed-methods` clippy lint).
#![allow(
    clippy::disallowed_methods,
    reason = "this module exists to wrap wait_with_output safely"
)]

use std::io::{ErrorKind, Write};
use std::path::Path;
use std::process::{Command, Output, Stdio};

use anyhow::{Context, Result, anyhow};

/// Launch a long-lived background process with no inherited terminal streams.
/// The child is deliberately not waited here: it owns a singleton lock and
/// publishes its own endpoint, while callers confirm readiness over IPC.
pub fn spawn_detached(command: &mut Command, log_path: &Path) -> Result<u32> {
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("create detached process log directory {}", parent.display())
        })?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let log = options
        .open(log_path)
        .with_context(|| format!("open detached process log {}", log_path.display()))?;
    let stderr = log.try_clone().context("clone detached process log")?;
    command.stdin(Stdio::null()).stdout(log).stderr(stderr);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let child = command.spawn().context("spawn detached child process")?;
    Ok(child.id())
}

/// Run `command` with `input` written to its stdin, returning the captured
/// output.
///
/// Sets `command`'s stdin/stdout/stderr to piped, spawns it, writes `input`
/// to stdin from a separate thread (closing stdin when the write finishes or
/// fails), and drains stdout/stderr on the caller's thread via
/// `wait_with_output`. The writer thread is joined before returning, so a
/// write failure is never silently discarded -- except a broken pipe, which
/// just means the child exited (or closed stdin) before consuming all of
/// `input`; in that case the child's real exit status and stderr are more
/// useful to the caller than a generic I/O error, so the output is still
/// returned.
pub fn run_with_input(command: &mut Command, input: &[u8]) -> Result<Output> {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawn child process")?;

    let mut stdin = child.stdin.take().context("child stdin is missing")?;
    let input = input.to_vec();
    let writer = std::thread::spawn(move || -> std::io::Result<()> {
        let result = stdin.write_all(&input);
        // Close stdin whether or not the write succeeded, so a child that is
        // blocked reading stdin (e.g. waiting for EOF) can proceed.
        drop(stdin);
        result
    });

    let output = child.wait_with_output().context("wait for child process")?;

    match writer.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) if error.kind() == ErrorKind::BrokenPipe => {
            // The child exited, or otherwise stopped reading, before we
            // finished writing. Report the child's actual status/stderr
            // instead of this expected write failure.
        }
        Ok(Err(error)) => return Err(error).context("write child process stdin"),
        Err(panic) => {
            return Err(anyhow!(
                "child process stdin writer thread panicked: {panic:?}"
            ));
        }
    }

    Ok(output)
}

/// Send `signal` to the process group led by `pid`.
///
/// Every caller signals a group it created for its own child, and wants that
/// group gone. `ESRCH` means it already is, so it reports success rather than
/// a failure. Darwin excludes zombies while counting signalable members of a
/// group and returns `EPERM` once that count reaches zero, which for a group
/// we own likewise means only exiting descendants remain. Any other error is
/// a real teardown failure and is returned so the caller can report it.
#[cfg(unix)]
pub fn signal_process_group(pid: i32, signal: i32) -> std::io::Result<()> {
    // SAFETY: the negated pid targets only the process group this process
    // created for its own child.
    if unsafe { libc::kill(-pid, signal) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if group_signal_error_is_ignorable(&error) {
        return Ok(());
    }
    Err(error)
}

#[cfg(unix)]
fn group_signal_error_is_ignorable(error: &std::io::Error) -> bool {
    if error.raw_os_error() == Some(libc::ESRCH) {
        return true;
    }
    #[cfg(target_os = "macos")]
    if error.raw_os_error() == Some(libc::EPERM) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn run_with_input_completes_when_child_echoes_input_larger_than_pipe_buffer() {
        // `cat` echoes stdin to stdout; feeding it well past the typical 64KB
        // pipe buffer reproduces the old deadlock (parent blocked in
        // write_all while the child blocks writing stdout that nobody is
        // draining yet) unless stdin is fed concurrently with output drain.
        let input = vec![b'x'; 512 * 1024];
        let mut command = Command::new("sh");
        command.arg("-c").arg("cat");

        let output = run_with_input(&mut command, &input)
            .expect("run_with_input should not deadlock or fail");

        assert!(output.status.success());
        assert_eq!(output.stdout, input);
    }

    #[cfg(unix)]
    #[test]
    fn run_with_input_reports_child_status_when_child_exits_before_reading_all_input() {
        // The child exits immediately without reading stdin, so the writer
        // thread hits a broken pipe partway through writing. That must not
        // surface as a generic write error; the caller should still see the
        // child's real exit status.
        let input = vec![b'x'; 512 * 1024];
        let mut command = Command::new("sh");
        command.arg("-c").arg("exit 3");

        let output = run_with_input(&mut command, &input)
            .expect("a broken pipe from an early exit must not be a hard error");

        assert_eq!(output.status.code(), Some(3));
    }

    #[test]
    fn run_with_input_returns_output_for_empty_input() {
        let mut command = Command::new("true");
        let output = run_with_input(&mut command, &[]).expect("run_with_input should succeed");
        assert!(output.status.success());
    }

    #[cfg(unix)]
    #[test]
    fn signalling_a_group_that_is_already_gone_succeeds() {
        // Cancelling a command whose child already exited is the common case;
        // it must not look like a teardown failure.
        use std::os::unix::process::CommandExt as _;

        let mut command = Command::new("sh");
        command.arg("-c").arg("exit 0");
        command.process_group(0);
        let mut child = command.spawn().expect("spawn short-lived child");
        let pid = child.id() as i32;
        child.wait().expect("reap short-lived child");

        signal_process_group(pid, libc::SIGKILL)
            .expect("signalling an already-exited process group must succeed");
    }

    #[cfg(unix)]
    #[test]
    fn signalling_a_live_group_reports_a_real_failure() {
        // An invalid signal number is a caller bug, not a group that already
        // exited, so it must surface instead of being swallowed.
        use std::os::unix::process::CommandExt as _;

        let mut command = Command::new("sleep");
        command.arg("30");
        command.process_group(0);
        let mut child = command.spawn().expect("spawn long-lived child");
        let pid = child.id() as i32;

        let error =
            signal_process_group(pid, 1234).expect_err("an invalid signal number must be reported");
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));

        signal_process_group(pid, libc::SIGKILL).expect("terminate the test child");
        child.wait().expect("reap long-lived child");
    }

    #[cfg(unix)]
    #[test]
    fn group_signal_error_only_ignores_a_gone_owned_group() {
        let missing = std::io::Error::from_raw_os_error(libc::ESRCH);
        assert!(group_signal_error_is_ignorable(&missing));

        let invalid = std::io::Error::from_raw_os_error(libc::EINVAL);
        assert!(!group_signal_error_is_ignorable(&invalid));

        let denied = std::io::Error::from_raw_os_error(libc::EPERM);
        #[cfg(target_os = "macos")]
        assert!(group_signal_error_is_ignorable(&denied));
        #[cfg(not(target_os = "macos"))]
        assert!(!group_signal_error_is_ignorable(&denied));
    }
}
