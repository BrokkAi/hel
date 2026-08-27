//! Durable, non-blocking diagnostics for controller-facing Hel processes.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

const RETAINED_LOGS: usize = 10;
const LOG_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) struct ControllerLog {
    _writer_guard: ReliableWorkerGuard,
}

/// A non-blocking logger with reliable delivery to its writer thread. The
/// `tracing_appender` default is bounded and lossy, which can discard a fatal
/// error when a controller emits a burst of diagnostics. An unbounded standard
/// channel keeps the UI call site free of filesystem I/O while retaining every
/// record until the worker writes it or the process exits.
#[derive(Clone)]
struct ReliableWriter {
    sender: Sender<LogMessage>,
}

enum LogMessage {
    Line(Vec<u8>),
    Shutdown,
}

struct ReliableWorkerGuard {
    sender: Option<Sender<LogMessage>>,
    worker: Option<JoinHandle<()>>,
    completed: Receiver<()>,
}

impl Write for ReliableWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let length = bytes.len();
        self.sender
            .send(LogMessage::Line(bytes.to_vec()))
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "log worker stopped")
            })?;
        Ok(length)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for ReliableWriter {
    type Writer = ReliableWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Drop for ReliableWorkerGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take()
            && sender.send(LogMessage::Shutdown).is_err()
        {
            eprintln!("Hel log writer shutdown signal could not be delivered");
            if let Some(worker) = self.worker.take()
                && let Err(error) = worker.join()
            {
                eprintln!("Hel log writer panicked: {error:?}");
            }
            return;
        }
        // A failed filesystem must not make quitting the dashboard hang. Give
        // the writer a bounded chance to drain and flush, then detach it; the
        // operating system will close the file when the process exits.
        match self.completed.recv_timeout(LOG_SHUTDOWN_TIMEOUT) {
            Ok(()) => {
                if let Some(worker) = self.worker.take()
                    && let Err(error) = worker.join()
                {
                    eprintln!("Hel log writer panicked: {error:?}");
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                eprintln!("Hel log writer did not drain before shutdown timeout");
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                eprintln!("Hel log writer completion channel closed unexpectedly");
            }
        }
    }
}

fn reliable_non_blocking(file: File) -> Result<(ReliableWriter, ReliableWorkerGuard)> {
    let (sender, receiver) = mpsc::channel();
    let (completed_tx, completed) = mpsc::sync_channel(1);
    let worker = thread::Builder::new()
        .name("hel-log-writer".into())
        .spawn(move || {
            write_log_messages(file, receiver);
            if completed_tx.send(()).is_err() {
                eprintln!("Hel log writer completion could not be reported");
            }
        })
        .context("spawn Hel log writer")?;
    let writer = ReliableWriter {
        sender: sender.clone(),
    };
    let guard = ReliableWorkerGuard {
        sender: Some(sender),
        worker: Some(worker),
        completed,
    };
    Ok((writer, guard))
}

fn write_log_messages(mut file: File, receiver: mpsc::Receiver<LogMessage>) {
    while let Ok(message) = receiver.recv() {
        match message {
            LogMessage::Line(line) => {
                if let Err(error) = file.write_all(&line) {
                    eprintln!("Hel log writer failed: {error}");
                    break;
                }
            }
            LogMessage::Shutdown => break,
        }
    }
    if let Err(error) = file.flush() {
        eprintln!("Hel log writer failed to flush: {error}");
    }
}

impl ControllerLog {
    pub(crate) fn start(command: &'static str) -> Result<Self> {
        let directory = hel::hel_config::data_dir().join("logs");
        fs::create_dir_all(&directory)
            .with_context(|| format!("create Hel log directory {}", directory.display()))?;
        prune_logs(&directory, RETAINED_LOGS.saturating_sub(1))?;

        let path = directory.join(log_filename());
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .with_context(|| format!("create Hel log {}", path.display()))?;
        let (writer, writer_guard) = reliable_non_blocking(file)?;
        let (filter, filter_error) = env_filter("info");
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_env_filter(filter)
            .with_writer(writer)
            .try_init()
            .map_err(|error| anyhow::anyhow!("install Hel log subscriber: {error}"))?;

        tracing::info!(
            version = env!("CARGO_PKG_VERSION"),
            process_id = std::process::id(),
            command,
            log = %path.display(),
            "Hel started"
        );
        if let Some(error) = filter_error {
            tracing::warn!(%error, "ignored invalid RUST_LOG filter");
        }
        Ok(Self {
            _writer_guard: writer_guard,
        })
    }
}

pub(crate) fn start_stderr() -> Result<()> {
    let (filter, filter_error) = env_filter("warn");
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("install Hel stderr subscriber: {error}"))?;
    if let Some(error) = filter_error {
        tracing::warn!(%error, "ignored invalid RUST_LOG filter");
    }
    Ok(())
}

fn env_filter(default: &str) -> (EnvFilter, Option<String>) {
    match std::env::var("RUST_LOG") {
        Ok(value) => match EnvFilter::try_new(value) {
            Ok(filter) => (filter, None),
            Err(error) => (EnvFilter::new(default), Some(error.to_string())),
        },
        Err(std::env::VarError::NotPresent) => (EnvFilter::new(default), None),
        Err(error @ std::env::VarError::NotUnicode(_)) => {
            (EnvFilter::new(default), Some(error.to_string()))
        }
    }
}

fn log_filename() -> String {
    format!(
        "hel-{}-{}.log",
        Utc::now().format("%Y%m%dT%H%M%S%.3fZ"),
        std::process::id()
    )
}

fn prune_logs(directory: &Path, retain: usize) -> Result<()> {
    let mut logs = Vec::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read Hel log directory {}", directory.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", directory.display()))?;
        if let Some(path) = {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            (name.starts_with("hel-") && name.ends_with(".log")).then_some(entry.path())
        } {
            logs.push(path);
        }
    }
    logs.sort_unstable();
    let remove = logs.len().saturating_sub(retain);
    for path in logs.into_iter().take(remove) {
        fs::remove_file(&path)
            .with_context(|| format!("remove expired Hel log {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reliable_writer_queues_every_line_without_dropping_a_burst() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("burst.log");
        let file = File::create(&path).unwrap();
        let (mut writer, guard) = reliable_non_blocking(file).unwrap();
        for line in 0..10_000 {
            writer
                .write_all(format!("error {line}\n").as_bytes())
                .unwrap();
        }
        drop(writer);
        drop(guard);

        let contents = fs::read_to_string(path).unwrap();
        assert_eq!(contents.lines().count(), 10_000);
        assert!(contents.ends_with("error 9999\n"));
    }

    #[test]
    fn prune_logs_keeps_newest_managed_logs_and_unrelated_files() {
        let directory = tempfile::tempdir().unwrap();
        for name in [
            "hel-20260824T000000.000Z-1.log",
            "hel-20260825T000000.000Z-2.log",
            "hel-20260826T000000.000Z-3.log",
            "notes.log",
        ] {
            fs::write(directory.path().join(name), name).unwrap();
        }

        prune_logs(directory.path(), 2).unwrap();

        assert!(
            !directory
                .path()
                .join("hel-20260824T000000.000Z-1.log")
                .exists()
        );
        assert!(
            directory
                .path()
                .join("hel-20260825T000000.000Z-2.log")
                .exists()
        );
        assert!(
            directory
                .path()
                .join("hel-20260826T000000.000Z-3.log")
                .exists()
        );
        assert!(directory.path().join("notes.log").exists());
    }
}
