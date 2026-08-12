//! Authenticated, path-confined Git smart-protocol bridge for local bundles.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;

use crate::hel_local_git::canonical_repository;
use crate::hel_targets::CommandSpec;

const BRIDGE_MAGIC: &[u8] = b"HEL-GIT-BRIDGE-1\n";
const MAX_FRAME: usize = 1024 * 1024;
const MAX_OPEN: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitBrokerSpec {
    pub session_id: String,
    pub bridge: CommandSpec,
    pub repositories: BTreeMap<String, PathBuf>,
    pub ready_path: PathBuf,
    pub pid_path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GitOpen {
    repository: String,
    service: String,
}

impl GitBrokerSpec {
    pub fn write(&self, path: &Path) -> Result<()> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        crate::hel_config::atomic_write(path, &serde_json::to_vec_pretty(self)?)
    }

    pub fn read(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("read Git broker spec {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parse Git broker spec {}", path.display()))
    }
}

#[cfg(unix)]
pub fn broker_is_alive(pid_path: &Path) -> bool {
    let Ok(pid) = std::fs::read_to_string(pid_path) else {
        return false;
    };
    let Ok(pid) = pid.trim().parse::<i32>() else {
        return false;
    };
    // Signal zero performs existence/permission checking without changing the process.
    unsafe { libc::kill(pid, 0) == 0 }
}

#[cfg(not(unix))]
pub fn broker_is_alive(_pid_path: &Path) -> bool {
    false
}

pub async fn run_broker(spec_path: &Path) -> Result<()> {
    let spec = GitBrokerSpec::read(spec_path)?;
    let repositories = spec
        .repositories
        .iter()
        .map(|(id, path)| Ok((id.clone(), canonical_repository(path)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    if let Some(parent) = spec.ready_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&spec.pid_path, std::process::id().to_string())?;
    let result = run_bridge_process(&spec, repositories).await;
    let _ = std::fs::remove_file(&spec.ready_path);
    let _ = std::fs::remove_file(&spec.pid_path);
    result
}

async fn run_bridge_process(
    spec: &GitBrokerSpec,
    repositories: BTreeMap<String, PathBuf>,
) -> Result<()> {
    let mut child = Command::new(&spec.bridge.program)
        .args(&spec.bridge.args)
        .envs(&spec.bridge.env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("start {}", spec.bridge.purpose))?;
    let mut input = child.stdin.take().context("Git bridge stdin is missing")?;
    let mut output = child
        .stdout
        .take()
        .context("Git bridge stdout is missing")?;
    let mut magic = vec![0; BRIDGE_MAGIC.len()];
    output
        .read_exact(&mut magic)
        .await
        .context("read Git bridge greeting")?;
    ensure!(magic == BRIDGE_MAGIC, "target Git bridge version mismatch");
    crate::hel_config::atomic_write(&spec.ready_path, b"ready\n")?;

    loop {
        let Some(open) = read_frame_optional(&mut output, MAX_OPEN).await? else {
            break;
        };
        let open: GitOpen = serde_json::from_slice(&open).context("decode Git bridge request")?;
        let repository = repositories.get(&open.repository).with_context(|| {
            format!(
                "Git bridge requested unknown repository {:?}",
                open.repository
            )
        })?;
        serve_git(&mut input, &mut output, repository, &open.service).await?;
    }
    let status = child.wait().await.context("wait for target Git bridge")?;
    ensure!(status.success(), "target Git bridge exited with {status}");
    Ok(())
}

async fn serve_git<W, R>(
    bridge_input: &mut W,
    bridge_output: &mut R,
    repository: &Path,
    service: &str,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let command = match service {
        "git-upload-pack" => "upload-pack",
        "git-receive-pack" => "receive-pack",
        _ => bail!("unsupported Git service {service:?}"),
    };
    let mut git = Command::new("git");
    git.args([
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "receive.denyCurrentBranch=updateInstead",
        "-c",
        "receive.denyNonFastForwards=true",
        "-c",
        "receive.denyDeletes=true",
        command,
    ]);
    git.arg(repository)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut git = git
        .spawn()
        .with_context(|| format!("start git {command}"))?;
    let mut git_input = git.stdin.take().context("Git service stdin is missing")?;
    let mut git_output = git.stdout.take().context("Git service stdout is missing")?;

    let from_target = async {
        while let Some(frame) = read_frame_optional(bridge_output, MAX_FRAME).await? {
            git_input.write_all(&frame).await?;
        }
        git_input.shutdown().await?;
        Ok::<(), anyhow::Error>(())
    };
    let to_target = async {
        let mut buffer = vec![0; 64 * 1024];
        loop {
            let count = git_output.read(&mut buffer).await?;
            if count == 0 {
                write_frame(bridge_input, &[]).await?;
                break;
            }
            write_frame(bridge_input, &buffer[..count]).await?;
        }
        Ok::<(), anyhow::Error>(())
    };
    tokio::pin!(from_target);
    tokio::pin!(to_target);
    tokio::select! {
        result = &mut to_target => result?,
        result = &mut from_target => {
            result?;
            to_target.await?;
        }
    }
    let status = git.wait().await.context("wait for Git service")?;
    ensure!(status.success(), "git {command} exited with {status}");
    Ok(())
}

async fn write_frame(writer: &mut (impl AsyncWrite + Unpin), data: &[u8]) -> Result<()> {
    ensure!(data.len() <= MAX_FRAME, "Git bridge frame is too large");
    writer.write_u32(data.len() as u32).await?;
    writer.write_all(data).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_frame_optional(
    reader: &mut (impl AsyncRead + Unpin),
    maximum: usize,
) -> Result<Option<Vec<u8>>> {
    let length = match reader.read_u32().await {
        Ok(length) => length as usize,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if length == 0 {
        return Ok(None);
    }
    ensure!(length <= maximum, "Git bridge frame is too large");
    let mut data = vec![0; length];
    reader.read_exact(&mut data).await?;
    Ok(Some(data))
}

#[cfg(unix)]
pub async fn run_worker_bridge(root: &Path) -> Result<()> {
    use tokio::net::UnixListener;

    std::fs::create_dir_all(root)?;
    let socket = root.join("git.sock");
    if socket.exists() {
        let _ = std::fs::remove_file(&socket);
    }
    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("bind Git proxy socket {}", socket.display()))?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    stdout.write_all(BRIDGE_MAGIC).await?;
    stdout.flush().await?;
    loop {
        let (stream, _) = listener.accept().await.context("accept Git proxy client")?;
        let (mut read, mut write) = stream.into_split();
        let open = read_handshake(&mut read).await?;
        write_frame(&mut stdout, &open).await?;
        let from_git = async {
            let mut buffer = vec![0; 64 * 1024];
            loop {
                let count = read.read(&mut buffer).await?;
                if count == 0 {
                    write_frame(&mut stdout, &[]).await?;
                    break;
                }
                write_frame(&mut stdout, &buffer[..count]).await?;
            }
            Ok::<(), anyhow::Error>(())
        };
        let to_git = async {
            while let Some(frame) = read_frame_optional(&mut stdin, MAX_FRAME).await? {
                write.write_all(&frame).await?;
            }
            write.shutdown().await?;
            Ok::<(), anyhow::Error>(())
        };
        tokio::pin!(from_git);
        tokio::pin!(to_git);
        tokio::select! {
            result = &mut to_git => result?,
            result = &mut from_git => {
                result?;
                to_git.await?;
            }
        }
    }
}

#[cfg(unix)]
pub async fn run_worker_proxy(root: &Path, repository: &str, service: &str) -> Result<()> {
    use tokio::net::UnixStream;

    crate::hel_config::validate_id("repository", repository)?;
    ensure!(
        matches!(service, "git-upload-pack" | "git-receive-pack"),
        "unsupported Git service"
    );
    let mut socket = UnixStream::connect(root.join("git.sock"))
        .await
        .with_context(|| format!("connect Git bridge at {}", root.display()))?;
    let open = serde_json::to_vec(&GitOpen {
        repository: repository.into(),
        service: service.into(),
    })?;
    socket.write_all(&open).await?;
    socket.write_all(b"\n").await?;
    let (mut socket_read, mut socket_write) = socket.into_split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let from_git = async {
        tokio::io::copy(&mut stdin, &mut socket_write).await?;
        socket_write.shutdown().await
    };
    let to_git = async {
        tokio::io::copy(&mut socket_read, &mut stdout).await?;
        stdout.shutdown().await
    };
    tokio::pin!(from_git);
    tokio::pin!(to_git);
    tokio::select! {
        result = &mut to_git => {
            result?;
        }
        result = &mut from_git => {
            result?;
            to_git.await?;
        }
    }
    Ok(())
}

#[cfg(unix)]
async fn read_handshake(reader: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    loop {
        ensure!(data.len() < MAX_OPEN, "Git proxy handshake is too large");
        let byte = reader.read_u8().await?;
        if byte == b'\n' {
            break;
        }
        data.push(byte);
    }
    Ok(data)
}

#[cfg(not(unix))]
pub async fn run_worker_bridge(_root: &Path) -> Result<()> {
    bail!("Git proxy workers require Unix")
}

#[cfg(not(unix))]
pub async fn run_worker_proxy(_root: &Path, _repository: &str, _service: &str) -> Result<()> {
    bail!("Git proxy workers require Unix")
}
