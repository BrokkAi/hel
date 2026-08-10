//! Controller-side client for a target worker's JSON-lines proxy.

use std::process::Stdio;

use anyhow::{Context, Result, anyhow, bail};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::hel_targets::CommandSpec;
use crate::hel_worker::{
    Attachment, MAX_FRAME_BYTES, PROTOCOL_VERSION, RequestEnvelope, ResponseBody, ResponseEnvelope,
    ResponsePayload, SequencedEvent, VersionRange, WorkerRequest, WorkerSnapshot, WorkerStatus,
};

/// A live stdio connection to `hel worker proxy` on a session target.
///
/// Dropping this value kills only the short-lived proxy process. The detached
/// target worker and its ACP harness continue running.
pub struct WorkerClient {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    next_request: u64,
    /// Random per-connection component of request IDs. The worker's
    /// idempotency ledger outlives connections, so a counter alone would
    /// collide when the same controller process reconnects.
    connection_nonce: u64,
    protocol_version: u32,
    session_id: String,
    latest_seq: u64,
}

#[derive(Debug, Clone)]
pub struct WorkerBootstrap {
    pub snapshot: WorkerSnapshot,
    pub events: Vec<SequencedEvent>,
}

impl WorkerClient {
    /// Spawn a reconnect command, negotiate the protocol, and verify that the
    /// proxy reached the expected session.
    pub async fn connect(spec: &CommandSpec, expected_session_id: &str) -> Result<Self> {
        let mut child = Command::new(&spec.program)
            .args(&spec.args)
            .envs(&spec.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("start worker proxy for {}", spec.purpose))?;
        let input = child
            .stdin
            .take()
            .context("worker proxy stdin unavailable")?;
        let output = child
            .stdout
            .take()
            .context("worker proxy stdout unavailable")?;
        let mut nonce_bytes = [0_u8; 8];
        getrandom::fill(&mut nonce_bytes)
            .map_err(|error| anyhow!("generate worker request nonce: {error}"))?;
        let mut client = Self {
            child,
            input,
            output: BufReader::new(output).lines(),
            next_request: 1,
            connection_nonce: u64::from_le_bytes(nonce_bytes),
            protocol_version: PROTOCOL_VERSION,
            session_id: String::new(),
            latest_seq: 0,
        };

        let response = client
            .call(WorkerRequest::Hello {
                client_version: env!("CARGO_PKG_VERSION").to_owned(),
                supported: VersionRange::CURRENT,
            })
            .await?;
        let ResponsePayload::Hello {
            negotiated,
            session_id,
            ..
        } = response
        else {
            bail!("worker returned an unexpected hello response")
        };
        if session_id != expected_session_id {
            bail!("worker belongs to session {session_id}, not {expected_session_id}");
        }
        client.protocol_version = negotiated;
        client.session_id = session_id;
        Ok(client)
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn latest_seq(&self) -> u64 {
        self.latest_seq
    }

    /// Fetch a coherent snapshot and the complete canonical transcript.
    pub async fn bootstrap(&mut self) -> Result<WorkerBootstrap> {
        let snapshot = self.snapshot().await?;
        let events = self.replay_after(0).await?;
        Ok(WorkerBootstrap { snapshot, events })
    }

    pub async fn status(&mut self) -> Result<WorkerStatus> {
        match self.call(WorkerRequest::Status).await? {
            ResponsePayload::Status(status) => Ok(status),
            _ => bail!("worker returned an unexpected status response"),
        }
    }

    pub async fn snapshot(&mut self) -> Result<WorkerSnapshot> {
        match self.call(WorkerRequest::Snapshot).await? {
            ResponsePayload::Snapshot(snapshot) => Ok(snapshot),
            _ => bail!("worker returned an unexpected snapshot response"),
        }
    }

    pub async fn replay_after(&mut self, after_seq: u64) -> Result<Vec<SequencedEvent>> {
        match self.call(WorkerRequest::Subscribe { after_seq }).await? {
            ResponsePayload::Replay { events, latest_seq } => {
                self.latest_seq = self.latest_seq.max(latest_seq);
                Ok(events)
            }
            _ => bail!("worker returned an unexpected replay response"),
        }
    }

    /// Poll for events emitted after the most recently observed sequence.
    pub async fn sync(&mut self) -> Result<Vec<SequencedEvent>> {
        self.replay_after(self.latest_seq).await
    }

    pub async fn prompt(&mut self, text: String, attachments: Vec<Attachment>) -> Result<u64> {
        self.accepted(WorkerRequest::Prompt { text, attachments })
            .await
    }

    pub async fn cancel(&mut self) -> Result<u64> {
        self.accepted(WorkerRequest::Cancel).await
    }

    pub async fn checkpoint(&mut self, reason: Option<String>) -> Result<u64> {
        self.accepted(WorkerRequest::Checkpoint { reason }).await
    }

    pub async fn close(&mut self) -> Result<u64> {
        self.accepted(WorkerRequest::Close).await
    }

    /// Disconnect the transport without closing the target worker or harness.
    pub async fn detach(mut self) -> Result<()> {
        self.input
            .shutdown()
            .await
            .context("close worker proxy stdin")?;
        match tokio::time::timeout(std::time::Duration::from_millis(500), self.child.wait()).await {
            Ok(status) => {
                status.context("wait for worker proxy")?;
            }
            Err(_) => {
                self.child.start_kill().context("stop worker proxy")?;
                let _ = self.child.wait().await;
            }
        }
        Ok(())
    }

    async fn accepted(&mut self, request: WorkerRequest) -> Result<u64> {
        match self.call(request).await? {
            ResponsePayload::Accepted { seq } => Ok(seq),
            _ => bail!("worker returned an unexpected mutation response"),
        }
    }

    async fn call(&mut self, request: WorkerRequest) -> Result<ResponsePayload> {
        let request_id = self.request_id();
        let envelope = RequestEnvelope {
            request_id: request_id.clone(),
            protocol_version: self.protocol_version,
            request,
        };
        let mut frame = serde_json::to_vec(&envelope)?;
        if frame.len() > MAX_FRAME_BYTES {
            bail!("worker request frame is too large");
        }
        frame.push(b'\n');
        self.input
            .write_all(&frame)
            .await
            .context("write worker request")?;
        self.input.flush().await.context("flush worker request")?;

        let line = self
            .output
            .next_line()
            .await
            .context("read worker response")?
            .ok_or_else(|| anyhow!("worker proxy disconnected"))?;
        if line.len() > MAX_FRAME_BYTES {
            bail!("worker response frame is too large");
        }
        decode_response(&line, &request_id, self.protocol_version)
    }

    fn request_id(&mut self) -> String {
        let id = format!(
            "hel-{:016x}-{}",
            self.connection_nonce, self.next_request
        );
        self.next_request = self.next_request.wrapping_add(1);
        id
    }
}

fn decode_response(line: &str, request_id: &str, protocol: u32) -> Result<ResponsePayload> {
    let response: ResponseEnvelope =
        serde_json::from_str(line).context("decode worker response")?;
    if response.request_id != request_id {
        bail!(
            "worker response ID mismatch: expected {request_id}, got {}",
            response.request_id
        );
    }
    if response.protocol_version != protocol {
        bail!(
            "worker response protocol mismatch: expected {protocol}, got {}",
            response.protocol_version
        );
    }
    match response.body {
        ResponseBody::Ok { payload } => Ok(payload),
        ResponseBody::Error { error } => Err(anyhow!(
            "worker rejected request ({:?}): {}",
            error.code,
            error.message
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_worker::{ErrorCode, ProtocolError};

    #[test]
    fn command_spec_preserves_argv_boundaries() {
        let spec = CommandSpec::new("ssh", ["host", "hel worker proxy --root '/odd path'"]);
        assert_eq!(spec.program, "ssh");
        assert_eq!(spec.args.len(), 2);
        assert_eq!(spec.args[1], "hel worker proxy --root '/odd path'");
    }

    #[test]
    fn protocol_version_range_contains_current_version() {
        assert_eq!(
            VersionRange::CURRENT.negotiate(VersionRange::CURRENT),
            Some(PROTOCOL_VERSION)
        );
    }

    #[test]
    fn response_decoder_rejects_crossed_request_ids() {
        let response = ResponseEnvelope {
            request_id: "other".into(),
            protocol_version: PROTOCOL_VERSION,
            body: ResponseBody::Ok {
                payload: ResponsePayload::Accepted { seq: 1 },
            },
        };
        let encoded = serde_json::to_string(&response).unwrap();
        let error = decode_response(&encoded, "wanted", PROTOCOL_VERSION).unwrap_err();
        assert!(error.to_string().contains("ID mismatch"));
    }

    #[test]
    fn response_decoder_surfaces_worker_errors() {
        let response = ResponseEnvelope {
            request_id: "r1".into(),
            protocol_version: PROTOCOL_VERSION,
            body: ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::InvalidState,
                    message: "busy".into(),
                    retryable: false,
                },
            },
        };
        let encoded = serde_json::to_string(&response).unwrap();
        let error = decode_response(&encoded, "r1", PROTOCOL_VERSION).unwrap_err();
        assert!(error.to_string().contains("busy"));
    }
}
