//! Controller-side client for a target worker's JSON-lines proxy.

use std::process::Stdio;

use anyhow::{Context, Result, anyhow, bail};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::hel_targets::CommandSpec;
use crate::hel_worker::{
    Attachment, MAX_FRAME_BYTES, PROTOCOL_VERSION, RequestEnvelope, ResponseBody, ResponseEnvelope,
    ResponsePayload, SequencedEvent, VersionRange, WorkerRequest, WorkerSessionSummary,
    WorkerSnapshot, WorkerStatus,
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
    worker_version: String,
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
            worker_version: String::new(),
            latest_seq: 0,
        };

        let response = client
            .call_hello(WorkerRequest::Hello {
                client_version: env!("CARGO_PKG_VERSION").to_owned(),
                supported: VersionRange::CURRENT,
            })
            .await?;
        let ResponsePayload::Hello {
            negotiated,
            worker_version,
            session_id,
        } = response
        else {
            bail!("worker returned an unexpected hello response")
        };
        if session_id != expected_session_id {
            bail!("worker belongs to session {session_id}, not {expected_session_id}");
        }
        if VersionRange::CURRENT.negotiate(VersionRange {
            min: negotiated,
            max: negotiated,
        }) != Some(negotiated)
        {
            bail!("worker negotiated unsupported protocol {negotiated}");
        }
        client.protocol_version = negotiated;
        client.session_id = session_id;
        client.worker_version = worker_version;
        Ok(client)
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn latest_seq(&self) -> u64 {
        self.latest_seq
    }

    pub fn worker_version(&self) -> &str {
        &self.worker_version
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// Lightweight fail-on-use guard for future UI operations. New methods
    /// declare only their minimum protocol; no capability matrix is needed.
    pub fn require_protocol(&self, operation: &str, minimum: u32) -> Result<()> {
        if self.protocol_version < minimum {
            bail!(
                "{operation} requires worker protocol {minimum}, but worker {} negotiated protocol {}; update or restart that worker to use this operation",
                self.worker_version,
                self.protocol_version
            );
        }
        Ok(())
    }

    /// Fetch a coherent snapshot and the complete canonical transcript.
    pub async fn bootstrap(&mut self) -> Result<WorkerBootstrap> {
        self.bootstrap_with_page_timeout(None).await
    }

    pub async fn bootstrap_with_timeout(
        &mut self,
        page_timeout: std::time::Duration,
    ) -> Result<WorkerBootstrap> {
        self.bootstrap_with_page_timeout(Some(page_timeout)).await
    }

    async fn bootstrap_with_page_timeout(
        &mut self,
        page_timeout: Option<std::time::Duration>,
    ) -> Result<WorkerBootstrap> {
        let snapshot = match page_timeout {
            Some(timeout) => tokio::time::timeout(timeout, self.snapshot())
                .await
                .context("worker snapshot timed out")??,
            None => self.snapshot().await?,
        };
        let mut events = Vec::new();
        let mut cursor = 0;
        loop {
            let replay = self.replay_page(cursor);
            let (page, through) = match page_timeout {
                Some(timeout) => tokio::time::timeout(timeout, replay)
                    .await
                    .context("worker replay page timed out")??,
                None => replay.await?,
            };
            if page.is_empty() {
                cursor = through;
                break;
            }
            events.extend(page);
            cursor = through;
        }
        self.latest_seq = cursor;
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
        let (events, latest_seq) = self.replay_page(after_seq).await?;
        self.latest_seq = latest_seq;
        Ok(events)
    }

    async fn replay_page(&mut self, after_seq: u64) -> Result<(Vec<SequencedEvent>, u64)> {
        match self.call(WorkerRequest::Subscribe { after_seq }).await? {
            ResponsePayload::Replay { events, latest_seq } => Ok((events, latest_seq)),
            _ => bail!("worker returned an unexpected replay response"),
        }
    }

    pub async fn session_summary(
        &mut self,
        viewed_through: u64,
        transcript_entry_limit: u16,
    ) -> Result<WorkerSessionSummary> {
        self.require_protocol("bounded session summaries", 3)?;
        match self
            .call(WorkerRequest::SessionSummary {
                viewed_through,
                transcript_entry_limit,
            })
            .await?
        {
            ResponsePayload::SessionSummary(summary) => {
                self.latest_seq = summary.latest_seq;
                Ok(summary)
            }
            _ => bail!("worker returned an unexpected session summary response"),
        }
    }

    /// Poll for events emitted after the most recently observed sequence.
    pub async fn sync(&mut self) -> Result<Vec<SequencedEvent>> {
        let mut cursor = self.latest_seq;
        let (mut events, through) = self.replay_page(cursor).await?;
        cursor = through;
        while !events.is_empty() {
            let (page, through) = self.replay_page(cursor).await?;
            cursor = through;
            if page.is_empty() {
                break;
            }
            events.extend(page);
        }
        self.latest_seq = cursor;
        Ok(events)
    }

    pub async fn prompt(&mut self, text: String, attachments: Vec<Attachment>) -> Result<u64> {
        self.accepted(WorkerRequest::Prompt { text, attachments })
            .await
    }

    pub async fn compact(&mut self, text: String) -> Result<String> {
        match self.call(WorkerRequest::Compact { text }).await? {
            ResponsePayload::Compacted { text } => Ok(text),
            _ => bail!("worker returned an unexpected compaction response"),
        }
    }

    pub async fn cancel(&mut self) -> Result<u64> {
        self.accepted(WorkerRequest::Cancel).await
    }

    pub async fn set_config(&mut self, key: String, value: String) -> Result<u64> {
        self.accepted(WorkerRequest::SetConfig {
            key,
            value: serde_json::Value::String(value),
        })
        .await
    }

    pub async fn checkpoint(&mut self, reason: Option<String>) -> Result<u64> {
        self.require_protocol("tool-quiescent checkpointing", 2)?;
        self.accepted(WorkerRequest::CheckpointWhenQuiescent { reason })
            .await
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
        let operation = request.method_name();
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
        decode_response(&line, &request_id, self.protocol_version).with_context(|| {
            format!(
                "worker {} could not perform {operation}",
                self.worker_version
            )
        })
    }

    async fn call_hello(&mut self, request: WorkerRequest) -> Result<ResponsePayload> {
        let request_id = self.request_id();
        let envelope = RequestEnvelope {
            request_id: request_id.clone(),
            protocol_version: PROTOCOL_VERSION,
            request,
        };
        let mut frame = serde_json::to_vec(&envelope)?;
        frame.push(b'\n');
        self.input.write_all(&frame).await?;
        self.input.flush().await?;
        let line = self
            .output
            .next_line()
            .await
            .context("read worker hello response")?
            .ok_or_else(|| anyhow!("worker proxy disconnected"))?;
        decode_hello_response(&line, &request_id)
    }

    fn request_id(&mut self) -> String {
        let id = format!("hel-{:016x}-{}", self.connection_nonce, self.next_request);
        self.next_request = self.next_request.wrapping_add(1);
        id
    }
}

impl crate::hel_compaction::CompactionBackend for WorkerClient {
    fn compact<'a>(
        &'a mut self,
        prompt: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send + 'a>> {
        Box::pin(async move { self.compact(prompt).await })
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

fn decode_hello_response(line: &str, request_id: &str) -> Result<ResponsePayload> {
    let response: ResponseEnvelope =
        serde_json::from_str(line).context("decode worker hello response")?;
    if response.request_id != request_id {
        bail!(
            "worker response ID mismatch: expected {request_id}, got {}",
            response.request_id
        );
    }
    match response.body {
        ResponseBody::Ok {
            payload: payload @ ResponsePayload::Hello { negotiated, .. },
        } => {
            if response.protocol_version != negotiated {
                bail!(
                    "worker hello envelope uses protocol {}, negotiated {negotiated}",
                    response.protocol_version
                );
            }
            Ok(payload)
        }
        ResponseBody::Ok { .. } => bail!("worker returned an unexpected hello response"),
        ResponseBody::Error { error } => Err(anyhow!(
            "worker rejected hello ({:?}): {}",
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
                    detail: None,
                },
            },
        };
        let encoded = serde_json::to_string(&response).unwrap();
        let error = decode_response(&encoded, "r1", PROTOCOL_VERSION).unwrap_err();
        assert!(error.to_string().contains("busy"));
    }

    #[test]
    fn frozen_v1_hello_response_bootstraps_the_current_client() {
        let payload = decode_hello_response(
            include_str!("../tests/fixtures/worker-v1/hello-response.json").trim(),
            "hello-1",
        )
        .unwrap();
        assert!(matches!(
            payload,
            ResponsePayload::Hello {
                negotiated: 1,
                worker_version,
                ..
            } if worker_version == "1.0.0"
        ));
    }
}
