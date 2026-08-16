//! Controller-side client for a session relay's JSON-lines proxy.

use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::hel_credentials::CredentialSnapshot;
use crate::hel_targets::CommandSpec;
use crate::hel_worker::{
    MAX_FRAME_BYTES, RELAY_EVENT_GENESIS_DIGEST, RELAY_PROTOCOL_VERSION, RelayCommand, RelayCursor,
    RelayErrorCode, RelayEvent, RelayOperationalState, RelayProtocolError, RelayRequest,
    RelayRequestEnvelope, RelayResponseBody, RelayResponseEnvelope, RelayResponsePayload,
    RelayVersionRange, validate_relay_event,
};

const RELAY_RPC_TIMEOUT: Duration = Duration::from_secs(15);

/// Forward a relay proxy's stderr to the log, one line at a time, until the
/// child closes it. Reporting rather than dropping keeps connect failures
/// diagnosable now that the controller no longer shares its terminal.
async fn drain_proxy_stderr(errors: tokio::process::ChildStderr, purpose: String) {
    let mut lines = BufReader::new(errors).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) if line.trim().is_empty() => continue,
            Ok(Some(line)) => tracing::warn!(%purpose, %line, "relay proxy stderr"),
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(%purpose, %error, "read relay proxy stderr");
                return;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct RelayAttachment {
    pub state: RelayOperationalState,
    pub events: Vec<RelayEvent>,
    pub through_ordinal: u64,
    pub through_digest: String,
}

/// One bounded page in a catch-up whose upper frontier was fixed before any
/// page was applied. The relay may return newer events on later `Attach`
/// calls; those are deliberately left for the next catch-up.
#[derive(Debug, Clone)]
pub struct RelayEventPage {
    pub events: Vec<RelayEvent>,
    pub through_ordinal: u64,
    pub through_digest: String,
}

#[derive(Debug, Clone)]
pub struct RelayCatchUp {
    pub state: RelayOperationalState,
    pub frontier: RelayCursor,
    pub first_page: RelayEventPage,
}

#[derive(Debug)]
pub struct RelayRejected(pub RelayProtocolError);

impl std::fmt::Display for RelayRejected {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "relay rejected request ({:?}): {}",
            self.0.code, self.0.message
        )
    }
}

impl std::error::Error for RelayRejected {}

impl RelayRejected {
    pub fn is_desynchronized(&self) -> bool {
        self.0.code == RelayErrorCode::Desynchronized
    }
}

/// Controller-side connection to the durable ACP relay protocol.
///
/// This type does not construct transcript state or request an unbounded
/// history. Callers persist each attachment page, then acknowledge its
/// `through` frontier only after committing it locally.
pub struct RelayClient {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    request_timeout: Duration,
    next_request: u64,
    connection_nonce: u64,
    protocol_version: u32,
    session_id: String,
    relay_version: String,
    latest_ordinal: u64,
    latest_digest: String,
}

impl RelayClient {
    pub async fn connect(spec: &CommandSpec, expected_session_id: &str) -> Result<Self> {
        Self::connect_with_timeout(spec, expected_session_id, RELAY_RPC_TIMEOUT).await
    }

    async fn connect_with_timeout(
        spec: &CommandSpec,
        expected_session_id: &str,
        request_timeout: Duration,
    ) -> Result<Self> {
        let mut child = Command::new(&spec.program)
            .args(&spec.args)
            .envs(&spec.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Never inherit: the controller owns a TUI alternate screen, so a
            // child writing to the shared stderr corrupts the display outside
            // the renderer's buffer. Drain it into the log instead.
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("start session relay proxy for {}", spec.purpose))?;
        if let Some(errors) = child.stderr.take() {
            let purpose = spec.purpose.clone();
            tokio::spawn(drain_proxy_stderr(errors, purpose));
        }
        let input = child
            .stdin
            .take()
            .context("relay proxy stdin unavailable")?;
        let output = child
            .stdout
            .take()
            .context("relay proxy stdout unavailable")?;
        let mut nonce_bytes = [0_u8; 8];
        getrandom::fill(&mut nonce_bytes)
            .map_err(|error| anyhow!("generate relay request nonce: {error}"))?;
        let mut client = Self {
            child,
            input,
            output: BufReader::new(output),
            request_timeout,
            next_request: 1,
            connection_nonce: u64::from_le_bytes(nonce_bytes),
            protocol_version: RELAY_PROTOCOL_VERSION,
            session_id: String::new(),
            relay_version: String::new(),
            latest_ordinal: 0,
            latest_digest: RELAY_EVENT_GENESIS_DIGEST.to_owned(),
        };
        let response = client
            .call_hello(RelayRequest::Hello {
                controller_version: env!("CARGO_PKG_VERSION").to_owned(),
                supported: RelayVersionRange::CURRENT,
            })
            .await?;
        let RelayResponsePayload::Hello {
            negotiated,
            relay_version,
            session_id,
        } = response
        else {
            bail!("relay returned an unexpected hello response")
        };
        if session_id != expected_session_id {
            bail!("relay belongs to session {session_id}, not {expected_session_id}");
        }
        if negotiated != RELAY_PROTOCOL_VERSION {
            bail!(
                "relay negotiated unsupported protocol {negotiated}; this controller requires {RELAY_PROTOCOL_VERSION}"
            );
        }
        client.protocol_version = negotiated;
        client.session_id = session_id;
        client.relay_version = relay_version;
        Ok(client)
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn relay_version(&self) -> &str {
        &self.relay_version
    }

    pub fn latest_ordinal(&self) -> u64 {
        self.latest_ordinal
    }

    pub fn latest_digest(&self) -> &str {
        &self.latest_digest
    }

    pub async fn attach(
        &mut self,
        after_ordinal: u64,
        after_digest: impl Into<String>,
    ) -> Result<RelayAttachment> {
        let after_digest = after_digest.into();
        match self
            .call(RelayRequest::Attach {
                after_ordinal,
                after_digest: after_digest.clone(),
            })
            .await?
        {
            RelayResponsePayload::Attached {
                state,
                events,
                through_ordinal,
                through_digest,
            } => {
                let mut cursor = RelayCursor {
                    ordinal: after_ordinal,
                    digest: after_digest,
                };
                for event in &events {
                    validate_relay_event(cursor.ordinal, &cursor.digest, event)
                        .context("verify relay attachment event chain")?;
                    cursor.ordinal = event.ordinal;
                    cursor.digest.clone_from(&event.digest);
                }
                if cursor.ordinal != through_ordinal || cursor.digest != through_digest {
                    bail!("relay attachment frontier does not match its event chain");
                }
                self.latest_ordinal = state.latest_ordinal;
                self.latest_digest = state.latest_digest.clone();
                Ok(RelayAttachment {
                    state,
                    events,
                    through_ordinal,
                    through_digest,
                })
            }
            _ => bail!("relay returned an unexpected attach response"),
        }
    }

    /// Start a bounded catch-up by capturing the relay frontier before the
    /// caller applies anything. Callers persist and acknowledge `first_page`,
    /// then request further pages with [`Self::next_catch_up_page`].
    pub async fn begin_catch_up(
        &mut self,
        after_ordinal: u64,
        after_digest: impl Into<String>,
    ) -> Result<RelayCatchUp> {
        let after_digest = after_digest.into();
        let first = self.attach(after_ordinal, after_digest.clone()).await?;
        let frontier = RelayCursor {
            ordinal: first.state.latest_ordinal,
            digest: first.state.latest_digest.clone(),
        };
        let previous = RelayCursor {
            ordinal: after_ordinal,
            digest: after_digest,
        };
        let state = first.state.clone();
        let first_page = clip_catch_up_page(first, &previous, &frontier)?;
        Ok(RelayCatchUp {
            state,
            frontier,
            first_page,
        })
    }

    /// Fetch the next bounded page without chasing events that arrived after
    /// `frontier` was captured. A response may contain such newer events; the
    /// returned page is clipped at the exact ordinal-and-digest frontier.
    pub async fn next_catch_up_page(
        &mut self,
        previous: &RelayCursor,
        frontier: &RelayCursor,
    ) -> Result<RelayEventPage> {
        if previous.ordinal >= frontier.ordinal {
            bail!("relay catch-up is already at its fixed frontier");
        }
        let attachment = self
            .attach(previous.ordinal, previous.digest.clone())
            .await?;
        clip_catch_up_page(attachment, previous, frontier)
    }

    pub async fn acknowledge(
        &mut self,
        through_ordinal: u64,
        through_digest: impl Into<String>,
    ) -> Result<RelayCursor> {
        match self
            .call(RelayRequest::Acknowledge {
                through_ordinal,
                through_digest: through_digest.into(),
            })
            .await?
        {
            RelayResponsePayload::Acknowledged {
                through_ordinal,
                through_digest,
            } => Ok(RelayCursor {
                ordinal: through_ordinal,
                digest: through_digest,
            }),
            _ => bail!("relay returned an unexpected acknowledgement response"),
        }
    }

    pub async fn status(&mut self) -> Result<RelayOperationalState> {
        match self.call(RelayRequest::Status).await? {
            RelayResponsePayload::Status(status) => {
                self.latest_ordinal = status.latest_ordinal;
                self.latest_digest = status.latest_digest.clone();
                Ok(status)
            }
            _ => bail!("relay returned an unexpected status response"),
        }
    }

    /// Return the fingerprint and freshness of this session's harness
    /// credentials without exposing the credential bytes.
    pub async fn credential_state(&mut self) -> Result<CredentialSnapshot> {
        credential_snapshot(self.call(RelayRequest::CredentialState).await?)
    }

    /// Read this session's credential file. Callers must keep these bytes out
    /// of durable relay observations, logs, and archives.
    pub async fn read_credentials(&mut self) -> Result<Vec<u8>> {
        match self.call(RelayRequest::ReadCredentials).await? {
            RelayResponsePayload::Credentials { data } => BASE64
                .decode(data.as_bytes())
                .context("decode relay credential payload"),
            _ => bail!("relay returned an unexpected credential response"),
        }
    }

    /// Install credentials into the harness home fixed by this session's
    /// launch config.
    pub async fn install_credentials(&mut self, bytes: &[u8]) -> Result<CredentialSnapshot> {
        credential_snapshot(
            self.call(RelayRequest::InstallCredentials {
                data: BASE64.encode(bytes),
            })
            .await?,
        )
    }

    /// Return the fingerprint of this session's synced skills trees without
    /// transferring the tree itself.
    pub async fn skills_state(&mut self) -> Result<crate::hel_skills::SkillsSyncState> {
        skills_sync_state(self.call(RelayRequest::SkillsState).await?)
    }

    /// Replace this session's synced skills trees with an encoded
    /// `hel_skills::SkillsArchive`. The destination directories are fixed by
    /// the session's launch config and the harness skills whitelist.
    pub async fn install_skills(
        &mut self,
        archive_bytes: &[u8],
    ) -> Result<crate::hel_skills::SkillsSyncState> {
        skills_sync_state(
            self.call(RelayRequest::InstallSkills {
                data: BASE64.encode(archive_bytes),
            })
            .await?,
        )
    }

    pub async fn submit(
        &mut self,
        command_id: impl Into<String>,
        command: RelayCommand,
    ) -> Result<u64> {
        let command_id = command_id.into();
        match self
            .call(RelayRequest::Submit {
                command_id: command_id.clone(),
                command,
            })
            .await?
        {
            RelayResponsePayload::Accepted {
                command_id: accepted_id,
                ordinal,
            } if accepted_id == command_id => Ok(ordinal),
            RelayResponsePayload::Accepted {
                command_id: accepted_id,
                ..
            } => bail!("relay accepted command under ID {accepted_id}, expected {command_id}"),
            _ => bail!("relay returned an unexpected command response"),
        }
    }

    pub async fn detach(mut self) -> Result<()> {
        self.input
            .shutdown()
            .await
            .context("close relay proxy stdin")?;
        match tokio::time::timeout(std::time::Duration::from_millis(500), self.child.wait()).await {
            Ok(status) => {
                status.context("wait for relay proxy")?;
            }
            Err(_) => {
                self.child.start_kill().context("stop relay proxy")?;
                let _ = self.child.wait().await;
            }
        }
        Ok(())
    }

    async fn call(&mut self, request: RelayRequest) -> Result<RelayResponsePayload> {
        let operation = request.method_name();
        let request_id = self.request_id();
        let envelope = RelayRequestEnvelope {
            request_id: request_id.clone(),
            protocol_version: self.protocol_version,
            request,
        };
        let mut frame = serde_json::to_vec(&envelope)?;
        if frame.len() > MAX_FRAME_BYTES {
            bail!("relay request frame is too large");
        }
        frame.push(b'\n');
        let line = tokio::time::timeout(self.request_timeout, async {
            self.input
                .write_all(&frame)
                .await
                .context("write relay request")?;
            self.input.flush().await.context("flush relay request")?;
            read_bounded_frame(&mut self.output)
                .await
                .context("read relay response")?
                .ok_or_else(|| anyhow!("relay proxy disconnected"))
        })
        .await
        .with_context(|| {
            format!(
                "relay {operation} timed out after {} seconds",
                self.request_timeout.as_secs_f64()
            )
        })??;
        decode_relay_response(&line, &request_id, self.protocol_version)
            .with_context(|| format!("relay {} could not perform {operation}", self.relay_version))
    }

    async fn call_hello(&mut self, request: RelayRequest) -> Result<RelayResponsePayload> {
        let request_id = self.request_id();
        let envelope = RelayRequestEnvelope {
            request_id: request_id.clone(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request,
        };
        let mut frame = serde_json::to_vec(&envelope)?;
        if frame.len() > MAX_FRAME_BYTES {
            bail!("relay hello request frame is too large");
        }
        frame.push(b'\n');
        let line = tokio::time::timeout(self.request_timeout, async {
            self.input
                .write_all(&frame)
                .await
                .context("write relay hello request")?;
            self.input
                .flush()
                .await
                .context("flush relay hello request")?;
            read_bounded_frame(&mut self.output)
                .await
                .context("read relay hello response")?
                .ok_or_else(|| anyhow!("relay proxy disconnected during hello"))
        })
        .await
        .with_context(|| {
            format!(
                "relay hello timed out after {} seconds",
                self.request_timeout.as_secs_f64()
            )
        })??;
        decode_relay_hello_response(&line, &request_id)
    }

    fn request_id(&mut self) -> String {
        let id = format!("relay-{:016x}-{}", self.connection_nonce, self.next_request);
        self.next_request = self.next_request.wrapping_add(1);
        id
    }
}

fn credential_snapshot(payload: RelayResponsePayload) -> Result<CredentialSnapshot> {
    match payload {
        RelayResponsePayload::CredentialState {
            present,
            fingerprint,
            freshness_epoch_ms,
        } => Ok(CredentialSnapshot {
            present,
            fingerprint,
            freshness_epoch_ms,
        }),
        _ => bail!("relay returned an unexpected credential state response"),
    }
}

fn skills_sync_state(payload: RelayResponsePayload) -> Result<crate::hel_skills::SkillsSyncState> {
    match payload {
        RelayResponsePayload::SkillsState {
            present,
            fingerprint,
        } => Ok(crate::hel_skills::SkillsSyncState {
            present,
            fingerprint,
        }),
        _ => bail!("relay returned an unexpected skills state response"),
    }
}

async fn read_bounded_frame(reader: &mut (impl AsyncBufRead + Unpin)) -> Result<Option<String>> {
    read_bounded_frame_with_limit(reader, MAX_FRAME_BYTES).await
}

async fn read_bounded_frame_with_limit(
    reader: &mut (impl AsyncBufRead + Unpin),
    maximum_bytes: usize,
) -> Result<Option<String>> {
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if frame.is_empty() {
                return Ok(None);
            }
            bail!("relay proxy disconnected in the middle of a response frame");
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let payload = newline.map_or(available, |position| &available[..position]);
        if frame.len().saturating_add(payload.len()) > maximum_bytes {
            bail!("relay response frame is too large");
        }
        frame.extend_from_slice(payload);
        reader.consume(consumed);
        if newline.is_some() {
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return String::from_utf8(frame)
                .context("relay response is not UTF-8")
                .map(Some);
        }
    }
}

fn clip_catch_up_page(
    page: RelayAttachment,
    previous: &RelayCursor,
    frontier: &RelayCursor,
) -> Result<RelayEventPage> {
    if previous.ordinal > frontier.ordinal {
        bail!("relay catch-up starts beyond its fixed frontier");
    }
    if previous.ordinal == frontier.ordinal {
        if previous != frontier {
            bail!("relay catch-up cursor digest differs from its fixed frontier");
        }
        if !page.events.is_empty() || page.through_ordinal != previous.ordinal {
            bail!("relay attachment advanced beyond its advertised frontier");
        }
        return Ok(RelayEventPage {
            events: Vec::new(),
            through_ordinal: previous.ordinal,
            through_digest: previous.digest.clone(),
        });
    }
    if page.through_ordinal <= previous.ordinal || page.events.is_empty() {
        bail!("relay catch-up page did not advance");
    }
    if page.through_ordinal <= frontier.ordinal {
        let through = RelayCursor {
            ordinal: page.through_ordinal,
            digest: page.through_digest.clone(),
        };
        if through.ordinal == frontier.ordinal && through != *frontier {
            bail!("relay catch-up page digest differs from its fixed frontier");
        }
        return Ok(RelayEventPage {
            events: page.events,
            through_ordinal: through.ordinal,
            through_digest: through.digest,
        });
    }

    let events = page
        .events
        .into_iter()
        .take_while(|event| event.ordinal <= frontier.ordinal)
        .collect::<Vec<_>>();
    let reached = events
        .last()
        .map(|event| RelayCursor {
            ordinal: event.ordinal,
            digest: event.digest.clone(),
        })
        .ok_or_else(|| anyhow!("relay catch-up page skipped its fixed frontier"))?;
    if reached != *frontier {
        bail!("relay catch-up page does not contain its fixed frontier");
    }
    Ok(RelayEventPage {
        events,
        through_ordinal: reached.ordinal,
        through_digest: reached.digest,
    })
}

fn decode_relay_response(
    line: &str,
    request_id: &str,
    protocol: u32,
) -> Result<RelayResponsePayload> {
    let response: RelayResponseEnvelope =
        serde_json::from_str(line).context("decode relay response")?;
    if response.request_id != request_id {
        bail!(
            "relay response ID mismatch: expected {request_id}, got {}",
            response.request_id
        );
    }
    if response.protocol_version != protocol {
        bail!(
            "relay response protocol mismatch: expected {protocol}, got {}",
            response.protocol_version
        );
    }
    match response.body {
        RelayResponseBody::Ok { payload } => Ok(payload),
        RelayResponseBody::Error { error } => Err(RelayRejected(error).into()),
    }
}

fn decode_relay_hello_response(line: &str, request_id: &str) -> Result<RelayResponsePayload> {
    let response: RelayResponseEnvelope =
        serde_json::from_str(line).context("decode relay hello response")?;
    if response.request_id != request_id {
        bail!(
            "relay response ID mismatch: expected {request_id}, got {}",
            response.request_id
        );
    }
    match response.body {
        RelayResponseBody::Ok {
            payload: payload @ RelayResponsePayload::Hello { negotiated, .. },
        } => {
            if response.protocol_version != negotiated {
                bail!(
                    "relay hello envelope uses protocol {}, negotiated {negotiated}",
                    response.protocol_version
                );
            }
            Ok(payload)
        }
        RelayResponseBody::Ok { .. } => bail!("relay returned an unexpected hello response"),
        RelayResponseBody::Error { error } => Err(RelayRejected(error).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_worker::{DurableRelay, RelayObservation};
    const SESSION_ID: &str = "018f9dd2-a3b4-7c8d-9000-123456789abc";

    #[test]
    fn relay_decoder_preserves_explicit_desynchronization() {
        let response = RelayResponseEnvelope {
            request_id: "relay-1".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            body: RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Desynchronized,
                    message: "journal gap".into(),
                    retryable: false,
                    detail: None,
                },
            },
        };
        let encoded = serde_json::to_string(&response).unwrap();
        let error = decode_relay_response(&encoded, "relay-1", RELAY_PROTOCOL_VERSION).unwrap_err();
        assert!(
            error
                .downcast_ref::<RelayRejected>()
                .is_some_and(RelayRejected::is_desynchronized)
        );
    }

    #[test]
    fn relay_decoder_rejects_crossed_request_ids() {
        let response = RelayResponseEnvelope {
            request_id: "other".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            body: RelayResponseBody::Ok {
                payload: RelayResponsePayload::Acknowledged {
                    through_ordinal: 4,
                    through_digest: "a".repeat(64),
                },
            },
        };
        let encoded = serde_json::to_string(&response).unwrap();
        assert!(
            decode_relay_response(&encoded, "wanted", RELAY_PROTOCOL_VERSION)
                .unwrap_err()
                .to_string()
                .contains("ID mismatch")
        );
    }

    #[test]
    fn command_spec_preserves_argv_boundaries() {
        let spec = CommandSpec::new("ssh", ["host", "hel worker proxy --root '/odd path'"]);
        assert_eq!(spec.program, "ssh");
        assert_eq!(spec.args.len(), 2);
        assert_eq!(spec.args[1], "hel worker proxy --root '/odd path'");
    }

    #[test]
    fn relay_protocol_version_range_contains_current_version() {
        assert_eq!(
            RelayVersionRange::CURRENT.negotiate(RelayVersionRange::CURRENT),
            Some(RELAY_PROTOCOL_VERSION)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn silent_proxy_handshake_has_a_bounded_deadline() {
        let spec = CommandSpec::new("sh", ["-c", "sleep 30"]).purpose("test silent relay proxy");
        let started = std::time::Instant::now();

        let error = RelayClient::connect_with_timeout(&spec, SESSION_ID, Duration::from_millis(50))
            .await
            .err()
            .expect("silent relay must time out");

        assert!(error.to_string().contains("relay hello timed out"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn response_frame_limit_is_enforced_before_newline() {
        let (mut writer, reader) = tokio::io::duplex(32);
        let write = tokio::spawn(async move {
            writer.write_all(b"123456789\n").await.unwrap();
        });
        let mut reader = BufReader::new(reader);

        let error = read_bounded_frame_with_limit(&mut reader, 8)
            .await
            .unwrap_err();

        write.await.unwrap();
        assert!(error.to_string().contains("frame is too large"));
    }

    #[test]
    fn catch_up_page_stops_at_the_frontier_captured_before_stream_growth() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        for message in ["one", "two", "arrived concurrently"] {
            relay
                .record_observation(RelayObservation::Warning {
                    message: message.into(),
                })
                .unwrap();
        }
        let all = relay.events_after(0, RELAY_EVENT_GENESIS_DIGEST).unwrap();
        let previous = RelayCursor {
            ordinal: all[0].ordinal,
            digest: all[0].digest.clone(),
        };
        let frontier = RelayCursor {
            ordinal: all[1].ordinal,
            digest: all[1].digest.clone(),
        };
        let page = RelayAttachment {
            state: relay.operational_state(),
            events: all[1..].to_vec(),
            through_ordinal: all[2].ordinal,
            through_digest: all[2].digest.clone(),
        };
        let clipped = clip_catch_up_page(page, &previous, &frontier).unwrap();
        assert_eq!(clipped.through_ordinal, frontier.ordinal);
        assert_eq!(clipped.through_digest, frontier.digest);
        assert_eq!(clipped.events.len(), 1);
        assert_eq!(clipped.events.last().unwrap().ordinal, 2);
    }
}
