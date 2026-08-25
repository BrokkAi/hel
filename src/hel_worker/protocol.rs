//! Wire protocol for the durable ACP relay: request/response envelopes,
//! error shapes, and newline-delimited JSON framing. This module is pure
//! serde plus byte-oriented framing; it has no filesystem or state-machine
//! concerns of its own.

use std::io::{BufRead, Write};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::hel_elicitation::ElicitationResponse;
use crate::hel_project_memory::ProjectMemorySnapshot;

use super::DurableRelay;
use super::journal::read_bounded_line;
use super::snapshot::{RelayCommand, RelayEvent, RelayOperationalState};
use super::{MAX_FRAME_BYTES, RELAY_MIN_PROTOCOL_VERSION, RELAY_PROTOCOL_VERSION};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayVersionRange {
    pub min: u32,
    pub max: u32,
}

impl RelayVersionRange {
    pub const CURRENT: Self = Self {
        min: RELAY_MIN_PROTOCOL_VERSION,
        max: RELAY_PROTOCOL_VERSION,
    };

    pub const fn contains(self, version: u32) -> bool {
        self.min <= version && version <= self.max
    }

    pub fn negotiate(self, peer: Self) -> Option<u32> {
        let minimum = self.min.max(peer.min);
        let maximum = self.max.min(peer.max);
        (minimum <= maximum).then_some(maximum)
    }
}

/// A request on the new controller-to-relay boundary. ACP payloads remain ACP
/// payloads; only durability and queue-control operations are Hel-specific.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "method",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RelayRequest {
    Hello {
        controller_version: String,
        supported: RelayVersionRange,
    },
    Attach {
        after_ordinal: u64,
        after_digest: String,
    },
    Acknowledge {
        through_ordinal: u64,
        through_digest: String,
    },
    Submit {
        command_id: String,
        command: RelayCommand,
    },
    Status,
    /// Add hidden background context attached to the next real prompt.
    /// This mutates only the relay-private snapshot and is never projected as
    /// conversation history.
    InstallPromptContext {
        text: String,
    },
    /// Read the session-private memory replica and the baseline it was seeded
    /// from. Connection-only: memory content never enters the relay journal.
    ProjectMemorySnapshot,
    /// Install a controller-reconciled tree into both the replica and its
    /// baseline for the next three-way synchronization.
    InstallProjectMemorySnapshot {
        snapshot: ProjectMemorySnapshot,
    },
    /// Report non-secret metadata for this session's harness credentials.
    /// The runtime handles credential requests on the connection and never
    /// passes them through the durable relay.
    CredentialState,
    /// Read this session's harness credential file as base64. The payload is
    /// connection-only and must never enter relay state or observations.
    ReadCredentials,
    /// Install a base64-encoded credential file into this session's harness
    /// home. The destination path is fixed by the worker launch config.
    InstallCredentials {
        data: String,
    },
    /// Report non-secret metadata for this session's synced skills trees.
    /// Handled on the connection like credential requests; the durable relay
    /// never sees them.
    SkillsState,
    /// Replace this session's synced skills trees with a base64-encoded
    /// `hel_skills` archive. The destination directories are fixed by the
    /// worker launch config and the harness skills whitelist.
    InstallSkills {
        data: String,
    },
    /// Run a prompt in a disposable ACP session and return its text. The
    /// runtime answers this on the connection: a scratch prompt is not session
    /// history, so it never reaches the durable relay, its journal, or its
    /// command ledger.
    Compact {
        prompt: String,
    },
    /// Resolve one in-flight form without journaling its answer.
    RespondElicitation {
        elicitation_id: String,
        response: ElicitationResponse,
    },
}

impl RelayRequest {
    pub const fn method_name(&self) -> &'static str {
        match self {
            Self::Hello { .. } => "hello",
            Self::Attach { .. } => "attach",
            Self::Acknowledge { .. } => "acknowledge",
            Self::Submit { .. } => "submit",
            Self::Status => "status",
            Self::InstallPromptContext { .. } => "install_prompt_context",
            Self::ProjectMemorySnapshot => "project_memory_snapshot",
            Self::InstallProjectMemorySnapshot { .. } => "install_project_memory_snapshot",
            Self::CredentialState => "credential_state",
            Self::ReadCredentials => "read_credentials",
            Self::InstallCredentials { .. } => "install_credentials",
            Self::SkillsState => "skills_state",
            Self::InstallSkills { .. } => "install_skills",
            Self::Compact { .. } => "compact",
            Self::RespondElicitation { .. } => "respond_elicitation",
        }
    }

    /// Oldest protocol that understands this method or command payload. Form
    /// answers landed in protocol 2, hidden context in 3, project-memory sync
    /// in 4, and user shell commands in 5.
    pub const fn minimum_protocol(&self) -> u32 {
        match self {
            Self::RespondElicitation { .. } => 2,
            Self::InstallPromptContext { .. } => 3,
            Self::ProjectMemorySnapshot | Self::InstallProjectMemorySnapshot { .. } => 4,
            Self::Submit { command, .. } => command.minimum_protocol(),
            _ => RELAY_MIN_PROTOCOL_VERSION,
        }
    }

    pub const fn supported_at(&self, protocol_version: u32) -> bool {
        RelayVersionRange::CURRENT.contains(protocol_version)
            && protocol_version >= self.minimum_protocol()
    }
}

pub(crate) fn incompatible_request_protocol(protocol_version: u32) -> RelayResponseBody {
    relay_error(
        RelayErrorCode::IncompatibleProtocol,
        format!(
            "request uses protocol {protocol_version}, relay supports protocol {}-{}",
            RELAY_MIN_PROTOCOL_VERSION, RELAY_PROTOCOL_VERSION
        ),
        false,
        None,
    )
}

pub fn incompatible_request_protocol_response(
    request_id: String,
    protocol_version: u32,
) -> RelayResponseEnvelope {
    RelayResponseEnvelope {
        request_id,
        protocol_version,
        body: incompatible_request_protocol(protocol_version),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayRequestEnvelope {
    pub request_id: String,
    pub protocol_version: u32,
    pub request: RelayRequest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelayResponseEnvelope {
    pub request_id: String,
    pub protocol_version: u32,
    #[serde(flatten)]
    pub body: RelayResponseBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
// This is a short-lived wire DTO. Boxing every successful response would add
// an allocation without reducing retained relay state.
#[allow(clippy::large_enum_variant)]
pub enum RelayResponseBody {
    Ok { payload: RelayResponsePayload },
    Error { error: RelayProtocolError },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum RelayResponsePayload {
    Hello {
        negotiated: u32,
        relay_version: String,
        session_id: String,
    },
    Attached {
        state: RelayOperationalState,
        events: Vec<RelayEvent>,
        through_ordinal: u64,
        through_digest: String,
    },
    Acknowledged {
        through_ordinal: u64,
        through_digest: String,
    },
    Accepted {
        command_id: String,
        ordinal: u64,
    },
    Status(RelayOperationalState),
    PromptContextInstalled,
    ProjectMemorySnapshot {
        baseline: ProjectMemorySnapshot,
        replica: ProjectMemorySnapshot,
    },
    ProjectMemorySnapshotInstalled,
    /// Fingerprint and freshness of a session's harness credentials. Neither
    /// value is secret.
    CredentialState {
        present: bool,
        fingerprint: String,
        freshness_epoch_ms: Option<i64>,
    },
    /// Base64 of a session's credential file. Sent only on the connection
    /// socket, never recorded.
    Credentials {
        data: String,
    },
    /// Fingerprint of a session's synced skills trees. Not secret.
    SkillsState {
        present: bool,
        fingerprint: String,
    },
    /// Agent text from a disposable ACP compaction session.
    Compacted {
        text: String,
    },
    ElicitationResolved {
        elicitation_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayProtocolError {
    pub code: RelayErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<RelayErrorDetail>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayErrorCode {
    IncompatibleProtocol,
    InvalidRequest,
    InvalidState,
    Desynchronized,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RelayErrorDetail {
    Desynchronized {
        requested_after: u64,
        requested_digest: String,
        earliest_available: u64,
        earliest_digest: String,
        latest: u64,
        latest_digest: String,
    },
}

pub(crate) fn relay_protocol_error(
    code: RelayErrorCode,
    message: impl Into<String>,
    retryable: bool,
    detail: Option<RelayErrorDetail>,
) -> RelayProtocolError {
    RelayProtocolError {
        code,
        message: message.into(),
        retryable,
        detail,
    }
}

pub(crate) fn relay_error(
    code: RelayErrorCode,
    message: impl Into<String>,
    retryable: bool,
    detail: Option<RelayErrorDetail>,
) -> RelayResponseBody {
    RelayResponseBody::Error {
        error: relay_protocol_error(code, message, retryable, detail),
    }
}

pub fn unsupported_relay_method_response(
    request_id: String,
    protocol_version: u32,
    method: String,
) -> RelayResponseEnvelope {
    RelayResponseEnvelope {
        request_id,
        protocol_version,
        body: relay_error(
            RelayErrorCode::InvalidRequest,
            format!("relay does not support method {method:?}"),
            false,
            None,
        ),
    }
}

pub fn invalid_relay_request_response(
    request_id: String,
    protocol_version: u32,
    message: String,
) -> RelayResponseEnvelope {
    RelayResponseEnvelope {
        request_id,
        protocol_version,
        body: relay_error(RelayErrorCode::InvalidRequest, message, false, None),
    }
}

pub fn read_relay_frame(reader: &mut impl BufRead) -> Result<Option<RelayRequestEnvelope>> {
    let mut bytes = Vec::new();
    let (read, _) = read_bounded_line(reader, &mut bytes, MAX_FRAME_BYTES)
        .context("read relay protocol frame")?;
    if read == 0 {
        return Ok(None);
    }
    if bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    if bytes.is_empty() {
        bail!("empty relay protocol frame");
    }
    serde_json::from_slice(&bytes)
        .context("parse relay protocol request")
        .map(Some)
}

pub fn write_relay_frame(writer: &mut impl Write, response: &RelayResponseEnvelope) -> Result<()> {
    serde_json::to_writer(&mut *writer, response)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

pub fn serve_relay_json_lines(
    reader: &mut impl BufRead,
    writer: &mut impl Write,
    relay: &mut DurableRelay,
) -> Result<()> {
    while let Some(request) = read_relay_frame(reader)? {
        let response = relay.handle(request);
        write_relay_frame(writer, &response)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_worker::test_support::*;

    #[test]
    fn relay_has_a_hard_protocol_v1_floor() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let response = relay.handle(RelayRequestEnvelope {
            request_id: "hello-old".into(),
            protocol_version: 0,
            request: RelayRequest::Hello {
                controller_version: "old".into(),
                supported: RelayVersionRange { min: 0, max: 0 },
            },
        });
        assert!(matches!(
            response.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::IncompatibleProtocol,
                    retryable: false,
                    ..
                }
            }
        ));
    }

    #[test]
    fn current_range_overlaps_protocol_v1() {
        let v1 = RelayVersionRange { min: 1, max: 1 };
        let v2 = RelayVersionRange { min: 2, max: 2 };
        assert_eq!(RelayVersionRange::CURRENT.negotiate(v1), Some(1));
        assert_eq!(v1.negotiate(RelayVersionRange::CURRENT), Some(1));
        assert_eq!(
            RelayVersionRange::CURRENT.negotiate(RelayVersionRange::CURRENT),
            Some(RELAY_PROTOCOL_VERSION)
        );
        assert_eq!(v1.negotiate(v2), None);
        assert!(RelayVersionRange::CURRENT.contains(1));
        assert!(RelayVersionRange::CURRENT.contains(2));
        assert!(RelayVersionRange::CURRENT.contains(3));
        assert!(RelayVersionRange::CURRENT.contains(4));
        assert!(!RelayVersionRange::CURRENT.contains(0));
        assert!(!RelayVersionRange::CURRENT.contains(RELAY_PROTOCOL_VERSION + 1));
        assert!(RelayRequest::Status.supported_at(1));
        assert!(
            !RelayRequest::RespondElicitation {
                elicitation_id: String::new(),
                response: crate::hel_elicitation::ElicitationResponse::Cancel,
            }
            .supported_at(1)
        );
    }

    #[test]
    fn hello_from_protocol_v1_controller_negotiates_v1() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let response = relay.handle(RelayRequestEnvelope {
            request_id: "hello-v1".into(),
            protocol_version: 1,
            request: RelayRequest::Hello {
                controller_version: "old".into(),
                supported: RelayVersionRange { min: 1, max: 1 },
            },
        });
        assert_eq!(response.protocol_version, 1);
        match response.body {
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Hello { negotiated, .. },
            } => assert_eq!(negotiated, 1),
            other => panic!("expected a v1 hello, got {other:?}"),
        }
    }

    #[test]
    fn protocol_v1_status_is_accepted() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let response = relay.handle(RelayRequestEnvelope {
            request_id: "status-v1".into(),
            protocol_version: 1,
            request: RelayRequest::Status,
        });
        assert_eq!(response.protocol_version, 1);
        assert!(matches!(
            response.body,
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Status(_)
            }
        ));
    }

    #[test]
    fn protocol_v1_cannot_respond_to_elicitation_on_the_durable_relay() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let response = relay.handle(RelayRequestEnvelope {
            request_id: "elicit-v1".into(),
            protocol_version: 1,
            request: RelayRequest::RespondElicitation {
                elicitation_id: "form-1".into(),
                response: crate::hel_elicitation::ElicitationResponse::Cancel,
            },
        });
        assert!(matches!(
            response.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::IncompatibleProtocol,
                    retryable: false,
                    ..
                }
            }
        ));
    }
}
