//! Stable parsing helpers for the relay-v1 JSON boundary.
//!
//! This module owns tolerant method-name parsing so extending the relay does
//! not turn an unknown request into a disconnected proxy.

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::hel_worker::{RelayRequest, RelayRequestEnvelope};

#[derive(Debug)]
pub enum DecodedRelayRequest {
    Known(RelayRequestEnvelope),
    Unknown {
        request_id: String,
        protocol_version: u32,
        method: String,
    },
    Invalid {
        request_id: String,
        protocol_version: u32,
        message: String,
    },
}

/// Decode the incompatible relay-v1 boundary. Unknown methods remain a
/// structured protocol error rather than being mistaken for transport loss.
pub fn decode_relay_request(bytes: &[u8]) -> Result<DecodedRelayRequest> {
    let identity: Value = serde_json::from_slice(bytes).context("parse relay protocol envelope")?;
    let request_id = identity
        .get("request_id")
        .and_then(Value::as_str)
        .unwrap_or("invalid-request")
        .to_owned();
    let protocol_version = identity
        .get("protocol_version")
        .and_then(Value::as_u64)
        .and_then(|version| u32::try_from(version).ok())
        .unwrap_or_default();
    let raw: RawRequestEnvelope = match serde_json::from_slice(bytes) {
        Ok(raw) => raw,
        Err(error) => {
            return Ok(DecodedRelayRequest::Invalid {
                request_id,
                protocol_version,
                message: format!("invalid relay request envelope: {error}"),
            });
        }
    };
    let method = raw
        .request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    match serde_json::from_value::<RelayRequest>(raw.request) {
        Ok(request) => Ok(DecodedRelayRequest::Known(RelayRequestEnvelope {
            request_id: raw.request_id,
            protocol_version: raw.protocol_version,
            request,
        })),
        Err(_error) if !method.is_empty() && !is_relay_v1_method(&method) => {
            Ok(DecodedRelayRequest::Unknown {
                request_id: raw.request_id,
                protocol_version: raw.protocol_version,
                method,
            })
        }
        Err(error) => Ok(DecodedRelayRequest::Invalid {
            request_id: raw.request_id,
            protocol_version: raw.protocol_version,
            message: format!("invalid {method:?} relay request: {error}"),
        }),
    }
}

fn is_relay_v1_method(method: &str) -> bool {
    matches!(
        method,
        "hello"
            | "attach"
            | "acknowledge"
            | "submit"
            | "status"
            | "credential_state"
            | "read_credentials"
            | "install_credentials"
            | "skills_state"
            | "install_skills"
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRequestEnvelope {
    request_id: String,
    protocol_version: u32,
    request: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_v1_decodes_attach_without_accepting_worker_methods() {
        let request = br#"{"request_id":"r1","protocol_version":1,"request":{"method":"attach","params":{"after_ordinal":7,"after_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}"#;
        let DecodedRelayRequest::Known(envelope) = decode_relay_request(request).unwrap() else {
            panic!("expected known relay request");
        };
        assert!(matches!(
            envelope.request,
            RelayRequest::Attach {
                after_ordinal: 7,
                ..
            }
        ));

        let legacy = br#"{"request_id":"r2","protocol_version":1,"request":{"method":"subscribe","params":{"after_seq":0}}}"#;
        assert!(matches!(
            decode_relay_request(legacy).unwrap(),
            DecodedRelayRequest::Unknown { ref method, .. } if method == "subscribe"
        ));

        let old_attach = br#"{"request_id":"r3","protocol_version":1,"request":{"method":"attach","params":{"controller_store_id":"retired-store","after_ordinal":7,"after_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}"#;
        assert!(matches!(
            decode_relay_request(old_attach).unwrap(),
            DecodedRelayRequest::Invalid { .. }
        ));
    }

    #[test]
    fn malformed_relay_method_is_structurally_invalid() {
        let request = br#"{"request_id":"r1","protocol_version":1,"request":{"method":"acknowledge","params":{}}}"#;
        assert!(matches!(
            decode_relay_request(request).unwrap(),
            DecodedRelayRequest::Invalid { .. }
        ));
    }

    #[test]
    fn credential_methods_decode_on_the_relay_v1_floor_without_a_path() {
        let state =
            br#"{"request_id":"r1","protocol_version":1,"request":{"method":"credential_state"}}"#;
        let DecodedRelayRequest::Known(envelope) = decode_relay_request(state).unwrap() else {
            panic!("credential_state should decode");
        };
        assert_eq!(envelope.protocol_version, 1);
        assert_eq!(envelope.request, RelayRequest::CredentialState);

        let install = br#"{"request_id":"r2","protocol_version":1,"request":{"method":"install_credentials","params":{"data":"e30="}}}"#;
        let DecodedRelayRequest::Known(envelope) = decode_relay_request(install).unwrap() else {
            panic!("install_credentials should decode");
        };
        assert_eq!(
            envelope.request,
            RelayRequest::InstallCredentials {
                data: "e30=".into()
            }
        );

        let caller_selected_path = br#"{"request_id":"r3","protocol_version":1,"request":{"method":"read_credentials","params":{"path":"/tmp/stolen"}}}"#;
        assert!(matches!(
            decode_relay_request(caller_selected_path).unwrap(),
            DecodedRelayRequest::Invalid { .. }
        ));
    }

    #[test]
    fn skills_methods_decode_on_the_relay_v1_floor_without_a_path() {
        let state =
            br#"{"request_id":"r1","protocol_version":1,"request":{"method":"skills_state"}}"#;
        let DecodedRelayRequest::Known(envelope) = decode_relay_request(state).unwrap() else {
            panic!("skills_state should decode");
        };
        assert_eq!(envelope.request, RelayRequest::SkillsState);

        let install = br#"{"request_id":"r2","protocol_version":1,"request":{"method":"install_skills","params":{"data":"SEVMU0tJTDE="}}}"#;
        let DecodedRelayRequest::Known(envelope) = decode_relay_request(install).unwrap() else {
            panic!("install_skills should decode");
        };
        assert_eq!(
            envelope.request,
            RelayRequest::InstallSkills {
                data: "SEVMU0tJTDE=".into()
            }
        );

        let caller_selected_path = br#"{"request_id":"r3","protocol_version":1,"request":{"method":"install_skills","params":{"data":"e30=","path":"/tmp/planted"}}}"#;
        assert!(matches!(
            decode_relay_request(caller_selected_path).unwrap(),
            DecodedRelayRequest::Invalid { .. }
        ));
    }

    #[test]
    fn relay_v1_rejects_unknown_envelope_and_nested_range_fields() {
        let retired_top_level = br#"{"request_id":"r1","protocol_version":1,"controller_store_id":"retired-store","request":{"method":"status"}}"#;
        assert!(matches!(
            decode_relay_request(retired_top_level).unwrap(),
            DecodedRelayRequest::Invalid { .. }
        ));

        let nested = br#"{"request_id":"r2","protocol_version":1,"request":{"method":"hello","params":{"controller_version":"1.0.0","supported":{"min":1,"max":1,"preferred":1}}}}"#;
        assert!(matches!(
            decode_relay_request(nested).unwrap(),
            DecodedRelayRequest::Invalid { .. }
        ));
    }
}
