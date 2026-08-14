//! Stable parsing helpers for the worker's protocol-v1 JSON boundary.
//!
//! The typed v1 DTOs remain in `hel_worker` for now.  This module owns the
//! version-neutral bootstrap and tolerant method-name parsing so extending the
//! worker does not turn an unknown request into a disconnected proxy.

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::hel_worker::{RequestEnvelope, WorkerRequest};

#[derive(Debug, Deserialize)]
struct RawRequestEnvelope {
    request_id: String,
    protocol_version: u32,
    request: Value,
}

#[derive(Debug)]
pub enum DecodedRequest {
    Known(RequestEnvelope),
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

pub fn decode_request(bytes: &[u8]) -> Result<DecodedRequest> {
    let raw: RawRequestEnvelope =
        serde_json::from_slice(bytes).context("parse worker protocol envelope")?;
    let method = raw
        .request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    match serde_json::from_value::<WorkerRequest>(raw.request) {
        Ok(request) => Ok(DecodedRequest::Known(RequestEnvelope {
            request_id: raw.request_id,
            protocol_version: raw.protocol_version,
            request,
        })),
        Err(_error) if !method.is_empty() && !is_v1_method(&method) => {
            Ok(DecodedRequest::Unknown {
                request_id: raw.request_id,
                protocol_version: raw.protocol_version,
                method,
            })
        }
        Err(error) => Ok(DecodedRequest::Invalid {
            request_id: raw.request_id,
            protocol_version: raw.protocol_version,
            message: format!("invalid {method:?} request: {error}"),
        }),
    }
}

fn is_v1_method(method: &str) -> bool {
    matches!(
        method,
        "hello"
            | "status"
            | "snapshot"
            | "subscribe"
            | "prompt"
            | "compact"
            | "cancel"
            | "set_config"
            | "checkpoint"
            | "close"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_method_retains_envelope_for_structured_rejection() {
        let request = br#"{"request_id":"r1","protocol_version":1,"request":{"method":"future_action","params":{"value":1}}}"#;
        let DecodedRequest::Unknown {
            request_id,
            protocol_version,
            method,
        } = decode_request(request).unwrap()
        else {
            panic!("expected unknown request");
        };
        assert_eq!(request_id, "r1");
        assert_eq!(protocol_version, 1);
        assert_eq!(method, "future_action");
    }

    #[test]
    fn credential_methods_decode_without_a_path_from_the_caller() {
        let request =
            br#"{"request_id":"r1","protocol_version":3,"request":{"method":"credential_state"}}"#;
        let DecodedRequest::Known(envelope) = decode_request(request).unwrap() else {
            panic!("credential_state should decode");
        };
        assert_eq!(envelope.request, WorkerRequest::CredentialState);

        let request = br#"{"request_id":"r2","protocol_version":3,"request":{"method":"install_credentials","params":{"data":"e30="}}}"#;
        let DecodedRequest::Known(envelope) = decode_request(request).unwrap() else {
            panic!("install_credentials should decode");
        };
        assert_eq!(
            envelope.request,
            WorkerRequest::InstallCredentials {
                data: "e30=".into()
            }
        );
    }

    #[test]
    fn malformed_known_method_is_an_invalid_request() {
        let request = br#"{"request_id":"r1","protocol_version":1,"request":{"method":"subscribe","params":{}}}"#;
        assert!(matches!(
            decode_request(request).unwrap(),
            DecodedRequest::Invalid { .. }
        ));
    }
}
