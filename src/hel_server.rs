//! Daemon-owned, phone-oriented control surface for Hel.
//!
//! The server deliberately owns no controller business logic. It publishes a
//! redacted projection of controller state and forwards validated, typed
//! actions through a channel supplied by the controller.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Component, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result as AnyResult};
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::header::{
    CACHE_CONTROL, CONTENT_TYPE, COOKIE, HeaderValue, LOCATION, REFERRER_POLICY, SET_COOKIE,
};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::hel_chat::BrowserTranscript;
use crate::hel_config::{HelConfig, TargetTemplate, validate_id};
use crate::hel_elicitation::{ElicitationRequest, ElicitationResponse, MAX_ELICITATION_BYTES};
use crate::hel_state::{HelState, SessionState};

const COOKIE_NAME: &str = "hel_viewer_session";
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const EPHEMERAL_SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_BODY_BYTES: usize = 128 * 1024;
const MAX_CODE_FAILURES: u32 = 5;
const CODE_LOCKOUT_BASE: Duration = Duration::from_secs(30);
const CODE_LOCKOUT_CAP: Duration = Duration::from_secs(60 * 60);
const MAX_TITLE_CHARS: usize = 120;
const MAX_PROMPT_CHARS: usize = 64 * 1024;
/// Image prompts need far more room than any other phone request. Browser
/// uploads are base64-encoded, so two ordinary photographs already exceed the
/// general body limit even when each one fits it. The larger bound therefore
/// stays scoped to the action route that carries prompts.
const MAX_PROMPT_BODY_BYTES: usize = 32 * 1024 * 1024;
const COOKIE_KEY_BYTES: usize = 32;
const COOKIE_KEY_FILE: &str = "phone-cookie-key";

/// Where the phone cookie signing key lives: beside Hel's other private
/// controller state, never in the shared config directory.
pub fn cookie_key_path() -> PathBuf {
    crate::hel_config::data_dir().join(COOKIE_KEY_FILE)
}

/// Load the phone cookie signing key, creating it on first use.
///
/// Session cookies are stateless, so this file is the only thing that keeps a
/// signed-in phone signed in across daemon restarts. Deleting it is
/// therefore the explicit sign-everyone-out gesture: the next start writes a
/// new key and every outstanding cookie stops validating. A missing file is
/// ordinary first use; an unreadable or too-short one is replaced loudly,
/// because refusing to start would be a worse answer than asking phones to
/// enter the viewer code again.
pub fn load_or_create_cookie_key(path: &std::path::Path) -> AnyResult<Vec<u8>> {
    match std::fs::read(path) {
        Ok(key) if key.len() >= COOKIE_KEY_BYTES => return Ok(key),
        Ok(key) => tracing::warn!(
            path = %path.display(),
            bytes = key.len(),
            "phone cookie key is shorter than {COOKIE_KEY_BYTES} bytes; generating a new key signs every phone out"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::warn!(
            path = %path.display(),
            "could not read the phone cookie key ({error}); generating a new key signs every phone out"
        ),
    }
    let key = generate_cookie_key()?;
    crate::hel_config::atomic_write(path, &key)
        .with_context(|| format!("persist Hel phone cookie key {}", path.display()))?;
    Ok(key.to_vec())
}

/// Options for the daemon's phone service.
///
/// `ServerOptions::new` generates both the six-digit viewer code and an
/// ephemeral cookie key. A caller that wants cookies to survive server
/// restarts installs a persisted key with `set_cookie_key`, which
/// `load_or_create_cookie_key` reads from its private Hel data directory. The
/// key and viewer code are intentionally omitted from `Debug` output.
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub snapshot_rx: watch::Receiver<ViewerSnapshot>,
    pub conversation_rx: watch::Receiver<BTreeMap<String, BrowserTranscript>>,
    pub action_tx: mpsc::Sender<ControllerRequest>,
    pub receipt_tx: mpsc::Sender<ReadReceiptRequest>,
    pub shutdown: CancellationToken,
    pub session_ttl: Duration,
    /// Keep this enabled for direct HTTPS or an HTTPS reverse proxy. It may be
    /// disabled only for an explicitly trusted HTTP development endpoint.
    pub secure_cookie: bool,
    tls_config: Option<axum_server::tls_rustls::RustlsConfig>,
    viewer_code: String,
    login_token: String,
    cookie_key: Vec<u8>,
}

impl ServerOptions {
    pub fn new(
        bind: SocketAddr,
        snapshot_rx: watch::Receiver<ViewerSnapshot>,
        conversation_rx: watch::Receiver<BTreeMap<String, BrowserTranscript>>,
        action_tx: mpsc::Sender<ControllerRequest>,
        receipt_tx: mpsc::Sender<ReadReceiptRequest>,
    ) -> AnyResult<Self> {
        Ok(Self {
            bind,
            snapshot_rx,
            conversation_rx,
            action_tx,
            receipt_tx,
            shutdown: CancellationToken::new(),
            session_ttl: DEFAULT_SESSION_TTL,
            secure_cookie: true,
            tls_config: None,
            viewer_code: generate_viewer_code()?,
            login_token: generate_login_token()?,
            cookie_key: generate_cookie_key()?.to_vec(),
        })
    }

    pub fn viewer_code(&self) -> &str {
        &self.viewer_code
    }

    pub fn login_token(&self) -> &str {
        &self.login_token
    }

    /// Serve HTTPS directly using the supplied Rustls configuration. Hel's
    /// CLI can load its persisted certificate (including a Tailscale-issued
    /// certificate) and pass it here without coupling this module to disk.
    pub fn set_tls_config(&mut self, config: axum_server::tls_rustls::RustlsConfig) {
        self.tls_config = Some(config);
        self.secure_cookie = true;
    }

    /// Install a persisted signing key. Rotating this value signs every phone
    /// out without maintaining a server-side session database.
    pub fn set_cookie_key(&mut self, key: Vec<u8>) -> AnyResult<()> {
        anyhow::ensure!(
            key.len() >= COOKIE_KEY_BYTES,
            "cookie signing key must be at least {COOKIE_KEY_BYTES} bytes"
        );
        self.cookie_key = key;
        Ok(())
    }

    #[cfg(test)]
    fn with_test_credentials(mut self, code: &str, key: &[u8]) -> Self {
        self.viewer_code = code.to_string();
        self.login_token = "test-login-token".into();
        self.cookie_key = key.to_vec();
        self.secure_cookie = false;
        self
    }
}

/// Run the phone server until its shutdown token is cancelled.
///
/// This binds only the requested listener. It does not daemonize, provision a
/// target, or keep sessions alive: controller availability is required, just
/// like MJ's explicit remote-viewer model.
pub async fn run_server(options: ServerOptions) -> AnyResult<()> {
    let mut options = options;
    let bind = options.bind;
    let shutdown = options.shutdown.clone();
    let viewer_code = options.viewer_code.clone();
    let tls_config = options.tls_config.take();
    let app = router(options);
    println!("Hel viewer code: {viewer_code}");
    if let Some(tls_config) = tls_config {
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            shutdown.cancelled().await;
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(2)));
        });
        axum_server::bind_rustls(bind, tls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .context("run Hel HTTPS phone server")
    } else {
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .with_context(|| format!("bind Hel phone server to {bind}"))?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .context("run Hel HTTP phone server")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerSnapshot {
    pub revision: u64,
    pub generated_at: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workspaces: Vec<ViewerWorkspace>,
    pub sessions: Vec<ViewerSession>,
    pub profiles: Vec<ViewerProfile>,
    pub targets: Vec<ViewerTarget>,
    pub bundles: Vec<ViewerBundle>,
}

impl ViewerSnapshot {
    /// Build the public projection. In particular, this never copies profile
    /// homes/environment, SSH hosts/keys, container environment, AWS details,
    /// concrete resource locators, native session IDs, or raw error strings.
    pub fn from_config_state(config: &HelConfig, state: &HelState, revision: u64) -> Self {
        let sessions = state
            .sessions
            .values()
            .map(|session| {
                let finish = session
                    .state
                    .is_active()
                    .then(|| crate::hel_controller::session_finish_effect(session).ok())
                    .flatten()
                    .map(ViewerFinish::from_effect);
                ViewerSession {
                    id: session.id.clone(),
                    workspace_id: session.workspace_id.clone(),
                    title: session.display_title().to_owned(),
                    harness_kind: session.harness_kind.id().into(),
                    profile_id: session.last_profile.clone(),
                    bundle_id: session.bundle_id.clone(),
                    target_id: session.target_template_id.clone(),
                    state: session_state_name(session.state).into(),
                    created_at: session.created_at.clone(),
                    updated_at: session.updated_at.clone(),
                    has_error: session.last_error.is_some(),
                    preview: Vec::new(),
                    queued_prompts: Vec::new(),
                    active_user_shells: Vec::new(),
                    pending_elicitations: Vec::new(),
                    conversation_available: false,
                    prompt_images_supported: false,
                    finish,
                    incompatible_resume_targets: config
                        .targets
                        .keys()
                        .filter(|target_id| {
                            crate::hel_controller::resume_compatibility(session, config, target_id)
                                .is_err()
                        })
                        .cloned()
                        .collect(),
                }
            })
            .collect();
        let profiles = config
            .profiles
            .iter()
            .map(|(id, profile)| ViewerProfile {
                id: id.clone(),
                harness_kind: profile.kind.id().into(),
                quota: None,
            })
            .collect();
        let targets = config
            .targets
            .iter()
            .map(|(id, target)| ViewerTarget {
                id: id.clone(),
                kind: target_kind_name(target).into(),
                requires_project_directory: matches!(
                    target,
                    TargetTemplate::LocalBare | TargetTemplate::SshBare { .. }
                ),
            })
            .collect();
        let bundles = config
            .bundles
            .iter()
            .map(|(id, bundle)| ViewerBundle {
                id: id.clone(),
                primary_repository: bundle.primary_repo.clone(),
                repositories: bundle
                    .repositories
                    .iter()
                    .map(|repository| ViewerRepository {
                        id: repository.id.clone(),
                        github: repository.github.clone(),
                        destination: repository.destination.to_string_lossy().into_owned(),
                    })
                    .collect(),
            })
            .collect();
        Self {
            revision,
            generated_at: now_unix().to_string(),
            workspaces: Vec::new(),
            sessions,
            profiles,
            targets,
            bundles,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerSession {
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub workspace_id: String,
    pub title: String,
    pub harness_kind: String,
    pub profile_id: String,
    pub bundle_id: String,
    pub target_id: String,
    pub state: String,
    pub created_at: String,
    pub updated_at: String,
    pub has_error: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preview: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued_prompts: Vec<ViewerQueuedPrompt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_user_shells: Vec<ViewerUserShell>,
    /// Form questions the session is blocked on, published so a phone can
    /// answer them. These are the agent's own questions, already visible in
    /// the transcript, so they travel whole rather than redacted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_elicitations: Vec<ElicitationRequest>,
    pub conversation_available: bool,
    /// Privacy-safe presentation of the exact resource Finish will release.
    /// Raw target locator fields never cross the viewer boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish: Option<ViewerFinish>,
    /// Whether this session's agent advertised support for image content in
    /// prompts. The viewer offers the image controls only when it did, and the
    /// server refuses images for a session that did not.
    #[serde(default)]
    pub prompt_images_supported: bool,
    /// Target ids this session cannot resume on. Only the ids travel: the
    /// controller's reasons name project paths and SSH hosts, which this
    /// projection deliberately keeps on the controller.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub incompatible_resume_targets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerFinish {
    pub kind: String,
    pub consequence: String,
    pub primary_action: String,
}

impl ViewerFinish {
    fn from_effect(effect: crate::hel_controller::SessionFinishEffect) -> Self {
        Self {
            kind: effect.kind().into(),
            consequence: effect.consequence().into(),
            primary_action: effect.primary_action().into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerWorkspace {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerQueuedPrompt {
    pub id: String,
    pub text: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerUserShell {
    pub id: String,
    pub command: String,
    pub started_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerProfile {
    pub id: String,
    pub harness_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<ViewerQuota>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerQuota {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<String>,
    pub stale: bool,
    /// Error state only. Raw vendor errors may contain paths or account data
    /// and remain on the controller.
    pub has_error: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerTarget {
    pub id: String,
    pub kind: String,
    pub requires_project_directory: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerBundle {
    pub id: String,
    pub primary_repository: String,
    pub repositories: Vec<ViewerRepository>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerRepository {
    pub id: String,
    pub github: Option<String>,
    pub destination: String,
}

/// The complete set of operations a phone may ask the controller to perform.
/// Destructive force-cleanup and secret/config editing are intentionally not
/// representable here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ControllerAction {
    New {
        #[serde(default)]
        workspace_id: String,
        profile_id: String,
        bundle_id: String,
        target_id: String,
        title: String,
        #[serde(default)]
        project_directory: Option<PathBuf>,
    },
    Resume {
        session_id: String,
        profile_id: String,
        target_id: String,
        queue: ResumeQueueDisposition,
    },
    Open {
        session_id: String,
    },
    Prompt {
        session_id: String,
        text: String,
        /// Images to send with the prompt. The controller turns each one into
        /// the ACP image content block its prompt path already speaks.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        images: Vec<ViewerPromptImage>,
    },
    RunShell {
        session_id: String,
        command: String,
    },
    CancelShell {
        session_id: String,
        shell_command_id: String,
    },
    Finish {
        session_id: String,
    },
    Cancel {
        session_id: String,
    },
    RemoveQueuedPrompt {
        session_id: String,
        queue_id: String,
    },
    /// Answer one of the session's pending form questions.
    RespondElicitation {
        session_id: String,
        elicitation_id: String,
        response: ElicitationResponse,
    },
}

/// One image a phone attached to a prompt, still base64-encoded as the browser
/// read it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerPromptImage {
    pub data_base64: String,
    pub mime_type: String,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResumeQueueDisposition {
    Start,
    Discard,
}

/// The controller's answer to one phone action.
///
/// The answer means "accepted", not "finished": provisioning, resume and Finish
/// run for minutes, and a phone on a mobile network drops a request held open
/// that long. How the action then goes travels in snapshots — session state,
/// queued prompts, transcripts, and `has_error`.
///
/// Only the outcome crosses this boundary. The controller's own failure text
/// names profile homes, project paths and SSH hosts, so it stays on the
/// controller and the phone gets a fixed message it can act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionOutcome {
    /// Admitted and now running; watch the snapshot for what happens next.
    Accepted,
    /// The controller already runs as many phone actions as it allows.
    Busy,
    /// This session already has an operation running.
    SessionBusy,
    /// A cancel found no operation to cancel.
    NotCancellable,
    /// The controller could not start the action at all.
    Failed,
}

impl ActionOutcome {
    /// The reply an outcome owes the phone, or `None` when it was accepted.
    const fn rejection(self) -> Option<ApiError> {
        match self {
            Self::Accepted => None,
            Self::Busy => Some(ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "the controller is at its concurrent action limit; retry shortly",
            )),
            Self::SessionBusy => Some(ApiError::new(
                StatusCode::CONFLICT,
                "another operation is already running for this session",
            )),
            Self::NotCancellable => Some(ApiError::new(
                StatusCode::CONFLICT,
                "the session has no cancellable operation",
            )),
            Self::Failed => Some(ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the controller could not start this action",
            )),
        }
    }
}

#[derive(Debug)]
pub struct ControllerRequest {
    pub action: ControllerAction,
    pub reply: tokio::sync::oneshot::Sender<ActionOutcome>,
}

/// A phone acknowledging how far it has read a conversation.
///
/// This deliberately is not a `ControllerAction`: the viewer posts it after
/// every conversation fetch, and a fetch follows every revision. Routing it
/// through the action pipeline made each receipt reload the controller, bump
/// the revision and broadcast a snapshot, which triggered the next fetch, so
/// viewer and controller never went quiet; it also consumed the session's
/// single action slot, intermittently rejecting real actions. A receipt
/// therefore travels on its own channel and only persists one cursor field.
#[derive(Debug)]
pub struct ReadReceiptRequest {
    pub client_id: String,
    pub session_id: String,
    pub through: u64,
    pub reply: tokio::sync::oneshot::Sender<Result<(), String>>,
}

#[derive(Clone)]
struct ServerState {
    snapshot_rx: watch::Receiver<ViewerSnapshot>,
    conversation_rx: watch::Receiver<BTreeMap<String, BrowserTranscript>>,
    action_tx: mpsc::Sender<ControllerRequest>,
    receipt_tx: mpsc::Sender<ReadReceiptRequest>,
    viewer_code: Arc<str>,
    login_token: Arc<str>,
    cookie_key: Arc<[u8]>,
    session_ttl: Duration,
    secure_cookie: bool,
    code_guard: Arc<Mutex<CodeGuard>>,
}

/// Online-guessing defence for the deliberately small viewer code.
///
/// Five wrong codes lock the endpoint, and each further lockout lasts twice as
/// long as the one before it, up to an hour. The escalation count survives an
/// expired lockout, so a script cannot recover its full allowance by waiting;
/// a correct code clears the whole history, so one mistyped digit still costs
/// at most a single short wait.
#[derive(Debug, Default)]
struct CodeGuard {
    failures: u32,
    lockouts: u32,
    locked_until: Option<Instant>,
}

impl CodeGuard {
    fn locked_at(&mut self, now: Instant) -> bool {
        match self.locked_until {
            Some(until) if now < until => true,
            Some(_) => {
                // The wait is served: allow a fresh run of attempts, but keep
                // the escalation history that makes the next wait longer.
                self.locked_until = None;
                self.failures = 0;
                false
            }
            None => false,
        }
    }

    fn record_failure_at(&mut self, now: Instant) {
        self.failures = self.failures.saturating_add(1);
        if self.failures < MAX_CODE_FAILURES {
            return;
        }
        self.failures = 0;
        self.lockouts = self.lockouts.saturating_add(1);
        self.locked_until = Some(now + code_lockout(self.lockouts));
    }
}

/// Doubling backoff, capped so the owner of a locked-out server is never shut
/// out for longer than it takes to notice.
fn code_lockout(lockouts: u32) -> Duration {
    let multiplier = 1_u32
        .checked_shl(lockouts.saturating_sub(1))
        .unwrap_or(u32::MAX);
    CODE_LOCKOUT_BASE
        .saturating_mul(multiplier)
        .min(CODE_LOCKOUT_CAP)
}

fn router(options: ServerOptions) -> Router {
    let state = ServerState {
        snapshot_rx: options.snapshot_rx,
        conversation_rx: options.conversation_rx,
        action_tx: options.action_tx,
        receipt_tx: options.receipt_tx,
        viewer_code: options.viewer_code.into(),
        login_token: options.login_token.into(),
        cookie_key: options.cookie_key.into(),
        session_ttl: options.session_ttl,
        secure_cookie: options.secure_cookie,
        code_guard: Arc::new(Mutex::new(CodeGuard::default())),
    };
    let protected = Router::new()
        .route("/api/snapshot", get(snapshot))
        .route("/api/conversations/{session_id}", get(conversation))
        .route(
            "/api/conversations/{session_id}/read",
            post(mark_conversation_read),
        )
        .route("/api/events", get(events))
        .route(
            "/api/actions",
            post(action).layer(DefaultBodyLimit::max(MAX_PROMPT_BODY_BYTES)),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ));
    Router::new()
        .route("/", get(viewer))
        .route("/login", get(viewer))
        .route("/manifest.webmanifest", get(manifest))
        .route("/service-worker.js", get(service_worker))
        .route("/icon.svg", get(icon))
        .route("/auth/session", post(create_session).delete(clear_session))
        .route("/auth/login", get(create_session_from_query))
        .merge(protected)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

async fn require_session(
    State(state): State<ServerState>,
    request: Request,
    next: Next,
) -> Result<Response<Body>, ApiError> {
    let cookie = request
        .headers()
        .get(COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|header| cookie_value(header, COOKIE_NAME));
    if cookie.is_some_and(|value| session_cookie_valid(&state.cookie_key, value, now_unix())) {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoginRequest {
    code: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoginQuery {
    token: String,
}

async fn create_session_from_query(
    State(state): State<ServerState>,
    Query(query): Query<LoginQuery>,
) -> Result<Response<Body>, ApiError> {
    if !constant_time_eq(state.login_token.as_bytes(), query.token.trim().as_bytes()) {
        return Err(ApiError::unauthorized());
    }
    let mut response = issue_session_cookie(&state, StatusCode::SEE_OTHER)?;
    response
        .headers_mut()
        .insert(LOCATION, HeaderValue::from_static("/"));
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert(REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    Ok(response)
}

async fn create_session(
    State(state): State<ServerState>,
    Json(request): Json<LoginRequest>,
) -> Result<Response<Body>, ApiError> {
    if code_locked(&state) {
        return Err(ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "too many incorrect codes; wait and try again",
        ));
    }
    if !constant_time_eq(state.viewer_code.as_bytes(), request.code.trim().as_bytes()) {
        record_code_failure(&state);
        return Err(ApiError::unauthorized());
    }
    reset_code_failures(&state);
    issue_session_cookie(&state, StatusCode::NO_CONTENT)
}

fn issue_session_cookie(
    state: &ServerState,
    status: StatusCode,
) -> Result<Response<Body>, ApiError> {
    let ephemeral = state.session_ttl.is_zero();
    let validity = if ephemeral {
        EPHEMERAL_SESSION_TTL
    } else {
        state.session_ttl
    };
    let value = signed_cookie_value(
        &state.cookie_key,
        now_unix().saturating_add(validity.as_secs()),
    );
    let cookie = session_cookie_header(
        &value,
        (!ephemeral).then_some(validity.as_secs()),
        state.secure_cookie,
    )?;
    let mut response = status.into_response();
    response.headers_mut().insert(SET_COOKIE, cookie);
    Ok(response)
}

async fn clear_session(State(state): State<ServerState>) -> Response<Body> {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert(SET_COOKIE, clear_cookie_header(state.secure_cookie));
    response
}

async fn snapshot(State(state): State<ServerState>) -> Response<Body> {
    let mut response = Json(state.snapshot_rx.borrow().clone()).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// Hand one validated action to the controller and answer as soon as the
/// controller accepts it. Waiting for completion would hold the request open
/// for the whole of a provision, resume or Finish, which mobile networks end
/// long before the work does — reporting failure for an action that is in fact
/// still running.
async fn action(
    State(state): State<ServerState>,
    Json(action): Json<ControllerAction>,
) -> Result<StatusCode, ApiError> {
    validate_action(&action, &state.snapshot_rx.borrow())?;
    let action = decode_prompt_images_off_task(action).await?;
    let (reply, outcome) = tokio::sync::oneshot::channel();
    state
        .action_tx
        .send(ControllerRequest { action, reply })
        .await
        .map_err(|_| ApiError::controller_unavailable())?;
    let outcome = outcome
        .await
        .map_err(|_| ApiError::controller_unavailable())?;
    match outcome.rejection() {
        Some(rejection) => Err(rejection),
        None => Ok(StatusCode::ACCEPTED),
    }
}

#[derive(Debug, Deserialize)]
struct ConversationQuery {
    after_seq: Option<u64>,
}

async fn conversation(
    State(state): State<ServerState>,
    Path(session_id): Path<String>,
    Query(query): Query<ConversationQuery>,
) -> Result<Json<BrowserTranscript>, ApiError> {
    validate_public_id(&session_id)?;
    let conversations = state.conversation_rx.borrow();
    let transcript = conversations
        .get(&session_id)
        .ok_or_else(|| ApiError::not_found("conversation unavailable"))?;
    let mut response = transcript.clone();
    if let Some(after) = query.after_seq {
        response.reset = after < response.window_start_seq;
        if !response.reset {
            response.entries.retain(|entry| entry.updated_seq > after);
        }
    }
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadRequest {
    through: u64,
}

async fn mark_conversation_read(
    State(state): State<ServerState>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ReadRequest>,
) -> Result<StatusCode, ApiError> {
    validate_public_id(&session_id)?;
    require_session_record(&state.snapshot_rx.borrow(), &session_id)?;
    let (reply, result) = tokio::sync::oneshot::channel();
    let cookie = headers
        .get(COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|header| cookie_value(header, COOKIE_NAME))
        .ok_or_else(ApiError::unauthorized)?;
    state
        .receipt_tx
        .send(ReadReceiptRequest {
            client_id: format!("phone:{cookie}"),
            session_id,
            through: request.through,
            reply,
        })
        .await
        .map_err(|_| ApiError::controller_unavailable())?;
    result
        .await
        .map_err(|_| ApiError::controller_unavailable())?
        .map_err(|_| ApiError::new(StatusCode::CONFLICT, "read receipt failed"))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn events(State(state): State<ServerState>) -> impl IntoResponse {
    let mut snapshots = state.snapshot_rx.clone();
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(8);
    tokio::spawn(async move {
        let initial = snapshots.borrow().revision;
        if tx
            .send(Ok(Event::default()
                .event("revision")
                .data(initial.to_string())))
            .await
            .is_err()
        {
            return;
        }
        while snapshots.changed().await.is_ok() {
            let revision = snapshots.borrow_and_update().revision;
            if tx
                .send(Ok(Event::default()
                    .event("revision")
                    .data(revision.to_string())))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    Sse::new(ReceiverStream::new(rx)).keep_alive(KeepAlive::default())
}

/// Check attached images without decoding megabytes of base64 on the task that
/// serves the request. Everything else about an action is cheap enough to
/// check inline; a full multi-image prompt is not.
async fn decode_prompt_images_off_task(
    action: ControllerAction,
) -> Result<ControllerAction, ApiError> {
    let ControllerAction::Prompt { images, .. } = &action else {
        return Ok(action);
    };
    if images.is_empty() {
        return Ok(action);
    }
    tokio::task::spawn_blocking(move || {
        let ControllerAction::Prompt { images, .. } = &action else {
            unreachable!("only prompt actions carry images")
        };
        validate_prompt_images(images)?;
        Ok(action)
    })
    .await
    .map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "the server could not check the attached images",
        )
    })?
}

fn validate_prompt_images(images: &[ViewerPromptImage]) -> Result<(), ApiError> {
    for image in images {
        if !image.mime_type.starts_with("image/") {
            return Err(ApiError::bad_request(
                "image mime type must start with image/",
            ));
        }
        if image.width == 0 || image.height == 0 {
            return Err(ApiError::bad_request(
                "image dimensions must be greater than zero",
            ));
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&image.data_base64)
            .map_err(|_| ApiError::bad_request("image data must be valid base64"))?;
        if bytes.is_empty() {
            return Err(ApiError::bad_request("image data must not be empty"));
        }
    }
    Ok(())
}

fn validate_action(action: &ControllerAction, snapshot: &ViewerSnapshot) -> Result<(), ApiError> {
    match action {
        ControllerAction::New {
            profile_id,
            bundle_id,
            target_id,
            title,
            project_directory,
            ..
        } => {
            validate_public_id(profile_id)?;
            validate_public_id(bundle_id)?;
            validate_public_id(target_id)?;
            validate_title(title)?;
            require_profile(snapshot, profile_id)?;
            require_bundle(snapshot, bundle_id)?;
            let target = require_target(snapshot, target_id)?;
            if target.requires_project_directory != project_directory.is_some() {
                return Err(ApiError::bad_request(
                    "project_directory is required exactly for bare targets",
                ));
            }
            if let Some(directory) = project_directory
                && (!directory.is_absolute()
                    || directory
                        .components()
                        .any(|component| component == Component::ParentDir))
            {
                return Err(ApiError::bad_request(
                    "project_directory must be an absolute safe path",
                ));
            }
        }
        ControllerAction::Resume {
            session_id,
            profile_id,
            target_id,
            ..
        } => {
            validate_public_id(session_id)?;
            validate_public_id(profile_id)?;
            validate_public_id(target_id)?;
            let session = require_session_record(snapshot, session_id)?;
            if session.state != "saved" {
                return Err(ApiError::bad_request("only saved sessions can be resumed"));
            }
            require_profile(snapshot, profile_id)?;
            require_target(snapshot, target_id)?;
            if session
                .incompatible_resume_targets
                .iter()
                .any(|incompatible| incompatible == target_id)
            {
                return Err(ApiError::bad_request(
                    "this session cannot resume on that target",
                ));
            }
        }
        ControllerAction::Finish { session_id } => {
            validate_public_id(session_id)?;
            let session = require_session_record(snapshot, session_id)?;
            if session.finish.is_none() {
                return Err(ApiError::bad_request(
                    "only active sessions with a live target can be finished",
                ));
            }
        }
        ControllerAction::Open { session_id } | ControllerAction::Cancel { session_id } => {
            validate_public_id(session_id)?;
            require_session_record(snapshot, session_id)?;
        }
        ControllerAction::Prompt {
            session_id,
            text,
            images,
        } => {
            validate_public_id(session_id)?;
            let session = require_session_record(snapshot, session_id)?;
            if text.starts_with('!') {
                return Err(ApiError::bad_request(
                    "leading ! is reserved for shell commands",
                ));
            }
            if text.chars().count() > MAX_PROMPT_CHARS {
                return Err(ApiError::bad_request(
                    "prompt must contain 1-65536 characters",
                ));
            }
            if text.trim().is_empty() && images.is_empty() {
                return Err(ApiError::bad_request(
                    "prompt must contain text or an image",
                ));
            }
            if !images.is_empty() && !session.prompt_images_supported {
                return Err(ApiError::bad_request(
                    "this session does not support image prompts",
                ));
            }
        }
        ControllerAction::RunShell {
            session_id,
            command,
        } => {
            validate_public_id(session_id)?;
            require_session_record(snapshot, session_id)?;
            if command.trim().is_empty() || command.chars().count() > MAX_PROMPT_CHARS {
                return Err(ApiError::bad_request(
                    "shell command must contain 1-65536 characters",
                ));
            }
        }
        ControllerAction::CancelShell {
            session_id,
            shell_command_id,
        } => {
            validate_public_id(session_id)?;
            validate_public_id(shell_command_id)?;
            let session = require_session_record(snapshot, session_id)?;
            if !session
                .active_user_shells
                .iter()
                .any(|shell| shell.id == *shell_command_id)
            {
                return Err(ApiError::bad_request("unknown active shell command"));
            }
        }
        ControllerAction::RemoveQueuedPrompt {
            session_id,
            queue_id,
        } => {
            validate_public_id(session_id)?;
            validate_public_id(queue_id)?;
            require_session_record(snapshot, session_id)?;
        }
        ControllerAction::RespondElicitation {
            session_id,
            elicitation_id,
            response,
        } => {
            validate_public_id(session_id)?;
            validate_public_id(elicitation_id)?;
            let session = require_session_record(snapshot, session_id)?;
            let request = session
                .pending_elicitations
                .iter()
                .find(|request| request.id == *elicitation_id)
                .ok_or_else(|| ApiError::not_found("unknown elicitation"))?;
            if serde_json::to_vec(response).map_or(usize::MAX, |encoded| encoded.len())
                > MAX_ELICITATION_BYTES
            {
                return Err(ApiError::bad_request("elicitation answer is too large"));
            }
            // The answer has to satisfy the question the agent actually asked.
            // A phone can post one for a request the session has already
            // replaced, and forwarding that would answer a live question with
            // content the agent never offered.
            if request.validate_response(response).is_err() {
                return Err(ApiError::bad_request(
                    "the answer does not match this elicitation request",
                ));
            }
        }
    }
    Ok(())
}

fn validate_public_id(id: &str) -> Result<(), ApiError> {
    validate_id("request", id).map_err(|_| ApiError::bad_request("invalid id"))
}

fn validate_title(title: &str) -> Result<(), ApiError> {
    if title.trim().is_empty() || title.chars().count() > MAX_TITLE_CHARS {
        Err(ApiError::bad_request("title must contain 1-120 characters"))
    } else {
        Ok(())
    }
}

fn require_session_record<'a>(
    snapshot: &'a ViewerSnapshot,
    id: &str,
) -> Result<&'a ViewerSession, ApiError> {
    snapshot
        .sessions
        .iter()
        .find(|session| session.id == id)
        .ok_or_else(|| ApiError::not_found("unknown session"))
}

fn require_profile<'a>(
    snapshot: &'a ViewerSnapshot,
    id: &str,
) -> Result<&'a ViewerProfile, ApiError> {
    snapshot
        .profiles
        .iter()
        .find(|profile| profile.id == id)
        .ok_or_else(|| ApiError::bad_request("unknown profile"))
}

fn require_target<'a>(
    snapshot: &'a ViewerSnapshot,
    id: &str,
) -> Result<&'a ViewerTarget, ApiError> {
    snapshot
        .targets
        .iter()
        .find(|target| target.id == id)
        .ok_or_else(|| ApiError::bad_request("unknown target"))
}

fn require_bundle(snapshot: &ViewerSnapshot, id: &str) -> Result<(), ApiError> {
    snapshot
        .bundles
        .iter()
        .any(|bundle| bundle.id == id)
        .then_some(())
        .ok_or_else(|| ApiError::bad_request("unknown bundle"))
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: &'static str,
}

impl ApiError {
    const fn new(status: StatusCode, message: &'static str) -> Self {
        Self { status, message }
    }

    const fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized")
    }

    const fn bad_request(message: &'static str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    const fn not_found(message: &'static str) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    const fn controller_unavailable() -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "controller unavailable")
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response<Body> {
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

fn code_locked(state: &ServerState) -> bool {
    state
        .code_guard
        .lock()
        .expect("viewer code guard poisoned")
        .locked_at(Instant::now())
}

fn record_code_failure(state: &ServerState) {
    state
        .code_guard
        .lock()
        .expect("viewer code guard poisoned")
        .record_failure_at(Instant::now());
}

fn reset_code_failures(state: &ServerState) {
    *state.code_guard.lock().expect("viewer code guard poisoned") = CodeGuard::default();
}

fn generate_viewer_code() -> AnyResult<String> {
    // Rejection sampling avoids modulo bias in the deliberately small code
    // space. Online attempts are separately rate-limited.
    const RANGE: u32 = 1_000_000;
    const LIMIT: u32 = u32::MAX - (u32::MAX % RANGE);
    loop {
        let mut bytes = [0_u8; 4];
        getrandom::fill(&mut bytes)
            .map_err(|error| anyhow::anyhow!("generate Hel viewer code: {error}"))?;
        let value = u32::from_le_bytes(bytes);
        if value < LIMIT {
            return Ok(format!("{:06}", value % RANGE));
        }
    }
}

fn generate_login_token() -> AnyResult<String> {
    let mut token = [0_u8; 32];
    getrandom::fill(&mut token)
        .map_err(|error| anyhow::anyhow!("generate Hel viewer login token: {error}"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token))
}

fn generate_cookie_key() -> AnyResult<[u8; COOKIE_KEY_BYTES]> {
    let mut key = [0_u8; COOKIE_KEY_BYTES];
    getrandom::fill(&mut key)
        .map_err(|error| anyhow::anyhow!("generate Hel cookie key: {error}"))?;
    Ok(key)
}

fn signed_cookie_value(key: &[u8], expiry: u64) -> String {
    let canonical = expiry.to_string();
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts arbitrary key lengths");
    mac.update(canonical.as_bytes());
    let signature =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{canonical}.{signature}")
}

fn session_cookie_valid(key: &[u8], value: &str, now: u64) -> bool {
    let Some((expiry, _)) = value.split_once('.') else {
        return false;
    };
    let Ok(expiry_value) = expiry.parse::<u64>() else {
        return false;
    };
    if now >= expiry_value {
        return false;
    }
    let expected = signed_cookie_value(key, expiry_value);
    constant_time_eq(expected.as_bytes(), value.as_bytes())
}

fn session_cookie_header(
    value: &str,
    max_age: Option<u64>,
    secure: bool,
) -> Result<HeaderValue, ApiError> {
    let mut header = format!("{COOKIE_NAME}={value}; Path=/; HttpOnly; SameSite=Strict");
    if secure {
        header.push_str("; Secure");
    }
    if let Some(max_age) = max_age {
        header.push_str(&format!("; Max-Age={max_age}"));
    }
    HeaderValue::from_str(&header)
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "cookie creation failed"))
}

fn clear_cookie_header(secure: bool) -> HeaderValue {
    let secure = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "{COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Strict{secure}; Max-Age=0"
    ))
    .expect("static cookie header is valid")
}

fn cookie_value<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    header
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find(|(cookie_name, _)| *cookie_name == name)
        .map(|(_, value)| value)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(u64::MAX)
}

const fn session_state_name(state: SessionState) -> &'static str {
    match state {
        SessionState::Provisioning => "provisioning",
        SessionState::Running => "running",
        SessionState::Disconnected => "disconnected",
        SessionState::Checkpointing => "checkpointing",
        SessionState::Closing => "finishing",
        SessionState::Destroying => "deleting",
        SessionState::Stopped => "saved",
        SessionState::Lost => "lost",
        SessionState::Error => "error",
        SessionState::DestroyedWithDataLoss => "destroyed-with-data-loss",
    }
}

const fn target_kind_name(target: &TargetTemplate) -> &'static str {
    match target {
        TargetTemplate::LocalBare => "local-bare",
        TargetTemplate::LocalPodman { .. } => "local-podman",
        TargetTemplate::AppleContainer { .. } => "apple-container",
        TargetTemplate::AwsEc2 { .. } => "aws-ec2",
        TargetTemplate::SshBare { .. } => "ssh-bare",
        TargetTemplate::SshPodman { .. } => "ssh-podman",
    }
}

async fn viewer() -> Response<Body> {
    static_response("text/html; charset=utf-8", VIEWER_HTML, true)
}

async fn manifest() -> Response<Body> {
    static_response("application/manifest+json", MANIFEST, false)
}

async fn service_worker() -> Response<Body> {
    static_response("text/javascript; charset=utf-8", SERVICE_WORKER, true)
}

async fn icon() -> Response<Body> {
    static_response("image/svg+xml", ICON, false)
}

fn static_response(
    content_type: &'static str,
    body: &'static str,
    no_store: bool,
) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    if no_store {
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    response
}

const MANIFEST: &str = r##"{
  "name": "Hel",
  "short_name": "Hel",
  "start_url": "/",
  "display": "standalone",
  "background_color": "#08090d",
  "theme_color": "#08090d",
  "icons": [{"src":"/icon.svg","sizes":"any","type":"image/svg+xml","purpose":"any maskable"}]
}"##;

const SERVICE_WORKER: &str = r#"
self.addEventListener('install', event => event.waitUntil(caches.open('hel-v1').then(cache => cache.addAll(['/', '/manifest.webmanifest', '/icon.svg']))));
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));
self.addEventListener('fetch', event => { if (event.request.method === 'GET' && !new URL(event.request.url).pathname.startsWith('/api/')) event.respondWith(fetch(event.request).catch(() => caches.match(event.request))); });
"#;

const ICON: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><rect width="512" height="512" rx="100" fill="#08090d"/><path d="M132 88v336M380 88v336M132 256h248" stroke="#b9ff5a" stroke-width="54" stroke-linecap="round"/></svg>"##;

const VIEWER_HTML: &str = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="theme-color" content="#08090d"><link rel="icon" href="/icon.svg"><link rel="manifest" href="/manifest.webmanifest"><title>Hel</title>
<style>:root{color-scheme:dark;font:16px system-ui;background:#08090d;color:#ecf2e5}body{margin:0;padding:env(safe-area-inset-top) 16px env(safe-area-inset-bottom);max-width:760px;margin:auto}header{display:flex;align-items:baseline;justify-content:space-between}h1{font-size:42px;letter-spacing:.06em;margin:22px 0 4px;color:#b9ff5a}.dim{color:#899184}.card{background:#13161d;border:1px solid #292e38;border-radius:14px;margin:12px 0;padding:14px}.row{display:flex;gap:8px;flex-wrap:wrap}button,input,select,textarea{font:inherit;color:inherit;background:#1d222b;border:1px solid #3b424e;border-radius:9px;padding:10px}button{background:#b9ff5a;color:#10140b;font-weight:700}button:disabled{opacity:.45}.danger{background:#ff786f}.secondary{background:#303743;color:#ecf2e5}.hidden{display:none}.pill{font-size:12px;border:1px solid #475043;border-radius:99px;padding:3px 8px}.pill.alert{border-color:#ff786f;color:#ff786f}.session h3{margin:0 0 8px}.session p{margin:5px 0}.preview{white-space:pre-wrap;border-left:2px solid #475043;padding-left:10px}.entry{border-left:3px solid #475043;padding:4px 0 4px 12px;margin:15px 0}.entry.user{border-color:#5dd9ff}.entry.agent{border-color:#91df62}.entry.thought,.entry.system{border-color:#59616d;color:#aab1a5}.entry.tool{border-color:#e2b34d}.entry.plan{border-color:#d985ff}.entry strong{display:block;margin-bottom:5px}.entry pre{font:inherit;white-space:pre-wrap;overflow-wrap:anywhere;margin:0}.queue-item{display:flex;gap:8px;align-items:start;justify-content:space-between;border-top:1px solid #292e38;padding:8px 0}.queue-item span{white-space:pre-wrap;overflow-wrap:anywhere}.elicitation{border-color:#d985ff}.elicitation-message{font:inherit;white-space:pre-wrap;overflow-wrap:anywhere;margin:0 0 10px}.elicitation-field{display:flex;flex-direction:column;gap:4px;margin:10px 0}.elicitation-field select[multiple]{min-height:120px}.elicitation-field input[type=checkbox]{align-self:start;width:22px;height:22px}#prompt-text{display:block;width:100%;box-sizing:border-box;min-height:76px;max-height:40vh;overflow-y:auto;white-space:pre-wrap;overflow-wrap:anywhere;background:#1d222b;border:1px solid #3b424e;border-radius:9px;padding:10px}#prompt-text:empty::before{content:attr(data-placeholder);color:#899184;pointer-events:none}#attachments{margin-top:8px}.attachment{display:flex;align-items:center;gap:8px;border:1px solid #3b424e;border-radius:9px;padding:6px 8px}.attachment img{width:44px;height:44px;object-fit:cover;border-radius:6px}.attachment button{padding:2px 10px}#conversation-feed{min-height:30vh}dialog{max-width:560px;color:inherit;background:#13161d;border:1px solid #475043;border-radius:14px;padding:18px}dialog::backdrop{background:#000a}dialog h2{margin-top:0}</style></head>
<body><header><div><h1>HEL</h1><div class="dim">Welcome to Hel.</div></div><button id="logout" class="hidden">Sign out</button></header>
<main id="login" class="card"><h2>Unlock viewer</h2><p class="dim">Enter the six-digit code shown by <code>hel daemon status</code>.</p><form id="login-form" class="row"><input id="code" inputmode="numeric" autocomplete="one-time-code" pattern="[0-9]{6}" maxlength="6" placeholder="000000" required><button>Enter</button></form><p id="login-error"></p></main>
<main id="app" class="hidden"><section id="dashboard"><section class="card"><h2>New session</h2><form id="new-form" class="row"><input id="new-title" maxlength="120" placeholder="Session title" required><select id="new-profile" aria-label="Profile"></select><select id="new-bundle" aria-label="Bundle"></select><select id="new-target" aria-label="Target"></select><input id="new-project-directory" class="hidden" placeholder="Absolute project directory"><button>Start</button></form><p id="action-error"></p></section><section><h2>Active sessions</h2><div id="active-sessions"></div></section><section><h2>Saved sessions</h2><p class="dim">Saved sessions run no workers.</p><div id="saved-sessions"></div></section><section class="card"><h2>Configured</h2><div id="configured"></div></section></section><section id="conversation" class="hidden"><button id="back" class="secondary">← Dashboard</button><div class="card"><h2 id="conversation-title">Conversation</h2><span id="conversation-state" class="pill"></span><div id="conversation-feed"></div></div><div id="elicitations"></div><section class="card"><h3>Queued prompts</h3><div id="conversation-queue"></div><h3>Shell commands</h3><div id="conversation-shells"></div></section><form id="prompt-form" class="card"><div id="prompt-text" class="composer-input" contenteditable="true" role="textbox" aria-multiline="true" enterkeyhint="send" spellcheck="true" aria-label="Message the agent" data-placeholder="Message the agent or use !command"></div><div id="attachments" class="row" aria-label="Attached images"></div><div class="row"><button>Send or queue</button><button type="button" id="attach-image" class="secondary" aria-label="Attach one or more images" hidden>Images</button><input id="image-picker" type="file" accept="image/*" multiple hidden></div><p id="conversation-error"></p></form></section></main>
<dialog id="finish-dialog"><h2 id="finish-title">Finish session?</h2><p id="finish-work"></p><p id="finish-queue" class="dim"></p><p id="finish-consequence"></p><form method="dialog" class="row"><button value="cancel" class="secondary">Cancel</button><button id="finish-confirm" value="finish" class="danger">Finish</button></form></dialog>
<script>
const login=document.querySelector('#login'),app=document.querySelector('#app'),dashboard=document.querySelector('#dashboard'),conversation=document.querySelector('#conversation'),activeSessions=document.querySelector('#active-sessions'),savedSessions=document.querySelector('#saved-sessions'),configured=document.querySelector('#configured'),logout=document.querySelector('#logout'),newForm=document.querySelector('#new-form'),newProfile=document.querySelector('#new-profile'),newBundle=document.querySelector('#new-bundle'),newTarget=document.querySelector('#new-target'),newProjectDirectory=document.querySelector('#new-project-directory'),actionError=document.querySelector('#action-error'),feed=document.querySelector('#conversation-feed'),queue=document.querySelector('#conversation-queue'),shells=document.querySelector('#conversation-shells'),elicitations=document.querySelector('#elicitations'),promptText=document.querySelector('#prompt-text'),attachments=document.querySelector('#attachments'),attachImage=document.querySelector('#attach-image'),imagePicker=document.querySelector('#image-picker'),finishDialog=document.querySelector('#finish-dialog'),finishTitle=document.querySelector('#finish-title'),finishWork=document.querySelector('#finish-work'),finishQueue=document.querySelector('#finish-queue'),finishConsequence=document.querySelector('#finish-consequence'),finishConfirm=document.querySelector('#finish-confirm');let snapshot,currentSession,cursor=0,acknowledged=0,eventSource;const pendingFinishes=new Set();
async function request(url,options={}){const response=await fetch(url,{...options,headers:{'content-type':'application/json',...(options.headers||{})}});if(response.status===401)throw new Error('unauthorized');if(!response.ok){const body=await response.json().catch(()=>({}));throw new Error(body.error||response.statusText)}if(response.status===202||response.status===204)return null;return response.json()}
function options(items,selected){return items.map(x=>`<option value="${escapeAttr(x.id)}" ${x.id===selected?'selected':''}>${escapeHtml(x.id)}</option>`).join('')}
function syncProjectDirectory(){const required=snapshot?.targets.find(x=>x.id===newTarget.value)?.requires_project_directory===true;newProjectDirectory.classList.toggle('hidden',!required);newProjectDirectory.required=required;if(!required)newProjectDirectory.value=''}
function startEvents(){if(eventSource)eventSource.close();eventSource=new EventSource('/api/events');eventSource.addEventListener('revision',()=>{refresh();if(currentSession)loadConversation(true)})}
function sessionCard(x,saved){const finishing=pendingFinishes.has(x.id)||x.state==='finishing',state=finishing?'finishing':x.state;const details=`<p><span class="pill">${escapeHtml(state)}</span>${x.has_error?' <span class="pill alert">needs attention</span>':''}${x.pending_elicitations?.length?' <span class="pill alert">input needed</span>':''} ${escapeHtml(x.harness_kind)} · ${escapeHtml(x.profile_id)}</p><p class="dim">${escapeHtml(x.bundle_id)} → ${escapeHtml(x.target_id)} · ${(x.queued_prompts||[]).length} queued</p>${x.preview?.length?`<p class="preview">${x.preview.map(escapeHtml).join('\n')}</p>`:''}`;let actions;if(saved){actions=`<p class="dim">Saved · no worker running</p><div class="row"><button data-action="resume" data-id="${escapeAttr(x.id)}" data-profile="${escapeAttr(x.profile_id)}" data-target="${escapeAttr(x.target_id)}">Resume</button></div>`}else if(x.state==='provisioning'){actions=`<div class="row"><button class="danger" data-action="cancel" data-id="${escapeAttr(x.id)}">Cancel</button></div>`}else{actions=`<div class="row"><button data-action="open" data-id="${escapeAttr(x.id)}" ${x.conversation_available&&!finishing?'':'disabled'}>Open</button>${x.finish?`<button class="danger" data-action="finish" data-id="${escapeAttr(x.id)}" ${finishing?'disabled':''}>${finishing?'Finishing':'Finish'}</button>`:''}</div>`}return `<article class="card session"><h3>${escapeHtml(x.title)}</h3>${details}${actions}</article>`}
function renderSessions(){const saved=snapshot.sessions.filter(x=>x.state==='saved'),active=snapshot.sessions.filter(x=>x.state!=='saved');activeSessions.innerHTML=active.map(x=>sessionCard(x,false)).join('')||'<p class="dim">No active sessions.</p>';savedSessions.innerHTML=saved.map(x=>sessionCard(x,true)).join('')||'<p class="dim">No saved sessions.</p>'}
async function refresh(){try{snapshot=await request('/api/snapshot');for(const id of pendingFinishes){const session=snapshot.sessions.find(x=>x.id===id);if(!session||session.state==='saved'||session.has_error)pendingFinishes.delete(id)}login.classList.add('hidden');app.classList.remove('hidden');logout.classList.remove('hidden');if(!newProfile.value)newProfile.innerHTML=options(snapshot.profiles);if(!newBundle.value)newBundle.innerHTML=options(snapshot.bundles);if(!newTarget.value)newTarget.innerHTML=options(snapshot.targets);syncProjectDirectory();renderSessions();const profileRows=snapshot.profiles.map(p=>`<p><strong>${escapeHtml(p.id)}</strong> · ${escapeHtml(p.harness_kind)}<br><span class="dim">${p.quota?escapeHtml(p.quota.summary)+(p.quota.stale?' · stale':'')+(p.quota.has_error?' · unavailable':''):'quota unavailable'}</span></p>`).join('');configured.innerHTML=profileRows+`<p class="dim">${snapshot.targets.length} targets · ${snapshot.bundles.length} bundles</p>`;if(currentSession){const session=snapshot.sessions.find(x=>x.id===currentSession);if(!session?.conversation_available){showDashboard()}else{renderQueue(session);renderElicitations(session);renderAttachments();document.querySelector('#conversation-state').textContent=session.state}}if(!eventSource)startEvents();return true}catch(e){if(e.message==='unauthorized'){snapshot=undefined;currentSession=null;if(eventSource){eventSource.close();eventSource=undefined}login.classList.remove('hidden');app.classList.add('hidden');logout.classList.add('hidden')}return false}}
async function restoreRoute(){if(!await refresh())return;const match=location.hash.match(/^#conversation\/([A-Za-z0-9_-]+)$/);if(match)await openConversation(match[1])}
function renderQueue(session){queue.innerHTML=(session.queued_prompts||[]).map((x,i)=>`<div class="queue-item"><span>${i+1}. ${escapeHtml(x.text)}</span><button class="danger" data-queue-id="${escapeAttr(x.id)}">Remove</button></div>`).join('')||'<p class="dim">No queued prompts.</p>';shells.innerHTML=(session.active_user_shells||[]).map(x=>`<div class="queue-item"><span>$ ${escapeHtml(x.command)}</span><button class="danger" data-shell-id="${escapeAttr(x.id)}">Cancel</button></div>`).join('')||'<p class="dim">No running shells.</p>'}
// Every snapshot revision re-renders the conversation. Rebuilding a card the
// user is answering would wipe the half-filled form and steal focus, so each
// pending request keeps its live DOM until the request itself changes or
// leaves the snapshot.
const elicitationCards=new Map(),sentElicitations=new Set();
function elicitationKey(sessionId,id){return `${sessionId}\u001f${id}`}
function elicitationOptionLabel(option){return option.description?`${option.title} \u2014 ${option.description}`:option.title}
function elicitationControl(field){if(field.kind==='single_select'||field.kind==='multi_select'){const select=document.createElement('select');select.multiple=field.kind==='multi_select';if(!select.multiple&&!field.required)select.appendChild(new Option('',''));for(const option of field.options||[])select.appendChild(new Option(elicitationOptionLabel(option),option.value));if(field.kind==='single_select'&&field.default!=null)select.value=field.default;if(select.multiple&&(field.default||[]).length)for(const option of select.options)option.selected=field.default.includes(option.value);return select}const input=document.createElement('input');input.type=field.kind==='boolean'?'checkbox':(field.kind==='integer'||field.kind==='number'?'number':(field.secret?'password':'text'));if(field.kind==='integer')input.step='1';if(field.kind==='number')input.step='any';if(field.minimum!=null)input.min=field.minimum;if(field.maximum!=null)input.max=field.maximum;if(field.min_length!=null)input.minLength=field.min_length;if(field.max_length!=null)input.maxLength=field.max_length;if(field.pattern)input.pattern=field.pattern;if(field.kind==='boolean')input.checked=field.default===true;else if(field.default!=null)input.value=String(field.default);return input}
function elicitationFieldValue(field,control){if(field.kind==='multi_select'){const values=[...control.selectedOptions].map(option=>option.value);return values.length||field.required?values:undefined}if(field.kind==='boolean')return control.checked;if(control.value==='')return field.required&&(field.kind==='text'||field.kind==='single_select')?'':undefined;if(field.kind==='integer')return Number.parseInt(control.value,10);if(field.kind==='number')return Number(control.value);return control.value}
// Builds the controls and returns collect(), which reads them back as ACP
// content. A custom answer replaces the select it belongs to unless the
// request pairs it with one specific option, which is how Hel's chat form
// submits the same request.
function buildElicitationForm(form,request,register){const entries=[];for(const field of request.fields||[]){const wrapper=document.createElement('label');wrapper.className='elicitation-field';const label=document.createElement('span');label.textContent=`${field.title}${field.required?' *':''}`;const control=elicitationControl(field);control.required=Boolean(field.required)&&field.kind!=='boolean';register(control);wrapper.append(label,control);if(field.description){const description=document.createElement('span');description.className='dim';description.textContent=field.description;wrapper.append(description)}if(field.kind==='multi_select'){const check=()=>{const count=control.selectedOptions.length;const few=field.min_items!=null&&(count>0||field.required)&&count<field.min_items;const many=field.max_items!=null&&count>field.max_items;control.setCustomValidity(few?`Select at least ${field.min_items} option(s).`:(many?`Select at most ${field.max_items} option(s).`:''))};control.addEventListener('change',check);check()}form.append(wrapper);entries.push({field,control})}const customByOwner=new Map();for(const entry of entries){const owner=entry.field.custom_answer_for;if(!owner||entry.field.kind!=='text'||customByOwner.has(owner))continue;const target=entries.find(candidate=>candidate.field.id===owner);if(!target||!Array.isArray(target.field.options))continue;customByOwner.set(owner,entry)}return()=>{for(const entry of entries)if(entry.field.kind==='text')entry.control.value=entry.control.value.trim();if(!form.reportValidity())return null;const active=new Map();for(const [owner,entry] of customByOwner)if(entry.control.value!=='')active.set(owner,entry);const content={};for(const entry of entries){const {field,control}=entry;if(customByOwner.get(field.custom_answer_for)===entry){if(active.has(field.custom_answer_for))content[field.id]=control.value;continue}const custom=active.get(field.id);if(custom&&custom.field.custom_answer_option==null)continue;const value=elicitationFieldValue(field,control);if(value!==undefined)content[field.id]=value}return content}}
function buildElicitationCard(session,request){const card=document.createElement('section');card.className='card elicitation';const heading=document.createElement('strong');heading.textContent=request.title||'Input needed';const message=document.createElement('pre');message.className='elicitation-message';message.textContent=request.message;const form=document.createElement('form');const status=document.createElement('p');status.className='dim';const gated=[],register=control=>{gated.push(control);return control};const collect=buildElicitationForm(form,request,register);const actions=document.createElement('div');actions.className='row';const send=document.createElement('button');send.type='submit';send.textContent='Send answer';register(send);const decline=document.createElement('button');decline.type='button';decline.className='secondary';decline.textContent='Decline';register(decline);const cancel=document.createElement('button');cancel.type='button';cancel.className='danger';cancel.textContent='Cancel';register(cancel);decline.addEventListener('click',()=>{submitElicitation(session.id,request.id,{action:'decline'})});cancel.addEventListener('click',()=>{submitElicitation(session.id,request.id,{action:'cancel'})});actions.append(send,decline,cancel);form.append(actions);form.addEventListener('submit',event=>{event.preventDefault();const content=collect();if(content)submitElicitation(session.id,request.id,{action:'accept',content})});const nodes=[heading];if(request.description){const description=document.createElement('p');description.className='dim';description.textContent=request.description;nodes.push(description)}nodes.push(message,form,status);card.append(...nodes);return{card,setSent(sent){for(const control of gated)control.disabled=sent;status.textContent=sent?'Answer sent \u2014 waiting for the session to apply it.':''}}}
function renderElicitations(session){const pending=(session&&session.pending_elicitations)||[];if(session)for(const key of [...sentElicitations])if(key.startsWith(`${session.id}\u001f`)&&!pending.some(request=>elicitationKey(session.id,request.id)===key))sentElicitations.delete(key);const live=new Set(),cards=[];for(const request of pending){const key=elicitationKey(session.id,request.id),signature=JSON.stringify(request);live.add(key);let entry=elicitationCards.get(key);if(!entry||entry.signature!==signature){entry=buildElicitationCard(session,request);entry.signature=signature;elicitationCards.set(key,entry)}entry.setSent(sentElicitations.has(key));cards.push(entry.card)}for(const key of [...elicitationCards.keys()])if(!live.has(key))elicitationCards.delete(key);const mounted=[...elicitations.children];if(mounted.length!==cards.length||cards.some((card,index)=>mounted[index]!==card))elicitations.replaceChildren(...cards)}
async function submitElicitation(sessionId,elicitationId,response){const key=elicitationKey(sessionId,elicitationId);if(sentElicitations.has(key))return;sentElicitations.add(key);const rerender=()=>{const session=snapshot?.sessions.find(x=>x.id===sessionId);if(session&&sessionId===currentSession)renderElicitations(session)};rerender();try{await request('/api/actions',{method:'POST',body:JSON.stringify({action:'respond-elicitation',session_id:sessionId,elicitation_id:elicitationId,response})});document.querySelector('#conversation-error').textContent='';await refresh()}catch(err){sentElicitations.delete(key);document.querySelector('#conversation-error').textContent=err.message;rerender()}}
// The composer is a contenteditable rather than a textarea so a pasted or
// dropped image can be intercepted where it lands, and so the box grows with
// its content without a layout read on every keystroke. Rich content is
// refused at beforeinput, which keeps the box plain text however it arrives.
const MAX_PROMPT_REQUEST_BYTES=32*1024*1024;
let composerRevision=0,composerPreserveEmptyBreak=false,promptImages=[];
function composerText(){let text='';const blocks=new Set(['DIV','P']);const append=node=>{if(node.nodeType===Node.TEXT_NODE){text+=node.nodeValue||'';return}if(node.nodeName==='BR'){if(!node.dataset.composerFiller)text+='\n';return}const block=node!==promptText&&blocks.has(node.nodeName);if(block&&text&&!text.endsWith('\n'))text+='\n';node.childNodes.forEach(append);if(block&&node.nextSibling&&!text.endsWith('\n'))text+='\n'};append(promptText);return text.replace(/\r\n?/g,'\n')}
function setComposerText(text){promptText.textContent=text}
function placeComposerCaretAtEnd(){const selection=window.getSelection();if(!selection)return;const range=document.createRange();range.selectNodeContents(promptText);range.collapse(false);selection.removeAllRanges();selection.addRange(range)}
function placeComposerCaretAtPoint(x,y){let range=document.caretRangeFromPoint?.(x,y)||null;if(!range&&document.caretPositionFromPoint){const position=document.caretPositionFromPoint(x,y);if(position){range=document.createRange();range.setStart(position.offsetNode,position.offset);range.collapse(true)}}if(!range||!promptText.contains(range.startContainer))return;const selection=window.getSelection();if(!selection)return;selection.removeAllRanges();selection.addRange(range)}
function insertComposerFallback(node,filler=null){const selection=window.getSelection();const range=selection&&selection.rangeCount?selection.getRangeAt(0):null;if(!range||!promptText.contains(range.commonAncestorContainer)){promptText.append(node);if(filler)promptText.append(filler);placeComposerCaretAtEnd();return}range.deleteContents();range.insertNode(node);if(filler)node.after(filler);range.setStartAfter(node);range.collapse(true);selection.removeAllRanges();selection.addRange(range)}
// execCommand keeps the browser's own undo stack, so it is tried first; the
// fallback covers engines that refuse it, and the revision check covers those
// that run it without emitting the input event that keeps state in step.
function runComposerEdit(command,value,fallback){promptText.focus();const revision=composerRevision;if(document.execCommand(command,false,value)){if(composerRevision===revision)composerInputChanged();return}fallback();composerInputChanged()}
function insertComposerText(text){const normalized=text.replace(/\r\n?/g,'\n');runComposerEdit('insertText',normalized,()=>{insertComposerFallback(document.createTextNode(normalized))})}
function insertComposerLineBreak(){composerPreserveEmptyBreak=true;try{runComposerEdit('insertLineBreak',null,()=>{const filler=document.createElement('br');filler.dataset.composerFiller='true';insertComposerFallback(document.createElement('br'),filler)});let last=promptText;while(last.lastChild)last=last.lastChild;if(last.nodeName==='BR'&&last.previousSibling?.nodeName==='BR'){last.dataset.composerFiller='true'}}finally{composerPreserveEmptyBreak=false}}
// A cleared box can keep a stray break behind it, which leaves the placeholder
// hidden and the box looking occupied when it holds nothing.
function composerInputChanged(){composerRevision+=1;if(!composerPreserveEmptyBreak&&!promptText.textContent&&promptText.childNodes.length)promptText.replaceChildren()}
function readFileAsDataUrl(file){return new Promise((resolve,reject)=>{const reader=new FileReader();reader.addEventListener('load',()=>resolve(String(reader.result||'')),{once:true});reader.addEventListener('error',()=>reject(reader.error||new Error('file read failed')),{once:true});reader.readAsDataURL(file)})}
function imageDimensions(file){return new Promise((resolve,reject)=>{const url=URL.createObjectURL(file);const image=new Image();image.addEventListener('load',()=>{const size={width:image.naturalWidth,height:image.naturalHeight};URL.revokeObjectURL(url);resolve(size)},{once:true});image.addEventListener('error',()=>{URL.revokeObjectURL(url);reject(new Error('the browser could not decode this image'))},{once:true});image.src=url})}
async function promptImageFromFile(file){if(!file.type.startsWith('image/'))throw new Error(`${file.name||'That file'} is not an image`);if(file.size>=MAX_PROMPT_REQUEST_BYTES)throw new Error(`${file.name||'That image'} is too large for the 32 MiB request limit`);const [dataUrl,size]=await Promise.all([readFileAsDataUrl(file),imageDimensions(file)]);const comma=dataUrl.indexOf(',');if(comma<0||!dataUrl.slice(comma+1))throw new Error(`Could not read ${file.name||'that image'}`);return{data_base64:dataUrl.slice(comma+1),mime_type:file.type,width:size.width,height:size.height,name:file.name||'Pasted image'}}
async function attachImageFiles(files){const session=snapshot?.sessions.find(x=>x.id===currentSession);if(!currentSession||!session?.prompt_images_supported||!files.length)return;const sessionId=currentSession;try{const added=[];for(const file of files)added.push(await promptImageFromFile(file));if(currentSession!==sessionId)return;promptImages=promptImages.concat(added);renderAttachments();document.querySelector('#conversation-error').textContent=''}catch(err){document.querySelector('#conversation-error').textContent=err.message}}
function renderAttachments(){const session=snapshot?.sessions.find(x=>x.id===currentSession);attachImage.hidden=!session?.prompt_images_supported;attachments.replaceChildren();for(const [index,image] of promptImages.entries()){const chip=document.createElement('div');chip.className='attachment';const thumb=document.createElement('img');thumb.alt='';thumb.src=`data:${image.mime_type};base64,${image.data_base64}`;const caption=document.createElement('span');caption.textContent=`${image.name} \u00b7 ${image.width}\u00d7${image.height}`;const remove=document.createElement('button');remove.type='button';remove.className='danger';remove.setAttribute('aria-label',`Remove ${image.name}`);remove.textContent='\u00d7';remove.onclick=()=>{promptImages.splice(index,1);renderAttachments()};chip.append(thumb,caption,remove);attachments.append(chip)}}
async function submitPrompt(){if(!currentSession)return;const value=composerText(),images=promptImages;if(!value.trim()&&!images.length)return;const error=document.querySelector('#conversation-error');if(value.startsWith('!')&&images.length){error.textContent='Shell commands cannot carry images.';return}const body=value.startsWith('!')?{action:'run-shell',session_id:currentSession,command:value.slice(1)}:{action:'prompt',session_id:currentSession,text:value,images:images.map(image=>({data_base64:image.data_base64,mime_type:image.mime_type,width:image.width,height:image.height}))};const payload=JSON.stringify(body);if(new TextEncoder().encode(payload).byteLength>MAX_PROMPT_REQUEST_BYTES){error.textContent='Prompt attachments exceed the 32 MiB request limit.';return}try{await request('/api/actions',{method:'POST',body:payload});setComposerText('');promptImages=[];renderAttachments();error.textContent='';await refresh()}catch(err){error.textContent=err.message}}
function renderEntries(entries,replace){if(replace)feed.innerHTML='';for(const entry of entries){let node=document.querySelector(`[data-entry-id="${entry.id}"]`);if(!node){node=document.createElement('article');node.dataset.entryId=entry.id;feed.append(node)}node.className=`entry ${entry.role}`;const title=document.createElement('strong');title.textContent=entry.label;const body=document.createElement('pre');body.textContent=entry.lines.join('\n');node.replaceChildren(title,body)}window.scrollTo(0,document.body.scrollHeight)}
async function loadConversation(delta=false){if(!currentSession)return;try{const result=await request(`/api/conversations/${encodeURIComponent(currentSession)}${delta&&cursor?`?after_seq=${cursor}`:''}`);renderEntries(result.entries,!delta||result.reset);cursor=result.latest_seq;if(cursor>acknowledged){const through=cursor;await request(`/api/conversations/${encodeURIComponent(currentSession)}/read`,{method:'POST',body:JSON.stringify({through})});acknowledged=through}}catch(err){document.querySelector('#conversation-error').textContent=err.message}}
async function openConversation(id){const session=snapshot?.sessions.find(x=>x.id===id);if(!session?.conversation_available){showDashboard();return}currentSession=id;cursor=0;acknowledged=0;location.hash=`conversation/${id}`;dashboard.classList.add('hidden');conversation.classList.remove('hidden');document.querySelector('#conversation-title').textContent=session.title;document.querySelector('#conversation-state').textContent=session.state;renderQueue(session);renderElicitations(session);promptImages=[];renderAttachments();await loadConversation(false)}
function showDashboard(){currentSession=null;cursor=0;acknowledged=0;location.hash='';elicitations.replaceChildren();elicitationCards.clear();promptImages=[];renderAttachments();conversation.classList.add('hidden');dashboard.classList.remove('hidden')}
function confirmFinish(session){finishTitle.textContent=`Finish ${session.title}?`;finishWork.textContent='Hel will finish the current work, then save and verify recovery.';const queued=(session.queued_prompts||[]).length;finishQueue.textContent=queued===0?'No queued prompts are waiting.':queued===1?'1 queued prompt will be saved for resume.':`${queued} queued prompts will be saved for resume.`;finishConsequence.textContent=session.finish.consequence;finishConfirm.textContent=session.finish.primary_action;finishDialog.returnValue='';finishDialog.showModal();return new Promise(resolve=>finishDialog.addEventListener('close',()=>resolve(finishDialog.returnValue==='finish'),{once:true}))}
document.querySelector('#login-form').onsubmit=async e=>{e.preventDefault();try{await request('/auth/session',{method:'POST',body:JSON.stringify({code:document.querySelector('#code').value})});document.querySelector('#login-error').textContent='';await restoreRoute()}catch(err){document.querySelector('#login-error').textContent=err.message}};
logout.onclick=async()=>{await request('/auth/session',{method:'DELETE'});location.reload()};
newTarget.onchange=syncProjectDirectory;
newForm.onsubmit=async e=>{e.preventDefault();const target=snapshot.targets.find(x=>x.id===newTarget.value);try{await request('/api/actions',{method:'POST',body:JSON.stringify({action:'new',title:document.querySelector('#new-title').value,profile_id:newProfile.value,bundle_id:newBundle.value,target_id:newTarget.value,project_directory:target?.requires_project_directory?newProjectDirectory.value:null})});document.querySelector('#new-title').value='';actionError.textContent='';await refresh()}catch(err){actionError.textContent=err.message}};
async function sessionAction(e){const button=e.target.closest('button[data-action]');if(!button)return;if(button.dataset.action==='open')return openConversation(button.dataset.id);const session=snapshot.sessions.find(x=>x.id===button.dataset.id);if(button.dataset.action==='finish'){if(!session?.finish||!await confirmFinish(session))return;pendingFinishes.add(session.id);renderSessions()}const body={action:button.dataset.action,session_id:button.dataset.id};if(button.dataset.action==='resume'){body.profile_id=button.dataset.profile;body.target_id=button.dataset.target;body.queue='start';if(session?.queued_prompts?.length){const choice=prompt(`This session has ${session.queued_prompts.length} queued prompt(s). Type start to run them after resume, or discard to remove them.`,'start');if(choice===null)return;if(!['start','discard'].includes(choice.toLowerCase()))return alert('Enter start or discard.');body.queue=choice.toLowerCase()}}try{await request('/api/actions',{method:'POST',body:JSON.stringify(body)});actionError.textContent='';await refresh()}catch(err){pendingFinishes.delete(button.dataset.id);renderSessions();actionError.textContent=err.message}}
activeSessions.onclick=sessionAction;savedSessions.onclick=sessionAction;
document.querySelector('#back').onclick=showDashboard;
document.querySelector('#prompt-form').onsubmit=e=>{e.preventDefault();submitPrompt()};
promptText.addEventListener('input',composerInputChanged);
// Rich text, and anything a paste or drop would inject as markup, never
// belongs in a prompt: refuse it here and re-insert the plain text instead.
promptText.addEventListener('beforeinput',e=>{const kind=e.inputType||'';if(kind==='insertHTML'||kind.startsWith('insertFromDrop')||kind.startsWith('insertFromPaste')||kind.startsWith('format'))e.preventDefault()});
promptText.addEventListener('paste',e=>{const files=Array.from(e.clipboardData?.items||[]).filter(item=>item.kind==='file'&&item.type.startsWith('image/')).map(item=>item.getAsFile()).filter(Boolean);if(files.length){e.preventDefault();const session=snapshot?.sessions.find(x=>x.id===currentSession);if(session?.prompt_images_supported)attachImageFiles(files);else document.querySelector('#conversation-error').textContent='This session does not support image prompts.';return}const text=e.clipboardData?.getData('text/plain');if(text===undefined)return;e.preventDefault();insertComposerText(text)});
promptText.addEventListener('dragover',e=>{e.preventDefault();const types=Array.from(e.dataTransfer?.types||[]);if(e.dataTransfer)e.dataTransfer.dropEffect=types.some(type=>type==='text/plain'||type==='Files')?'copy':'none'});
promptText.addEventListener('drop',e=>{e.preventDefault();placeComposerCaretAtPoint(e.clientX,e.clientY);const files=Array.from(e.dataTransfer?.files||[]).filter(file=>file.type.startsWith('image/'));if(files.length){const session=snapshot?.sessions.find(x=>x.id===currentSession);if(session?.prompt_images_supported)attachImageFiles(files);else document.querySelector('#conversation-error').textContent='This session does not support image prompts.';return}const text=e.dataTransfer?.getData('text/plain')||'';if(text)insertComposerText(text)});
// An active IME composition steers its candidate with Enter and the arrows,
// so the composer must not read those keys until the composition ends.
promptText.addEventListener('keydown',e=>{if(e.isComposing||e.keyCode===229)return;if(e.key==='Enter'&&!e.shiftKey&&!e.metaKey&&!e.ctrlKey&&!e.altKey){e.preventDefault();submitPrompt();return}if(e.key==='Enter'&&e.shiftKey&&!e.metaKey&&!e.ctrlKey&&!e.altKey){e.preventDefault();insertComposerLineBreak();return}if((e.metaKey||e.ctrlKey)&&e.key==='Enter'){e.preventDefault();submitPrompt()}});
attachImage.onclick=()=>imagePicker.click();
imagePicker.onchange=()=>{const files=Array.from(imagePicker.files||[]);imagePicker.value='';attachImageFiles(files)};
queue.onclick=async e=>{const button=e.target.closest('button[data-queue-id]');if(!button)return;try{await request('/api/actions',{method:'POST',body:JSON.stringify({action:'remove-queued-prompt',session_id:currentSession,queue_id:button.dataset.queueId})});await refresh()}catch(err){document.querySelector('#conversation-error').textContent=err.message}};
shells.onclick=async e=>{const button=e.target.closest('button[data-shell-id]');if(!button)return;try{await request('/api/actions',{method:'POST',body:JSON.stringify({action:'cancel-shell',session_id:currentSession,shell_command_id:button.dataset.shellId})});await refresh()}catch(err){document.querySelector('#conversation-error').textContent=err.message}};
function escapeHtml(value){const e=document.createElement('span');e.textContent=value;return e.innerHTML}function escapeAttr(value){return escapeHtml(value).replaceAll('"','&quot;')}
window.addEventListener('online',()=>{startEvents();refresh()});
if('serviceWorker'in navigator)navigator.serviceWorker.register('/service-worker.js');restoreRoute();
</script></body></html>"##;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use axum::http::Request;
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    use crate::hel_config::{
        CONFIG_VERSION, ContainerTemplate, HarnessKind, HarnessProfile, ProjectBundle,
        ProjectRepository,
    };
    use crate::hel_state::{STATE_VERSION, SessionRecord, TargetLocator};

    fn sample_config_state() -> (HelConfig, HelState) {
        let config = HelConfig {
            version: CONFIG_VERSION,
            newer_config_version: None,
            phone: Default::default(),
            profiles: BTreeMap::from([(
                "codex-1".into(),
                HarnessProfile {
                    context_window_bytes: None,
                    kind: HarnessKind::Codex,
                    home: "/highly/secret/codex".into(),
                    executable: None,
                    environment: BTreeMap::from([("GH_TOKEN".into(), "secret-token".into())]),
                },
            )]),
            bundles: BTreeMap::from([(
                "hel".into(),
                ProjectBundle {
                    primary_repo: "hel".into(),
                    repositories: vec![ProjectRepository {
                        id: "hel".into(),
                        github: Some("owner/hel".into()),
                        local: Some("/private/source/hel".into()),
                        destination: "hel".into(),
                        git_ref: None,
                    }],
                },
            )]),
            targets: BTreeMap::from([
                (
                    "podman".into(),
                    TargetTemplate::LocalPodman {
                        container: ContainerTemplate {
                            image: "secret.registry/image".into(),
                            pull_policy: Default::default(),
                            platform: None,
                            cpus: None,
                            memory: None,
                            environment: BTreeMap::from([("TOKEN".into(), "secret-target".into())]),
                        },
                    },
                ),
                ("raw".into(), TargetTemplate::LocalBare),
            ]),
        };
        let state = HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([(
                "session-1".into(),
                SessionRecord {
                    workspace_id: crate::hel_workspace::DEFAULT_WORKSPACE_ID.to_owned(),
                    archived: false,
                    container_cpus: None,
                    container_memory: None,
                    id: "session-1".into(),
                    title: "Build Hel".into(),
                    harness_kind: HarnessKind::Codex,
                    last_profile: "codex-1".into(),
                    bundle_id: "hel".into(),
                    project_directory: None,
                    managed_worktree: None,
                    target_template_id: "podman".into(),
                    resource_allocation: None,
                    additional_mounts: vec![],
                    state: SessionState::Running,
                    target: Some(TargetLocator::LocalPodman {
                        container_id: "private-container-id".into(),
                    }),
                    native_session_id: Some("native-secret-id".into()),
                    acp_session_title: Some("Build Hel".into()),
                    session_title_override: None,
                    created_at: "now".into(),
                    updated_at: "now".into(),
                    viewed_through_event_ordinal: 0,
                    draft_input: String::new(),
                    last_error: Some("secret-token at /highly/secret/codex".into()),
                    last_checkpoint_error: None,
                    checkpoint: None,
                },
            )]),
            mount_history: BTreeMap::new(),
            container_sizes: BTreeMap::new(),
        };
        (config, state)
    }

    type TestServer = (
        Router,
        mpsc::Receiver<ControllerRequest>,
        mpsc::Receiver<ReadReceiptRequest>,
    );

    fn app() -> TestServer {
        app_with_conversations(BTreeMap::new())
    }

    fn app_with_conversations(conversations: BTreeMap<String, BrowserTranscript>) -> TestServer {
        app_with(conversations, |_| {})
    }

    fn app_with_snapshot(adjust: impl FnOnce(&mut ViewerSnapshot)) -> TestServer {
        app_with(BTreeMap::new(), adjust)
    }

    fn app_with(
        conversations: BTreeMap<String, BrowserTranscript>,
        adjust: impl FnOnce(&mut ViewerSnapshot),
    ) -> TestServer {
        let (config, state) = sample_config_state();
        let mut snapshot = ViewerSnapshot::from_config_state(&config, &state, 1);
        adjust(&mut snapshot);
        let (_snapshot_tx, snapshot_rx) = watch::channel(snapshot);
        let (_conversation_tx, conversation_rx) = watch::channel(conversations);
        let (action_tx, action_rx) = mpsc::channel(8);
        let (receipt_tx, receipt_rx) = mpsc::channel(8);
        let options = test_options(snapshot_rx, conversation_rx, action_tx, receipt_tx)
            .with_test_credentials("123456", b"01234567890123456789012345678901");
        (router(options), action_rx, receipt_rx)
    }

    fn test_options(
        snapshot_rx: watch::Receiver<ViewerSnapshot>,
        conversation_rx: watch::Receiver<BTreeMap<String, BrowserTranscript>>,
        action_tx: mpsc::Sender<ControllerRequest>,
        receipt_tx: mpsc::Sender<ReadReceiptRequest>,
    ) -> ServerOptions {
        ServerOptions::new(
            "127.0.0.1:0".parse().unwrap(),
            snapshot_rx,
            conversation_rx,
            action_tx,
            receipt_tx,
        )
        .unwrap()
    }

    fn detached_options() -> ServerOptions {
        let (config, state) = sample_config_state();
        let (_snapshot_tx, snapshot_rx) =
            watch::channel(ViewerSnapshot::from_config_state(&config, &state, 1));
        let (_conversation_tx, conversation_rx) = watch::channel(BTreeMap::new());
        let (action_tx, _action_rx) = mpsc::channel(1);
        let (receipt_tx, _receipt_rx) = mpsc::channel(1);
        test_options(snapshot_rx, conversation_rx, action_tx, receipt_tx)
    }

    async fn login_cookie(app: &Router) -> String {
        let response = app
            .clone()
            .oneshot(
                Request::post("/auth/session")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"code":"123456"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        response
            .headers()
            .get(SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn api_requires_a_valid_signed_cookie() {
        let (app, _, _) = app();
        let unauthorized = app
            .clone()
            .oneshot(Request::get("/api/snapshot").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let cookie = login_cookie(&app).await;
        let authorized = app
            .oneshot(
                Request::get("/api/snapshot")
                    .header(COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn qr_login_exchanges_the_secret_for_a_cookie_and_redirects_cleanly() {
        let (app, _, _) = app();
        let rejected = app
            .clone()
            .oneshot(
                Request::get("/auth/login?token=wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);

        let accepted = app
            .oneshot(
                Request::get("/auth/login?token=test-login-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::SEE_OTHER);
        assert_eq!(accepted.headers().get(LOCATION).unwrap(), "/");
        assert_eq!(accepted.headers().get(CACHE_CONTROL).unwrap(), "no-store");
        assert!(accepted.headers().contains_key(SET_COOKIE));
    }

    #[test]
    fn signed_cookie_rejects_expiry_and_tampering() {
        let key = b"01234567890123456789012345678901";
        let cookie = signed_cookie_value(key, 200);
        assert!(session_cookie_valid(key, &cookie, 100));
        assert!(!session_cookie_valid(key, &cookie, 200));
        assert!(!session_cookie_valid(key, &format!("{cookie}x"), 100));
        assert!(!session_cookie_valid(b"another-key", &cookie, 100));
    }

    #[test]
    fn generated_code_and_cookie_attributes_are_phone_safe() {
        let code = generate_viewer_code().unwrap();
        assert_eq!(code.len(), 6);
        assert!(code.bytes().all(|byte| byte.is_ascii_digit()));
        let header = session_cookie_header("signed", Some(60), true)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(header.contains("HttpOnly"));
        assert!(header.contains("SameSite=Strict"));
        assert!(header.contains("Secure"));
        assert!(header.contains("Max-Age=60"));
    }

    #[test]
    fn public_snapshot_omits_homes_environment_locators_and_raw_errors() {
        let (config, state) = sample_config_state();
        let json =
            serde_json::to_string(&ViewerSnapshot::from_config_state(&config, &state, 9)).unwrap();
        assert!(!json.contains("/highly/secret"));
        assert!(!json.contains("secret-token"));
        assert!(!json.contains("secret-target"));
        assert!(!json.contains("secret.registry"));
        assert!(!json.contains("native-secret-id"));
        assert!(!json.contains("private-container-id"));
        assert!(json.contains("\"has_error\":true"));
        assert!(json.contains("\"kind\":\"local-podman-container\""));
        assert!(json.contains("The Hel session container will be removed"));
    }

    #[test]
    fn viewer_finish_projection_is_target_aware_without_locators() {
        let (config, mut state) = sample_config_state();
        let cases = [
            (
                TargetLocator::LocalBare {
                    worker_root: "/private/local-worker".into(),
                },
                "local-bare-worker",
                "selected project directory will remain unchanged",
                "Stop worker and save",
            ),
            (
                TargetLocator::LocalPodman {
                    container_id: "private-local-container".into(),
                },
                "local-podman-container",
                "This computer and other containers will remain unchanged",
                "Remove container and save",
            ),
            (
                TargetLocator::AppleContainer {
                    container_id: "private-apple-container".into(),
                },
                "apple-container",
                "This computer and other containers will remain unchanged",
                "Remove container and save",
            ),
            (
                TargetLocator::SshBare {
                    host: "private-bare-host".into(),
                    workspace: "/private/remote-worker".into(),
                    worker_id: Some("private-worker-id".into()),
                },
                "remote-bare-worker",
                "remote host and selected project directory will remain unchanged",
                "Stop worker and save",
            ),
            (
                TargetLocator::SshPodman {
                    host: "private-podman-host".into(),
                    container_id: "private-remote-container".into(),
                },
                "remote-podman-container",
                "host and other containers will remain unchanged",
                "Remove container and save",
            ),
            (
                TargetLocator::AwsEc2 {
                    instance_id: "private-instance-id".into(),
                    address: Some("private-instance-address".into()),
                },
                "aws-ec2-instance",
                "EC2 session instance will be terminated",
                "Terminate instance and save",
            ),
        ];

        for (target, expected_kind, consequence, primary_action) in cases {
            state.sessions.get_mut("session-1").unwrap().target = Some(target);
            let snapshot = ViewerSnapshot::from_config_state(&config, &state, 9);
            let finish = snapshot.sessions[0].finish.as_ref().unwrap();
            assert_eq!(finish.kind, expected_kind);
            assert!(finish.consequence.contains(consequence));
            assert_eq!(finish.primary_action, primary_action);
            let json = serde_json::to_string(&snapshot).unwrap();
            assert!(!json.contains("private-"), "locator leaked in {json}");
        }
    }

    #[test]
    fn saved_viewer_session_has_no_finish_projection() {
        let (config, mut state) = sample_config_state();
        let session = state.sessions.get_mut("session-1").unwrap();
        session.state = SessionState::Stopped;
        session.target = None;

        let snapshot = ViewerSnapshot::from_config_state(&config, &state, 9);
        assert_eq!(snapshot.sessions[0].state, "saved");
        assert!(snapshot.sessions[0].finish.is_none());
        assert!(
            !serde_json::to_string(&snapshot.sessions[0])
                .unwrap()
                .contains("\"finish\"")
        );
    }

    fn sample_elicitation() -> ElicitationRequest {
        ElicitationRequest::from_acp_params(
            "elicitation-1",
            serde_json::json!({
                "sessionId": "session-1",
                "mode": "form",
                "message": "Which CI architecture should the workflow use?",
                "requestedSchema": {
                    "type": "object",
                    "required": ["question_0"],
                    "properties": {
                        "question_0": {
                            "type": "string",
                            "title": "CI architecture",
                            "oneOf": [
                                {"const": "reusable", "title": "Reusable workflow"},
                                {"const": "matrix", "title": "Matrix job"}
                            ]
                        },
                        "question_0_custom": {
                            "type": "string",
                            "title": "Other",
                            "_meta": {"_askUserQuestionCustomAnswer": {
                                "questionId": "question_0",
                                "isCustomAnswer": true
                            }}
                        }
                    }
                }
            }),
        )
        .expect("sample elicitation parses")
    }

    fn accept(pairs: &[(&str, &str)]) -> ElicitationResponse {
        ElicitationResponse::Accept {
            content: pairs
                .iter()
                .map(|(id, value)| {
                    (
                        (*id).to_owned(),
                        crate::hel_elicitation::ElicitationValue::String((*value).to_owned()),
                    )
                })
                .collect(),
        }
    }

    fn pending_elicitation_snapshot(snapshot: &mut ViewerSnapshot) {
        snapshot.sessions[0].pending_elicitations = vec![sample_elicitation()];
    }

    #[tokio::test]
    async fn elicitation_answer_is_typed_and_forwarded() {
        let (app, mut actions, _) = app_with_snapshot(pending_elicitation_snapshot);
        let cookie = login_cookie(&app).await;
        let response = tokio::spawn(
            app.oneshot(
                Request::post("/api/actions")
                    .header(COOKIE, cookie)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"action":"respond-elicitation","session_id":"session-1","elicitation_id":"elicitation-1","response":{"action":"accept","content":{"question_0":"reusable"}}}"#,
                    ))
                    .unwrap(),
            ),
        );
        let action = actions.recv().await.unwrap();
        assert_eq!(
            action.action,
            ControllerAction::RespondElicitation {
                session_id: "session-1".into(),
                elicitation_id: "elicitation-1".into(),
                response: accept(&[("question_0", "reusable")]),
            }
        );
        action.reply.send(ActionOutcome::Accepted).unwrap();
        assert_eq!(
            response.await.unwrap().unwrap().status(),
            StatusCode::ACCEPTED
        );
    }

    #[tokio::test]
    async fn elicitation_answer_for_an_unknown_request_is_refused_without_reaching_the_controller()
    {
        let (app, mut actions, _) = app_with_snapshot(pending_elicitation_snapshot);
        let cookie = login_cookie(&app).await;
        let response = app
            .oneshot(
                Request::post("/api/actions")
                    .header(COOKIE, cookie)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"action":"respond-elicitation","session_id":"session-1","elicitation_id":"elicitation-9","response":{"action":"cancel"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(actions.try_recv().is_err());
    }

    #[test]
    fn elicitation_answers_are_checked_against_the_request_the_agent_asked() {
        let (config, state) = sample_config_state();
        let mut snapshot = ViewerSnapshot::from_config_state(&config, &state, 1);
        pending_elicitation_snapshot(&mut snapshot);
        let respond = |response: ElicitationResponse| ControllerAction::RespondElicitation {
            session_id: "session-1".into(),
            elicitation_id: "elicitation-1".into(),
            response,
        };

        assert!(validate_action(&respond(accept(&[("question_0", "matrix")])), &snapshot).is_ok());
        // Declining and cancelling never carry content, so they are always
        // answerable.
        assert!(validate_action(&respond(ElicitationResponse::Decline), &snapshot).is_ok());
        // An option the agent never offered, a field it never published, and a
        // missing required answer are all refused.
        assert!(validate_action(&respond(accept(&[("question_0", "cron")])), &snapshot).is_err());
        assert!(validate_action(&respond(accept(&[("smuggled", "yes")])), &snapshot).is_err());
        assert!(validate_action(&respond(accept(&[])), &snapshot).is_err());
        // A custom answer stands in for the select it belongs to, exactly as
        // the chat form submits it.
        assert!(
            validate_action(
                &respond(accept(&[("question_0_custom", "a monorepo pipeline")])),
                &snapshot,
            )
            .is_ok()
        );
    }

    #[test]
    fn oversized_elicitation_answers_are_refused() {
        let (config, state) = sample_config_state();
        let mut snapshot = ViewerSnapshot::from_config_state(&config, &state, 1);
        pending_elicitation_snapshot(&mut snapshot);
        let long = "x".repeat(MAX_ELICITATION_BYTES);
        assert!(
            validate_action(
                &ControllerAction::RespondElicitation {
                    session_id: "session-1".into(),
                    elicitation_id: "elicitation-1".into(),
                    response: accept(&[("question_0_custom", long.as_str())]),
                },
                &snapshot,
            )
            .is_err()
        );
    }

    /// The card cache is the fix for answers vanishing under snapshot polls, so
    /// it is exercised as JavaScript: the render source is lifted out of the
    /// embedded viewer and run against a stub DOM.
    #[test]
    fn embedded_viewer_keeps_elicitation_answers_across_snapshot_polls() {
        let start = VIEWER_HTML
            .find("const elicitationCards=new Map()")
            .expect("elicitation card cache");
        let end = VIEWER_HTML[start..]
            .find("async function submitElicitation")
            .map(|offset| start + offset)
            .expect("elicitation rendering boundary");
        let source = &VIEWER_HTML[start..end];
        let dom = r#"
let replaceCalls = 0;
class Option {
  constructor(label, value) {
    this.label = label;
    this.value = value;
    this.selected = false;
  }
}
function makeEl(tag) {
  return {
    tagName: tag.toUpperCase(),
    children: [],
    options: [],
    selectedOptions: [],
    className: "",
    textContent: "",
    disabled: false,
    required: false,
    value: "",
    appendChild(child) {
      this.children.push(child);
      if (this.tagName === "SELECT") this.options.push(child);
      return child;
    },
    append(...kids) {
      this.children.push(...kids);
    },
    replaceChildren(...kids) {
      replaceCalls += 1;
      this.children = kids;
    },
    addEventListener() {},
    setCustomValidity() {},
    reportValidity() {
      return true;
    },
  };
}
const created = [];
const document = {
  createElement(tag) {
    const el = makeEl(tag);
    created.push(el);
    return el;
  },
};
const elicitations = makeEl("div");
async function submitElicitation() {}
"#;
        let checks = r#"
const request = {
  id: "elicitation-1",
  message: "Which CI architecture?",
  title: "CI",
  fields: [
    {
      id: "question_0",
      title: "CI architecture",
      required: false,
      kind: "single_select",
      options: [{ value: "reusable", title: "Reusable" }, { value: "matrix", title: "Matrix" }],
    },
    { id: "question_0_custom", title: "Other", required: false, kind: "text" },
  ],
};
const session = { id: "session-1", pending_elicitations: [request] };
renderElicitations(session);
const card = elicitations.children[0];
const select = created.find((el) => el.tagName === "SELECT");
const text = created.find((el) => el.tagName === "INPUT");
select.value = "reusable";
text.value = "keep me";
const attachments = replaceCalls;
renderElicitations(session);
if (elicitations.children[0] !== card) {
  throw new Error("a snapshot rebuilt the pending card");
}
if (select.value !== "reusable" || text.value !== "keep me") {
  throw new Error("a snapshot wiped the half-filled answer");
}
if (replaceCalls !== attachments) {
  throw new Error("a snapshot re-attached an unchanged card and dropped focus");
}
sentElicitations.add(elicitationKey("session-1", request.id));
renderElicitations(session);
if (elicitations.children[0] !== card) {
  throw new Error("a sent answer rebuilt the card");
}
if (!select.disabled || !text.disabled) {
  throw new Error("a sent answer left the controls live");
}
if (select.value !== "reusable") {
  throw new Error("a sent answer wiped the reply");
}
renderElicitations({ id: "session-1", pending_elicitations: [] });
if (elicitations.children.length !== 0 || elicitationCards.size !== 0) {
  throw new Error("an answered request stayed rendered");
}
if (sentElicitations.size !== 0) {
  throw new Error("a resolved request kept its sent marker");
}
"#;
        let script = format!("{dom}\n{source}\n{checks}");
        let output = std::process::Command::new("node")
            .args(["--input-type=module", "--eval"])
            .arg(script)
            .output()
            .expect("Node.js is required to exercise the embedded web viewer");
        assert!(
            output.status.success(),
            "embedded viewer elicitation rendering failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    fn sample_image(pixels: usize) -> ViewerPromptImage {
        ViewerPromptImage {
            data_base64: base64::engine::general_purpose::STANDARD.encode(vec![7_u8; pixels]),
            mime_type: "image/png".into(),
            width: 32,
            height: 24,
        }
    }

    fn image_capable(snapshot: &mut ViewerSnapshot) {
        snapshot.sessions[0].prompt_images_supported = true;
    }

    async fn post_action(app: Router, cookie: String, body: String) -> Response<Body> {
        app.oneshot(
            Request::post("/api/actions")
                .header(COOKIE, cookie)
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn image_prompt_reaches_the_controller_with_its_images() {
        let (app, mut actions, _) = app_with_snapshot(image_capable);
        let cookie = login_cookie(&app).await;
        let image = sample_image(8);
        let body = serde_json::to_string(&ControllerAction::Prompt {
            session_id: "session-1".into(),
            text: String::new(),
            images: vec![image.clone(), image.clone()],
        })
        .unwrap();
        let response = tokio::spawn(post_action(app, cookie, body));
        let action = actions.recv().await.unwrap();
        assert_eq!(
            action.action,
            ControllerAction::Prompt {
                session_id: "session-1".into(),
                text: String::new(),
                images: vec![image.clone(), image],
            }
        );
        action.reply.send(ActionOutcome::Accepted).unwrap();
        assert_eq!(response.await.unwrap().status(), StatusCode::ACCEPTED);
    }

    /// Base64 inflates an upload by a third, so two ordinary photographs pass
    /// the general body limit even when each one fits it. The action route
    /// carries prompts, so it is the route that gets the larger bound.
    #[tokio::test]
    async fn multi_image_prompts_are_accepted_over_the_general_body_limit() {
        let (app, mut actions, _) = app_with_snapshot(image_capable);
        let cookie = login_cookie(&app).await;
        let image = sample_image(MAX_BODY_BYTES / 2);
        let body = serde_json::to_string(&ControllerAction::Prompt {
            session_id: "session-1".into(),
            text: "look at these".into(),
            images: vec![image.clone(), image],
        })
        .unwrap();
        assert!(body.len() > MAX_BODY_BYTES);
        assert!(body.len() < MAX_PROMPT_BODY_BYTES);
        let response = tokio::spawn(post_action(app, cookie, body));
        let action = actions.recv().await.unwrap();
        action.reply.send(ActionOutcome::Accepted).unwrap();
        assert_eq!(response.await.unwrap().status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn a_body_over_the_prompt_limit_is_still_refused() {
        let (app, _actions, _) = app_with_snapshot(image_capable);
        let cookie = login_cookie(&app).await;
        let image = sample_image(MAX_PROMPT_BODY_BYTES);
        let body = serde_json::to_string(&ControllerAction::Prompt {
            session_id: "session-1".into(),
            text: String::new(),
            images: vec![image],
        })
        .unwrap();
        assert!(body.len() > MAX_PROMPT_BODY_BYTES);
        let response = post_action(app, cookie, body).await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn malformed_image_payloads_never_reach_the_controller() {
        let cases = [
            ("aW1hZ2U=", "text/plain", 32, 24),
            ("aW1hZ2U=", "image/png", 0, 24),
            ("not base64!", "image/png", 32, 24),
            ("", "image/png", 32, 24),
        ];
        for (data, mime, width, height) in cases {
            let (app, mut actions, _) = app_with_snapshot(image_capable);
            let cookie = login_cookie(&app).await;
            let body = serde_json::to_string(&ControllerAction::Prompt {
                session_id: "session-1".into(),
                text: String::new(),
                images: vec![ViewerPromptImage {
                    data_base64: data.into(),
                    mime_type: mime.into(),
                    width,
                    height,
                }],
            })
            .unwrap();
            let response = post_action(app, cookie, body).await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "expected {data:?}/{mime} {width}x{height} to be refused"
            );
            assert!(actions.try_recv().is_err());
        }
    }

    #[test]
    fn image_prompts_need_text_or_an_image_and_an_agent_that_takes_them() {
        let (config, state) = sample_config_state();
        let mut snapshot = ViewerSnapshot::from_config_state(&config, &state, 1);
        let prompt = |text: &str, images: Vec<ViewerPromptImage>| ControllerAction::Prompt {
            session_id: "session-1".into(),
            text: text.into(),
            images,
        };

        // Without the capability the session takes text only.
        assert!(validate_action(&prompt("ship it", Vec::new()), &snapshot).is_ok());
        assert!(validate_action(&prompt("", vec![sample_image(8)]), &snapshot).is_err());

        image_capable(&mut snapshot);
        // An image is a prompt on its own; nothing at all is not.
        assert!(validate_action(&prompt("", vec![sample_image(8)]), &snapshot).is_ok());
        assert!(validate_action(&prompt("   ", Vec::new()), &snapshot).is_err());
        assert!(validate_action(&prompt("", Vec::new()), &snapshot).is_err());
        // A shell command is still a shell command.
        assert!(validate_action(&prompt("!ls", vec![sample_image(8)]), &snapshot).is_err());
    }

    /// The composer holds a DOM, not a string, so the text a prompt sends is
    /// whatever this reader makes of that DOM. Run it as JavaScript.
    #[test]
    fn embedded_viewer_reads_multiline_composer_text_out_of_its_dom() {
        let start = VIEWER_HTML
            .find("function composerText()")
            .expect("composer reader");
        let end = VIEWER_HTML[start..]
            .find("function setComposerText(")
            .map(|offset| start + offset)
            .expect("composer reader boundary");
        let source = &VIEWER_HTML[start..end];
        let harness = r##"
const Node = { TEXT_NODE: 3 };
function textNode(value) {
  return { nodeType: 3, nodeValue: value, nodeName: "#text", childNodes: [], dataset: {} };
}
function element(name, children = [], dataset = {}) {
  const node = { nodeType: 1, nodeName: name, dataset, childNodes: children };
  children.forEach((child, index) => {
    child.nextSibling = children[index + 1] || null;
  });
  return node;
}
let promptText = null;
function read(children) {
  promptText = element("DIV", children);
  return composerText();
}
"##;
        let checks = r#"
const plain = read([textNode("ship it")]);
if (plain !== "ship it") throw new Error(`plain text became ${JSON.stringify(plain)}`);

const broken = read([textNode("first"), element("BR"), textNode("second")]);
if (broken !== "first\nsecond") throw new Error(`line break became ${JSON.stringify(broken)}`);

// The trailing break a browser leaves behind to keep the caret on a new line
// is scaffolding, not a line the user typed.
const filler = read([
  textNode("first"),
  element("BR"),
  element("BR", [], { composerFiller: "true" }),
]);
if (filler !== "first\n") throw new Error(`filler break became ${JSON.stringify(filler)}`);

const blocks = read([
  textNode("first"),
  element("DIV", [textNode("second")]),
  element("DIV", [textNode("third")]),
]);
if (blocks !== "first\nsecond\nthird") throw new Error(`blocks became ${JSON.stringify(blocks)}`);

const carriage = read([textNode("first\r\nsecond")]);
if (carriage !== "first\nsecond") throw new Error(`CRLF became ${JSON.stringify(carriage)}`);
"#;
        let script = format!("{harness}\n{source}\n{checks}");
        let output = std::process::Command::new("node")
            .args(["--input-type=module", "--eval"])
            .arg(script)
            .output()
            .expect("Node.js is required to exercise the embedded web viewer");
        assert!(
            output.status.success(),
            "embedded viewer composer reader failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[test]
    fn viewer_declares_the_icon_route_instead_of_requesting_a_missing_favicon() {
        assert!(VIEWER_HTML.contains(r#"<link rel="icon" href="/icon.svg">"#));
    }

    #[test]
    fn viewer_page_separates_active_finish_from_saved_resume() {
        assert!(VIEWER_HTML.contains("Active sessions"));
        assert!(VIEWER_HTML.contains("Saved sessions run no workers"));
        assert!(VIEWER_HTML.contains(r#"data-action="finish""#));
        assert!(VIEWER_HTML.contains("session.finish.consequence"));
        assert!(VIEWER_HTML.contains("session.finish.primary_action"));
        assert!(VIEWER_HTML.contains("Saved · no worker running"));
        assert!(!VIEWER_HTML.contains(r#"data-action="close""#));
        assert!(!VIEWER_HTML.contains(">Stop</button>"));
    }

    #[tokio::test]
    async fn valid_action_is_typed_and_forwarded() {
        let (app, mut actions, _) = app();
        let cookie = login_cookie(&app).await;
        let response = tokio::spawn(
            app.oneshot(
                Request::post("/api/actions")
                    .header(COOKIE, cookie)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"action":"prompt","session_id":"session-1","text":"ship it"}"#,
                    ))
                    .unwrap(),
            ),
        );
        let action = actions.recv().await.unwrap();
        assert_eq!(
            action.action,
            ControllerAction::Prompt {
                session_id: "session-1".into(),
                text: "ship it".into(),
                images: Vec::new(),
            }
        );
        action.reply.send(ActionOutcome::Accepted).unwrap();
        let response = response.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn finish_action_is_typed_and_forwarded() {
        let (app, mut actions, _) = app();
        let cookie = login_cookie(&app).await;
        let response = tokio::spawn(
            app.oneshot(
                Request::post("/api/actions")
                    .header(COOKIE, cookie)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"action":"finish","session_id":"session-1"}"#,
                    ))
                    .unwrap(),
            ),
        );
        let action = actions.recv().await.unwrap();
        assert_eq!(
            action.action,
            ControllerAction::Finish {
                session_id: "session-1".into(),
            }
        );
        action.reply.send(ActionOutcome::Accepted).unwrap();
        assert_eq!(
            response.await.unwrap().unwrap().status(),
            StatusCode::ACCEPTED
        );
    }

    #[tokio::test]
    async fn shell_action_is_typed_and_forwarded() {
        let (app, mut actions, _) = app();
        let cookie = login_cookie(&app).await;
        let response = tokio::spawn(
            app.oneshot(
                Request::post("/api/actions")
                    .header(COOKIE, cookie)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"action":"run-shell","session_id":"session-1","command":"cargo test"}"#,
                    ))
                    .unwrap(),
            ),
        );
        let action = actions.recv().await.unwrap();
        assert_eq!(
            action.action,
            ControllerAction::RunShell {
                session_id: "session-1".into(),
                command: "cargo test".into(),
            }
        );
        action.reply.send(ActionOutcome::Accepted).unwrap();
        assert_eq!(
            response.await.unwrap().unwrap().status(),
            StatusCode::ACCEPTED
        );
    }

    #[test]
    fn shell_action_validation_reserves_bang_prompts_and_checks_cancellation_ids() {
        let (config, state) = sample_config_state();
        let mut snapshot = ViewerSnapshot::from_config_state(&config, &state, 1);
        assert!(
            validate_action(
                &ControllerAction::Prompt {
                    session_id: "session-1".into(),
                    text: "!cargo test".into(),
                    images: Vec::new(),
                },
                &snapshot,
            )
            .is_err()
        );
        assert!(
            validate_action(
                &ControllerAction::RunShell {
                    session_id: "session-1".into(),
                    command: "cargo test".into(),
                },
                &snapshot,
            )
            .is_ok()
        );
        assert!(
            validate_action(
                &ControllerAction::CancelShell {
                    session_id: "session-1".into(),
                    shell_command_id: "shell-1".into(),
                },
                &snapshot,
            )
            .is_err()
        );

        snapshot.sessions[0]
            .active_user_shells
            .push(ViewerUserShell {
                id: "shell-1".into(),
                command: "cargo test".into(),
                started_at_ms: Some(10),
            });
        assert!(
            validate_action(
                &ControllerAction::CancelShell {
                    session_id: "session-1".into(),
                    shell_command_id: "shell-1".into(),
                },
                &snapshot,
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn bare_new_action_forwards_an_explicit_safe_project_directory() {
        let (app, mut actions, _) = app();
        let cookie = login_cookie(&app).await;
        let response = tokio::spawn(
            app.oneshot(
                Request::post("/api/actions")
                    .header(COOKIE, cookie)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"action":"new","profile_id":"codex-1","bundle_id":"hel","target_id":"raw","title":"Raw work","project_directory":"/work/project"}"#,
                    ))
                    .unwrap(),
            ),
        );
        let action = actions.recv().await.unwrap();
        assert_eq!(
            action.action,
            ControllerAction::New {
                workspace_id: String::new(),
                profile_id: "codex-1".into(),
                bundle_id: "hel".into(),
                target_id: "raw".into(),
                title: "Raw work".into(),
                project_directory: Some(PathBuf::from("/work/project")),
            }
        );
        action.reply.send(ActionOutcome::Accepted).unwrap();
        assert_eq!(
            response.await.unwrap().unwrap().status(),
            StatusCode::ACCEPTED
        );
    }

    #[test]
    fn new_action_requires_project_directory_exactly_for_bare_targets() {
        let (config, state) = sample_config_state();
        let snapshot = ViewerSnapshot::from_config_state(&config, &state, 1);
        let action = |target_id: &str, project_directory: Option<PathBuf>| ControllerAction::New {
            workspace_id: String::new(),
            profile_id: "codex-1".into(),
            bundle_id: "hel".into(),
            target_id: target_id.into(),
            title: "New work".into(),
            project_directory,
        };

        assert!(validate_action(&action("podman", None), &snapshot).is_ok());
        assert_eq!(
            validate_action(&action("podman", Some("/work".into())), &snapshot)
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            validate_action(&action("raw", None), &snapshot)
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            validate_action(&action("raw", Some("relative".into())), &snapshot)
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            validate_action(&action("raw", Some("/work/../secret".into())), &snapshot)
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );
        assert!(validate_action(&action("raw", Some("/work/project".into())), &snapshot).is_ok());
    }

    #[tokio::test]
    async fn cancel_action_is_typed_and_forwarded() {
        let (app, mut actions, _) = app();
        let cookie = login_cookie(&app).await;
        let response = tokio::spawn(
            app.oneshot(
                Request::post("/api/actions")
                    .header(COOKIE, cookie)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"action":"cancel","session_id":"session-1"}"#,
                    ))
                    .unwrap(),
            ),
        );
        let action = actions.recv().await.unwrap();
        assert_eq!(
            action.action,
            ControllerAction::Cancel {
                session_id: "session-1".into(),
            }
        );
        action.reply.send(ActionOutcome::Accepted).unwrap();
        assert_eq!(
            response.await.unwrap().unwrap().status(),
            StatusCode::ACCEPTED
        );
    }

    #[tokio::test]
    async fn action_validation_accepts_cross_harness_resume_and_rejects_unknown() {
        let (mut config, mut state) = sample_config_state();
        config.profiles.insert(
            "claude-1".into(),
            HarnessProfile {
                context_window_bytes: None,
                kind: HarnessKind::Claude,
                home: "/secret/claude".into(),
                executable: None,
                environment: BTreeMap::new(),
            },
        );
        let session = state.sessions.get_mut("session-1").unwrap();
        session.state = SessionState::Stopped;
        session.target = None;
        let snapshot = ViewerSnapshot::from_config_state(&config, &state, 1);
        validate_action(
            &ControllerAction::Resume {
                session_id: "session-1".into(),
                profile_id: "claude-1".into(),
                target_id: "podman".into(),
                queue: ResumeQueueDisposition::Start,
            },
            &snapshot,
        )
        .unwrap();

        let error = validate_action(
            &ControllerAction::Finish {
                session_id: "not-managed".into(),
            },
            &snapshot,
        )
        .unwrap_err();
        assert_eq!(error.status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn lifecycle_actions_are_available_only_in_the_matching_viewer_state() {
        let (config, mut state) = sample_config_state();
        let active = ViewerSnapshot::from_config_state(&config, &state, 1);
        assert!(
            validate_action(
                &ControllerAction::Finish {
                    session_id: "session-1".into(),
                },
                &active,
            )
            .is_ok()
        );
        assert_eq!(
            validate_action(
                &ControllerAction::Resume {
                    session_id: "session-1".into(),
                    profile_id: "codex-1".into(),
                    target_id: "podman".into(),
                    queue: ResumeQueueDisposition::Start,
                },
                &active,
            )
            .unwrap_err()
            .status,
            StatusCode::BAD_REQUEST
        );

        let session = state.sessions.get_mut("session-1").unwrap();
        session.state = SessionState::Stopped;
        session.target = None;
        let saved = ViewerSnapshot::from_config_state(&config, &state, 2);
        assert_eq!(
            validate_action(
                &ControllerAction::Finish {
                    session_id: "session-1".into(),
                },
                &saved,
            )
            .unwrap_err()
            .status,
            StatusCode::BAD_REQUEST
        );
        assert!(
            validate_action(
                &ControllerAction::Resume {
                    session_id: "session-1".into(),
                    profile_id: "codex-1".into(),
                    target_id: "podman".into(),
                    queue: ResumeQueueDisposition::Start,
                },
                &saved,
            )
            .is_ok()
        );
    }

    #[test]
    fn resume_action_refuses_a_target_the_session_cannot_use() {
        let (mut config, mut state) = sample_config_state();
        // A project that only exists on GitHub cannot become a checkout on this
        // machine, so the bare target stays out of reach for its sessions.
        config.bundles.get_mut("hel").unwrap().repositories[0].local = None;
        let session = state.sessions.get_mut("session-1").unwrap();
        session.state = SessionState::Stopped;
        session.target = None;
        let snapshot = ViewerSnapshot::from_config_state(&config, &state, 1);
        assert_eq!(
            snapshot.sessions[0].incompatible_resume_targets,
            vec!["raw".to_owned()]
        );

        let error = validate_action(
            &ControllerAction::Resume {
                session_id: "session-1".into(),
                profile_id: "codex-1".into(),
                target_id: "raw".into(),
                queue: ResumeQueueDisposition::Start,
            },
            &snapshot,
        )
        .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn snapshot_endpoint_returns_only_public_projection() {
        let (app, _, _) = app();
        let cookie = login_cookie(&app).await;
        let response = app
            .oneshot(
                Request::get("/api/snapshot")
                    .header(COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("session-1"));
        assert!(!body.contains("secret-token"));
        assert!(!body.contains("native-secret-id"));
        assert!(!body.contains("/private/source/hel"));

        let snapshot: serde_json::Value = serde_json::from_str(&body).unwrap();
        let repository = &snapshot["bundles"][0]["repositories"][0];
        assert_eq!(repository["id"], "hel");
        assert_eq!(repository["github"], "owner/hel");
        assert_eq!(repository["destination"], "hel");
        assert!(repository.get("local").is_none());
    }

    #[tokio::test]
    async fn conversation_endpoint_returns_authenticated_bounded_deltas() {
        let transcript = BrowserTranscript {
            latest_seq: 8,
            window_start_seq: 3,
            reset: false,
            entries: vec![
                crate::hel_chat::BrowserTranscriptEntry {
                    id: 3,
                    updated_seq: 3,
                    role: "user",
                    label: "You".into(),
                    recorded_at_ms: None,
                    lines: vec!["begin".into()],
                },
                crate::hel_chat::BrowserTranscriptEntry {
                    id: 7,
                    updated_seq: 8,
                    role: "agent",
                    label: "Agent".into(),
                    recorded_at_ms: None,
                    lines: vec!["live".into()],
                },
            ],
        };
        let (app, _, _) =
            app_with_conversations(BTreeMap::from([("session-1".into(), transcript)]));
        let cookie = login_cookie(&app).await;
        let response = app
            .oneshot(
                Request::get("/api/conversations/session-1?after_seq=3")
                    .header(COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["latest_seq"], 8);
        assert_eq!(body["reset"], false);
        assert_eq!(body["entries"].as_array().unwrap().len(), 1);
        assert_eq!(body["entries"][0]["lines"][0], "live");
    }

    #[tokio::test]
    async fn conversation_read_receipt_never_contends_with_a_running_action() {
        let (app, mut actions, mut receipts) = app();
        let cookie = login_cookie(&app).await;
        // A prompt for the same session stays in flight for the whole test, so
        // a receipt that still travelled the action pipeline would either
        // queue behind it or be rejected for the occupied session slot.
        let prompt = tokio::spawn(
            app.clone().oneshot(
                Request::post("/api/actions")
                    .header(COOKIE, cookie.clone())
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"action":"prompt","session_id":"session-1","text":"ship it"}"#,
                    ))
                    .unwrap(),
            ),
        );
        let action = actions.recv().await.unwrap();

        let response = tokio::spawn(
            app.oneshot(
                Request::post("/api/conversations/session-1/read")
                    .header(COOKIE, cookie)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"through":42}"#))
                    .unwrap(),
            ),
        );
        let receipt = receipts.recv().await.unwrap();
        assert_eq!(receipt.session_id, "session-1");
        assert_eq!(receipt.through, 42);
        receipt.reply.send(Ok(())).unwrap();
        assert_eq!(
            response.await.unwrap().unwrap().status(),
            StatusCode::NO_CONTENT
        );
        assert!(
            actions.try_recv().is_err(),
            "a read receipt must not queue a controller action"
        );

        action.reply.send(ActionOutcome::Accepted).unwrap();
        assert_eq!(
            prompt.await.unwrap().unwrap().status(),
            StatusCode::ACCEPTED
        );
    }

    #[tokio::test]
    async fn each_rejected_action_keeps_its_own_status_and_guidance() {
        for (outcome, status, guidance) in [
            (
                ActionOutcome::Busy,
                StatusCode::TOO_MANY_REQUESTS,
                "concurrent action limit",
            ),
            (
                ActionOutcome::SessionBusy,
                StatusCode::CONFLICT,
                "another operation is already running",
            ),
            (
                ActionOutcome::NotCancellable,
                StatusCode::CONFLICT,
                "no cancellable operation",
            ),
            (
                ActionOutcome::Failed,
                StatusCode::INTERNAL_SERVER_ERROR,
                "could not start this action",
            ),
        ] {
            let (app, mut actions, _) = app();
            let cookie = login_cookie(&app).await;
            let response = tokio::spawn(
                app.oneshot(
                    Request::post("/api/actions")
                        .header(COOKIE, cookie)
                        .header(CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            r#"{"action":"finish","session_id":"session-1"}"#,
                        ))
                        .unwrap(),
                ),
            );
            let request = actions.recv().await.unwrap();
            request.reply.send(outcome).unwrap();

            let response = response.await.unwrap().unwrap();
            assert_eq!(response.status(), status, "{outcome:?}");
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let error = body["error"].as_str().unwrap();
            assert!(error.contains(guidance), "{outcome:?} answered {error:?}");
        }
    }

    #[tokio::test]
    async fn the_viewer_shows_a_session_whose_action_failed_after_it_was_accepted() {
        // An accepted action reports its outcome only through snapshots, so
        // the page has to react to `has_error` for a late failure to be
        // visible at all.
        let (app, _, _) = app();
        let page = app
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let page = String::from_utf8(page.to_vec()).unwrap();
        assert!(page.contains("x.has_error?"), "viewer ignores has_error");
    }

    #[tokio::test]
    async fn repeated_wrong_codes_lock_the_login_endpoint() {
        let (app, _, _) = app();
        let attempt = |code: &'static str| {
            let app = app.clone();
            async move {
                app.oneshot(
                    Request::post("/auth/session")
                        .header(CONTENT_TYPE, "application/json")
                        .body(Body::from(format!(r#"{{"code":"{code}"}}"#)))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
            }
        };
        for _ in 0..MAX_CODE_FAILURES {
            assert_eq!(attempt("000000").await, StatusCode::UNAUTHORIZED);
        }
        assert_eq!(attempt("000000").await, StatusCode::TOO_MANY_REQUESTS);
        // Even the right code waits out the lockout, so guessing cannot be
        // hidden behind a correct-looking attempt.
        assert_eq!(attempt("123456").await, StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn viewer_code_lockouts_lengthen_instead_of_resetting_after_every_wait() {
        let serve_one_lockout = |guard: &mut CodeGuard, now: Instant| {
            for _ in 0..MAX_CODE_FAILURES {
                assert!(!guard.locked_at(now));
                guard.record_failure_at(now);
            }
            assert!(guard.locked_at(now));
            guard.locked_until.expect("the guard is locked") - now
        };

        let start = Instant::now();
        let mut guard = CodeGuard::default();
        let first = serve_one_lockout(&mut guard, start);
        assert_eq!(first, CODE_LOCKOUT_BASE);

        // Waiting out a lockout buys another run of attempts, not another
        // equally short lockout: a guard that reset here gave an attacker
        // MAX_CODE_FAILURES guesses every CODE_LOCKOUT_BASE for ever.
        let second_round = start + first;
        let second = serve_one_lockout(&mut guard, second_round);
        assert_eq!(second, CODE_LOCKOUT_BASE * 2);
        let third = serve_one_lockout(&mut guard, second_round + second);
        assert_eq!(third, CODE_LOCKOUT_BASE * 4);
        assert_eq!(code_lockout(u32::MAX), CODE_LOCKOUT_CAP);

        // A correct code clears the history, so one mistyped digit tomorrow
        // still costs only the shortest wait.
        let mut recovered = CodeGuard::default();
        assert_eq!(serve_one_lockout(&mut recovered, start), CODE_LOCKOUT_BASE);
    }

    #[test]
    fn persisted_cookie_key_survives_a_restart_and_stays_owner_only() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("phone-cookie-key");

        let first = load_or_create_cookie_key(&path).unwrap();
        assert!(first.len() >= COOKIE_KEY_BYTES);
        assert_eq!(std::fs::read(&path).unwrap(), first);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        // Two server processes started from the same key file honour each
        // other's cookies; a process that kept its generated key would not.
        let mut restarted = detached_options();
        restarted
            .set_cookie_key(load_or_create_cookie_key(&path).unwrap())
            .unwrap();
        let mut original = detached_options();
        original.set_cookie_key(first.clone()).unwrap();
        let cookie = signed_cookie_value(&original.cookie_key, 200);
        assert!(session_cookie_valid(&restarted.cookie_key, &cookie, 100));
        assert!(!session_cookie_valid(
            &detached_options().cookie_key,
            &cookie,
            100
        ));

        // Deleting the key file is the explicit sign-everyone-out gesture.
        std::fs::remove_file(&path).unwrap();
        let rotated = load_or_create_cookie_key(&path).unwrap();
        assert_ne!(rotated, first);
        assert!(!session_cookie_valid(&rotated, &cookie, 100));
    }

    #[test]
    fn corrupt_cookie_key_is_regenerated_instead_of_blocking_startup() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("phone-cookie-key");
        std::fs::write(&path, b"short").unwrap();

        let key = load_or_create_cookie_key(&path).unwrap();

        assert!(key.len() >= COOKIE_KEY_BYTES);
        assert_eq!(std::fs::read(&path).unwrap(), key);
        assert_eq!(load_or_create_cookie_key(&path).unwrap(), key);
    }
}
