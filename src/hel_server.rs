//! Explicit, phone-oriented control surface for Hel.
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
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, COOKIE, HeaderValue, SET_COOKIE};
use axum::http::{Response, StatusCode};
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
use crate::hel_state::{HelState, SessionState};

const COOKIE_NAME: &str = "hel_viewer_session";
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const EPHEMERAL_SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_BODY_BYTES: usize = 128 * 1024;
const MAX_CODE_FAILURES: u32 = 5;
const CODE_LOCKOUT: Duration = Duration::from_secs(30);
const MAX_TITLE_CHARS: usize = 120;
const MAX_PROMPT_CHARS: usize = 64 * 1024;

/// Options for the explicit `hel server` process.
///
/// `ServerOptions::new` generates both the six-digit viewer code and an
/// ephemeral cookie key. A caller that wants cookies to survive server
/// restarts can replace `cookie_key` with bytes loaded from its private Hel
/// data directory. The key and viewer code are intentionally omitted from
/// `Debug` output.
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub snapshot_rx: watch::Receiver<ViewerSnapshot>,
    pub conversation_rx: watch::Receiver<BTreeMap<String, BrowserTranscript>>,
    pub action_tx: mpsc::Sender<ControllerRequest>,
    pub shutdown: CancellationToken,
    pub session_ttl: Duration,
    /// Keep this enabled for direct HTTPS or an HTTPS reverse proxy. It may be
    /// disabled only for an explicitly trusted HTTP development endpoint.
    pub secure_cookie: bool,
    tls_config: Option<axum_server::tls_rustls::RustlsConfig>,
    viewer_code: String,
    cookie_key: Vec<u8>,
}

impl ServerOptions {
    pub fn new(
        bind: SocketAddr,
        snapshot_rx: watch::Receiver<ViewerSnapshot>,
        conversation_rx: watch::Receiver<BTreeMap<String, BrowserTranscript>>,
        action_tx: mpsc::Sender<ControllerRequest>,
    ) -> AnyResult<Self> {
        Ok(Self {
            bind,
            snapshot_rx,
            conversation_rx,
            action_tx,
            shutdown: CancellationToken::new(),
            session_ttl: DEFAULT_SESSION_TTL,
            secure_cookie: true,
            tls_config: None,
            viewer_code: generate_viewer_code()?,
            cookie_key: generate_cookie_key()?.to_vec(),
        })
    }

    pub fn viewer_code(&self) -> &str {
        &self.viewer_code
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
            key.len() >= 32,
            "cookie signing key must be at least 32 bytes"
        );
        self.cookie_key = key;
        Ok(())
    }

    #[cfg(test)]
    fn with_test_credentials(mut self, code: &str, key: &[u8]) -> Self {
        self.viewer_code = code.to_string();
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerSnapshot {
    pub revision: u64,
    pub generated_at: String,
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
            .map(|session| ViewerSession {
                id: session.id.clone(),
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
                conversation_available: false,
                incompatible_resume_targets: config
                    .targets
                    .keys()
                    .filter(|target_id| {
                        crate::hel_controller::resume_compatibility(session, config, target_id)
                            .is_err()
                    })
                    .cloned()
                    .collect(),
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
            sessions,
            profiles,
            targets,
            bundles,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewerSession {
    pub id: String,
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
    pub conversation_available: bool,
    /// Target ids this session cannot resume on. Only the ids travel: the
    /// controller's reasons name project paths and SSH hosts, which this
    /// projection deliberately keeps on the controller.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub incompatible_resume_targets: Vec<String>,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ControllerAction {
    New {
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
    Read {
        session_id: String,
        through: u64,
    },
    Prompt {
        session_id: String,
        text: String,
    },
    Close {
        session_id: String,
    },
    Cancel {
        session_id: String,
    },
    RemoveQueuedPrompt {
        session_id: String,
        queue_id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResumeQueueDisposition {
    Start,
    Discard,
}

#[derive(Debug)]
pub struct ControllerRequest {
    pub action: ControllerAction,
    pub reply: tokio::sync::oneshot::Sender<Result<(), String>>,
}

#[derive(Clone)]
struct ServerState {
    snapshot_rx: watch::Receiver<ViewerSnapshot>,
    conversation_rx: watch::Receiver<BTreeMap<String, BrowserTranscript>>,
    action_tx: mpsc::Sender<ControllerRequest>,
    viewer_code: Arc<str>,
    cookie_key: Arc<[u8]>,
    session_ttl: Duration,
    secure_cookie: bool,
    code_guard: Arc<Mutex<CodeGuard>>,
}

#[derive(Debug, Default)]
struct CodeGuard {
    failures: u32,
    locked_until: Option<Instant>,
}

fn router(options: ServerOptions) -> Router {
    let state = ServerState {
        snapshot_rx: options.snapshot_rx,
        conversation_rx: options.conversation_rx,
        action_tx: options.action_tx,
        viewer_code: options.viewer_code.into(),
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
        .route("/api/actions", post(action))
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
    let mut response = StatusCode::NO_CONTENT.into_response();
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

async fn action(
    State(state): State<ServerState>,
    Json(action): Json<ControllerAction>,
) -> Result<StatusCode, ApiError> {
    validate_action(&action, &state.snapshot_rx.borrow())?;
    let (reply, result) = tokio::sync::oneshot::channel();
    state
        .action_tx
        .send(ControllerRequest { action, reply })
        .await
        .map_err(|_| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "controller unavailable"))?;
    result
        .await
        .map_err(|_| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "controller unavailable"))?
        .map_err(|_| ApiError::new(StatusCode::CONFLICT, "action failed"))?;
    Ok(StatusCode::ACCEPTED)
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
    Json(request): Json<ReadRequest>,
) -> Result<StatusCode, ApiError> {
    validate_public_id(&session_id)?;
    require_session_record(&state.snapshot_rx.borrow(), &session_id)?;
    let (reply, result) = tokio::sync::oneshot::channel();
    state
        .action_tx
        .send(ControllerRequest {
            action: ControllerAction::Read {
                session_id,
                through: request.through,
            },
            reply,
        })
        .await
        .map_err(|_| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "controller unavailable"))?;
    result
        .await
        .map_err(|_| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "controller unavailable"))?
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

fn validate_action(action: &ControllerAction, snapshot: &ViewerSnapshot) -> Result<(), ApiError> {
    match action {
        ControllerAction::New {
            profile_id,
            bundle_id,
            target_id,
            title,
            project_directory,
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
        ControllerAction::Open { session_id }
        | ControllerAction::Close { session_id }
        | ControllerAction::Cancel { session_id }
        | ControllerAction::Read { session_id, .. } => {
            validate_public_id(session_id)?;
            require_session_record(snapshot, session_id)?;
        }
        ControllerAction::Prompt { session_id, text } => {
            validate_public_id(session_id)?;
            require_session_record(snapshot, session_id)?;
            if text.trim().is_empty() || text.chars().count() > MAX_PROMPT_CHARS {
                return Err(ApiError::bad_request(
                    "prompt must contain 1-65536 characters",
                ));
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
    let mut guard = state.code_guard.lock().expect("viewer code guard poisoned");
    match guard.locked_until {
        Some(until) if Instant::now() < until => true,
        Some(_) => {
            *guard = CodeGuard::default();
            false
        }
        None => false,
    }
}

fn record_code_failure(state: &ServerState) {
    let mut guard = state.code_guard.lock().expect("viewer code guard poisoned");
    guard.failures = guard.failures.saturating_add(1);
    if guard.failures >= MAX_CODE_FAILURES {
        guard.failures = 0;
        guard.locked_until = Some(Instant::now() + CODE_LOCKOUT);
    }
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

fn generate_cookie_key() -> AnyResult<[u8; 32]> {
    let mut key = [0_u8; 32];
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
        SessionState::Closing => "closing",
        SessionState::Destroying => "destroying",
        SessionState::Archived => "archived",
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
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="theme-color" content="#08090d"><link rel="manifest" href="/manifest.webmanifest"><title>Hel</title>
<style>:root{color-scheme:dark;font:16px system-ui;background:#08090d;color:#ecf2e5}body{margin:0;padding:env(safe-area-inset-top) 16px env(safe-area-inset-bottom);max-width:760px;margin:auto}header{display:flex;align-items:baseline;justify-content:space-between}h1{font-size:42px;letter-spacing:.06em;margin:22px 0 4px;color:#b9ff5a}.dim{color:#899184}.card{background:#13161d;border:1px solid #292e38;border-radius:14px;margin:12px 0;padding:14px}.row{display:flex;gap:8px;flex-wrap:wrap}button,input,select,textarea{font:inherit;color:inherit;background:#1d222b;border:1px solid #3b424e;border-radius:9px;padding:10px}button{background:#b9ff5a;color:#10140b;font-weight:700}button:disabled{opacity:.45}.danger{background:#ff786f}.secondary{background:#303743;color:#ecf2e5}.hidden{display:none}.pill{font-size:12px;border:1px solid #475043;border-radius:99px;padding:3px 8px}.session h3{margin:0 0 8px}.session p{margin:5px 0}.preview{white-space:pre-wrap;border-left:2px solid #475043;padding-left:10px}.entry{border-left:3px solid #475043;padding:4px 0 4px 12px;margin:15px 0}.entry.user{border-color:#5dd9ff}.entry.agent{border-color:#91df62}.entry.thought,.entry.system{border-color:#59616d;color:#aab1a5}.entry.tool{border-color:#e2b34d}.entry.plan{border-color:#d985ff}.entry strong{display:block;margin-bottom:5px}.entry pre{font:inherit;white-space:pre-wrap;overflow-wrap:anywhere;margin:0}.queue-item{display:flex;gap:8px;align-items:start;justify-content:space-between;border-top:1px solid #292e38;padding:8px 0}.queue-item span{white-space:pre-wrap;overflow-wrap:anywhere}textarea{width:100%;box-sizing:border-box;min-height:76px}#conversation-feed{min-height:30vh}</style></head>
<body><header><div><h1>HEL</h1><div class="dim">Welcome to Hel.</div></div><button id="logout" class="hidden">Sign out</button></header>
<main id="login" class="card"><h2>Unlock viewer</h2><p class="dim">Enter the six-digit code shown by <code>hel server</code>.</p><form id="login-form" class="row"><input id="code" inputmode="numeric" autocomplete="one-time-code" pattern="[0-9]{6}" maxlength="6" placeholder="000000" required><button>Enter</button></form><p id="login-error"></p></main>
<main id="app" class="hidden"><section id="dashboard"><section class="card"><h2>New session</h2><form id="new-form" class="row"><input id="new-title" maxlength="120" placeholder="Session title" required><select id="new-profile" aria-label="Profile"></select><select id="new-bundle" aria-label="Bundle"></select><select id="new-target" aria-label="Target"></select><input id="new-project-directory" class="hidden" placeholder="Absolute project directory"><button>Start</button></form><p id="action-error"></p></section><section><h2>Sessions</h2><div id="sessions"></div></section><section class="card"><h2>Configured</h2><div id="configured"></div></section></section><section id="conversation" class="hidden"><button id="back" class="secondary">← Dashboard</button><div class="card"><h2 id="conversation-title">Conversation</h2><span id="conversation-state" class="pill"></span><div id="conversation-feed"></div></div><section class="card"><h3>Queued prompts</h3><div id="conversation-queue"></div></section><form id="prompt-form" class="card"><textarea id="prompt-text" maxlength="65536" placeholder="Message the agent" required></textarea><button>Send or queue</button><p id="conversation-error"></p></form></section></main>
<script>
const login=document.querySelector('#login'),app=document.querySelector('#app'),dashboard=document.querySelector('#dashboard'),conversation=document.querySelector('#conversation'),sessions=document.querySelector('#sessions'),configured=document.querySelector('#configured'),logout=document.querySelector('#logout'),newForm=document.querySelector('#new-form'),newProfile=document.querySelector('#new-profile'),newBundle=document.querySelector('#new-bundle'),newTarget=document.querySelector('#new-target'),newProjectDirectory=document.querySelector('#new-project-directory'),actionError=document.querySelector('#action-error'),feed=document.querySelector('#conversation-feed'),queue=document.querySelector('#conversation-queue');let snapshot,currentSession,cursor=0,eventsStarted=false;
async function request(url,options={}){const response=await fetch(url,{...options,headers:{'content-type':'application/json',...(options.headers||{})}});if(response.status===401)throw new Error('unauthorized');if(!response.ok){const body=await response.json().catch(()=>({}));throw new Error(body.error||response.statusText)}return response.status===204?null:response.json()}
function options(items,selected){return items.map(x=>`<option value="${escapeAttr(x.id)}" ${x.id===selected?'selected':''}>${escapeHtml(x.id)}</option>`).join('')}
function syncProjectDirectory(){const required=snapshot?.targets.find(x=>x.id===newTarget.value)?.requires_project_directory===true;newProjectDirectory.classList.toggle('hidden',!required);newProjectDirectory.required=required;if(!required)newProjectDirectory.value=''}
async function refresh(){try{snapshot=await request('/api/snapshot');login.classList.add('hidden');app.classList.remove('hidden');logout.classList.remove('hidden');if(!newProfile.value)newProfile.innerHTML=options(snapshot.profiles);if(!newBundle.value)newBundle.innerHTML=options(snapshot.bundles);if(!newTarget.value)newTarget.innerHTML=options(snapshot.targets);syncProjectDirectory();sessions.innerHTML=snapshot.sessions.map(x=>`<article class="card session"><h3>${escapeHtml(x.title)}</h3><p><span class="pill">${escapeHtml(x.state)}</span> ${escapeHtml(x.harness_kind)} · ${escapeHtml(x.profile_id)}</p><p class="dim">${escapeHtml(x.bundle_id)} → ${escapeHtml(x.target_id)} · ${(x.queued_prompts||[]).length} queued</p>${x.preview?.length?`<p class="preview">${x.preview.map(escapeHtml).join('\n')}</p>`:''}<div class="row"><button data-action="open" data-id="${escapeAttr(x.id)}" ${x.conversation_available?'':'disabled'}>Open</button>${x.state==='provisioning'?`<button class="danger" data-action="cancel" data-id="${escapeAttr(x.id)}">Cancel</button>`:`<button data-action="resume" data-id="${escapeAttr(x.id)}" data-profile="${escapeAttr(x.profile_id)}" data-target="${escapeAttr(x.target_id)}">Resume</button><button class="danger" data-action="close" data-id="${escapeAttr(x.id)}">Archive</button>`}</div></article>`).join('')||'<p class="dim">No Hel-managed sessions.</p>';const profileRows=snapshot.profiles.map(p=>`<p><strong>${escapeHtml(p.id)}</strong> · ${escapeHtml(p.harness_kind)}<br><span class="dim">${p.quota?escapeHtml(p.quota.summary)+(p.quota.stale?' · stale':'')+(p.quota.has_error?' · unavailable':''):'quota unavailable'}</span></p>`).join('');configured.innerHTML=profileRows+`<p class="dim">${snapshot.targets.length} targets · ${snapshot.bundles.length} bundles</p>`;if(currentSession){const session=snapshot.sessions.find(x=>x.id===currentSession);if(!session?.conversation_available){showDashboard()}else{renderQueue(session);document.querySelector('#conversation-state').textContent=session.state}}if(!eventsStarted){eventsStarted=true;const source=new EventSource('/api/events');source.addEventListener('revision',()=>{refresh();if(currentSession)loadConversation(true)})}}catch(e){if(e.message==='unauthorized'){login.classList.remove('hidden');app.classList.add('hidden');logout.classList.add('hidden')}}}
function renderQueue(session){queue.innerHTML=(session.queued_prompts||[]).map((x,i)=>`<div class="queue-item"><span>${i+1}. ${escapeHtml(x.text)}</span><button class="danger" data-queue-id="${escapeAttr(x.id)}">Remove</button></div>`).join('')||'<p class="dim">No queued prompts.</p>'}
function renderEntries(entries,replace){if(replace)feed.innerHTML='';for(const entry of entries){let node=document.querySelector(`[data-entry-id="${entry.id}"]`);if(!node){node=document.createElement('article');node.dataset.entryId=entry.id;feed.append(node)}node.className=`entry ${entry.role}`;const title=document.createElement('strong');title.textContent=entry.label;const body=document.createElement('pre');body.textContent=entry.lines.join('\n');node.replaceChildren(title,body)}window.scrollTo(0,document.body.scrollHeight)}
async function loadConversation(delta=false){if(!currentSession)return;try{const result=await request(`/api/conversations/${encodeURIComponent(currentSession)}${delta&&cursor?`?after_seq=${cursor}`:''}`);renderEntries(result.entries,!delta||result.reset);cursor=result.latest_seq;await request(`/api/conversations/${encodeURIComponent(currentSession)}/read`,{method:'POST',body:JSON.stringify({through:cursor})})}catch(err){document.querySelector('#conversation-error').textContent=err.message}}
async function openConversation(id){currentSession=id;cursor=0;location.hash=`conversation/${id}`;dashboard.classList.add('hidden');conversation.classList.remove('hidden');const session=snapshot.sessions.find(x=>x.id===id);document.querySelector('#conversation-title').textContent=session?.title||'Conversation';document.querySelector('#conversation-state').textContent=session?.state||'';renderQueue(session||{});await loadConversation(false)}
function showDashboard(){currentSession=null;cursor=0;location.hash='';conversation.classList.add('hidden');dashboard.classList.remove('hidden')}
document.querySelector('#login-form').onsubmit=async e=>{e.preventDefault();try{await request('/auth/session',{method:'POST',body:JSON.stringify({code:document.querySelector('#code').value})});document.querySelector('#login-error').textContent='';refresh()}catch(err){document.querySelector('#login-error').textContent=err.message}};
logout.onclick=async()=>{await request('/auth/session',{method:'DELETE'});location.reload()};
newTarget.onchange=syncProjectDirectory;
newForm.onsubmit=async e=>{e.preventDefault();const target=snapshot.targets.find(x=>x.id===newTarget.value);try{await request('/api/actions',{method:'POST',body:JSON.stringify({action:'new',title:document.querySelector('#new-title').value,profile_id:newProfile.value,bundle_id:newBundle.value,target_id:newTarget.value,project_directory:target?.requires_project_directory?newProjectDirectory.value:null})});document.querySelector('#new-title').value='';actionError.textContent='';await refresh()}catch(err){actionError.textContent=err.message}};
sessions.onclick=async e=>{const button=e.target.closest('button[data-action]');if(!button)return;if(button.dataset.action==='open')return openConversation(button.dataset.id);if(button.dataset.action==='close'&&!confirm('Save a recovery copy, archive, and destroy this session target? Queued prompts will be preserved.'))return;const body={action:button.dataset.action,session_id:button.dataset.id};if(button.dataset.action==='resume'){body.profile_id=button.dataset.profile;body.target_id=button.dataset.target;const session=snapshot.sessions.find(x=>x.id===button.dataset.id);body.queue='start';if(session?.queued_prompts?.length){const choice=prompt(`This session has ${session.queued_prompts.length} queued prompt(s). Type start to run them after resume, or discard to remove them.`,'start');if(choice===null)return;if(!['start','discard'].includes(choice.toLowerCase()))return alert('Enter start or discard.');body.queue=choice.toLowerCase()}}try{await request('/api/actions',{method:'POST',body:JSON.stringify(body)});actionError.textContent='';await refresh()}catch(err){actionError.textContent=err.message}};
document.querySelector('#back').onclick=showDashboard;
document.querySelector('#prompt-form').onsubmit=async e=>{e.preventDefault();const text=document.querySelector('#prompt-text');try{await request('/api/actions',{method:'POST',body:JSON.stringify({action:'prompt',session_id:currentSession,text:text.value})});text.value='';document.querySelector('#conversation-error').textContent='';await refresh()}catch(err){document.querySelector('#conversation-error').textContent=err.message}};
queue.onclick=async e=>{const button=e.target.closest('button[data-queue-id]');if(!button)return;try{await request('/api/actions',{method:'POST',body:JSON.stringify({action:'remove-queued-prompt',session_id:currentSession,queue_id:button.dataset.queueId})});await refresh()}catch(err){document.querySelector('#conversation-error').textContent=err.message}};
function escapeHtml(value){const e=document.createElement('span');e.textContent=value;return e.innerHTML}function escapeAttr(value){return escapeHtml(value).replaceAll('"','&quot;')}
if('serviceWorker'in navigator)navigator.serviceWorker.register('/service-worker.js');refresh().then(()=>{const match=location.hash.match(/^#conversation\/([A-Za-z0-9_-]+)$/);if(match)openConversation(match[1])});
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
    use crate::hel_state::{STATE_VERSION, SessionRecord};

    fn sample_config_state() -> (HelConfig, HelState) {
        let config = HelConfig {
            version: CONFIG_VERSION,
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
                    target: None,
                    native_session_id: Some("native-secret-id".into()),
                    acp_session_title: Some("Build Hel".into()),
                    session_title_override: None,
                    created_at: "now".into(),
                    updated_at: "now".into(),
                    detached_after_event_ordinal: 0,
                    draft_input: String::new(),
                    last_error: Some("secret-token at /highly/secret/codex".into()),
                    last_checkpoint_error: None,
                    checkpoint: None,
                },
            )]),
            mount_history: BTreeMap::new(),
        };
        (config, state)
    }

    fn app() -> (Router, mpsc::Receiver<ControllerRequest>) {
        app_with_conversations(BTreeMap::new())
    }

    fn app_with_conversations(
        conversations: BTreeMap<String, BrowserTranscript>,
    ) -> (Router, mpsc::Receiver<ControllerRequest>) {
        let (config, state) = sample_config_state();
        let snapshot = ViewerSnapshot::from_config_state(&config, &state, 1);
        let (_snapshot_tx, snapshot_rx) = watch::channel(snapshot);
        let (_conversation_tx, conversation_rx) = watch::channel(conversations);
        let (action_tx, action_rx) = mpsc::channel(8);
        let options = ServerOptions::new(
            "127.0.0.1:0".parse().unwrap(),
            snapshot_rx,
            conversation_rx,
            action_tx,
        )
        .unwrap()
        .with_test_credentials("123456", b"01234567890123456789012345678901");
        (router(options), action_rx)
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
        let (app, _) = app();
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
        assert!(json.contains("\"has_error\":true"));
    }

    #[tokio::test]
    async fn valid_action_is_typed_and_forwarded() {
        let (app, mut actions) = app();
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
            }
        );
        action.reply.send(Ok(())).unwrap();
        let response = response.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn bare_new_action_forwards_an_explicit_safe_project_directory() {
        let (app, mut actions) = app();
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
                profile_id: "codex-1".into(),
                bundle_id: "hel".into(),
                target_id: "raw".into(),
                title: "Raw work".into(),
                project_directory: Some(PathBuf::from("/work/project")),
            }
        );
        action.reply.send(Ok(())).unwrap();
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
        let (app, mut actions) = app();
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
        action.reply.send(Ok(())).unwrap();
        assert_eq!(
            response.await.unwrap().unwrap().status(),
            StatusCode::ACCEPTED
        );
    }

    #[tokio::test]
    async fn action_validation_accepts_cross_harness_resume_and_rejects_unknown() {
        let (mut config, state) = sample_config_state();
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
            &ControllerAction::Close {
                session_id: "not-managed".into(),
            },
            &snapshot,
        )
        .unwrap_err();
        assert_eq!(error.status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn resume_action_refuses_a_target_the_session_cannot_use() {
        let (mut config, state) = sample_config_state();
        // A project that only exists on GitHub cannot become a checkout on this
        // machine, so the bare target stays out of reach for its sessions.
        config.bundles.get_mut("hel").unwrap().repositories[0].local = None;
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
        let (app, _) = app();
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
        let (app, _) = app_with_conversations(BTreeMap::from([("session-1".into(), transcript)]));
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
    async fn conversation_read_receipt_is_typed_and_forwarded() {
        let (app, mut actions) = app();
        let cookie = login_cookie(&app).await;
        let response = tokio::spawn(
            app.oneshot(
                Request::post("/api/conversations/session-1/read")
                    .header(COOKIE, cookie)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"through":42}"#))
                    .unwrap(),
            ),
        );
        let action = actions.recv().await.unwrap();
        assert_eq!(
            action.action,
            ControllerAction::Read {
                session_id: "session-1".into(),
                through: 42,
            }
        );
        action.reply.send(Ok(())).unwrap();
        assert_eq!(
            response.await.unwrap().unwrap().status(),
            StatusCode::NO_CONTENT
        );
    }
}
