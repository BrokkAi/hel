//! Harness credential interpretation and convergence.
//!
//! Hel clones a profile's harness credentials into every session's isolated
//! home. Rotating OAuth refresh tokens make those copies diverge: the first
//! copy to refresh invalidates the grant every other copy still holds. This
//! module owns the one interpretation of credential files Hel has, plus the
//! background service that reconciles the controller-side canonical copy with
//! each live session's copy in both directions.
//!
//! The same reconcile loop also pushes each profile's synced skills trees
//! (see `hel_skills`) into live sessions. Skills are not secrets and do not
//! rotate, so they converge in one direction only: the canonical home wins.
//!
//! Credential bytes travel only in worker request and response frames. They
//! never enter the durable event stream or a checkpoint archive. Fingerprints
//! and freshness timestamps are not secret and may appear in logs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, watch};

use crate::hel_config::{HarnessKind, HarnessProfile};
use crate::hel_setup::harness_authentication_marker;
use crate::hel_targets::CommandSpec;
use crate::hel_worker::{RelayEvent, RelayObservation};
use crate::hel_worker_client::RelayClient;

/// Credential files are small JSON documents. The cap keeps a hostile or
/// corrupt worker from making the controller buffer an arbitrary payload.
pub const MAX_CREDENTIAL_BYTES: usize = 1024 * 1024;

/// How often the coordinator reconciles every profile with its live sessions.
pub const SYNC_INTERVAL: Duration = Duration::from_secs(60);

pub fn credential_fingerprint(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Epoch milliseconds describing how current a credential copy is. Higher wins
/// when two copies of the same grant differ.
///
/// Key structure confirmed against real files on a developer machine:
/// * Claude `~/.claude/.credentials.json`: `{ "claudeAiOauth": { "accessToken",
///   "refreshToken", "expiresAt", "refreshTokenExpiresAt", "scopes",
///   "subscriptionType", "rateLimitTier" } }`. `expiresAt` is a 13-digit
///   number, so already epoch milliseconds.
/// * Codex `~/.codex/auth.json`: `{ "auth_mode", "tokens": { "access_token",
///   "refresh_token", "id_token", "account_id" }, "last_refresh" }`.
///   `last_refresh` is an RFC3339 string with fractional seconds and a `Z`
///   suffix.
/// * Kimi `~/.kimi-code/credentials/kimi-code.json`: `{ "access_token",
///   "refresh_token", "expires_at", "expires_in", "scope", "token_type" }`.
///   `expires_at` is a 10-digit number, so epoch seconds.
/// * Grok `~/.grok/auth.json`: an object keyed by `"<issuer>::<uuid>"`, each
///   value holding `{ "key", "refresh_token", "expires_at", ... }`.
///   `expires_at` is an RFC3339 string with nanosecond precision and a `Z`
///   suffix. A file may hold several grants, so the latest expiry wins.
///
/// Anything unparseable is `None` rather than a guess.
pub fn credential_freshness(kind: HarnessKind, bytes: &[u8]) -> Option<i64> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    match kind {
        HarnessKind::Claude => value.get("claudeAiOauth")?.get("expiresAt")?.as_i64(),
        HarnessKind::Codex => {
            let last_refresh = value.get("last_refresh")?.as_str()?;
            chrono::DateTime::parse_from_rfc3339(last_refresh)
                .ok()
                .map(|refreshed| refreshed.timestamp_millis())
        }
        HarnessKind::Kimi => value
            .get("expires_at")?
            .as_i64()
            .and_then(|seconds| seconds.checked_mul(1000)),
        HarnessKind::Grok => value
            .as_object()?
            .values()
            .filter_map(|grant| {
                let expires_at = grant.get("expires_at")?.as_str()?;
                chrono::DateTime::parse_from_rfc3339(expires_at)
                    .ok()
                    .map(|expiry| expiry.timestamp_millis())
            })
            .max(),
    }
}

/// Reject anything that is not a plausible credential document before it
/// replaces a canonical file or lands in a session home.
pub fn validate_credential_payload(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        bail!("credential payload is empty");
    }
    if bytes.len() > MAX_CREDENTIAL_BYTES {
        bail!(
            "credential payload is {} bytes, above the {MAX_CREDENTIAL_BYTES} byte limit",
            bytes.len()
        );
    }
    let text = std::str::from_utf8(bytes).context("credential payload is not valid UTF-8")?;
    serde_json::from_str::<serde_json::Value>(text).context("credential payload is not JSON")?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialSnapshot {
    pub present: bool,
    pub fingerprint: String,
    pub freshness_epoch_ms: Option<i64>,
}

impl CredentialSnapshot {
    pub fn absent() -> Self {
        Self {
            present: false,
            fingerprint: String::new(),
            freshness_epoch_ms: None,
        }
    }

    pub fn of(kind: HarnessKind, bytes: &[u8]) -> Self {
        Self {
            present: true,
            fingerprint: credential_fingerprint(bytes),
            freshness_epoch_ms: credential_freshness(kind, bytes),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncAction {
    /// Install the canonical copy into the session.
    Push,
    /// Adopt the session's copy as canonical.
    Pull,
    /// The copies already agree, or nothing can be decided.
    None,
}

/// Decide which way a profile's canonical copy and one session's copy converge.
///
/// When both sides are present, differ, and neither reports freshness, Hel
/// refuses to guess: file modification times are not a reliable proxy for grant
/// age across a container boundary.
pub fn reconcile(canonical: &CredentialSnapshot, session: &CredentialSnapshot) -> SyncAction {
    match (canonical.present, session.present) {
        (false, false) => SyncAction::None,
        (true, false) => SyncAction::Push,
        (false, true) => SyncAction::Pull,
        (true, true) => {
            if canonical.fingerprint == session.fingerprint {
                return SyncAction::None;
            }
            match (canonical.freshness_epoch_ms, session.freshness_epoch_ms) {
                (Some(canonical), Some(session)) if canonical > session => SyncAction::Push,
                (Some(canonical), Some(session)) if session > canonical => SyncAction::Pull,
                (Some(_), Some(_)) => SyncAction::None,
                (Some(_), None) => SyncAction::Push,
                (None, Some(_)) => SyncAction::Pull,
                (None, None) => SyncAction::None,
            }
        }
    }
}

/// Read a credential file, treating "not there" as a snapshot rather than an
/// error. A directory or unreadable file is an error worth surfacing.
pub fn read_credential_file(
    kind: HarnessKind,
    path: &Path,
) -> Result<(CredentialSnapshot, Vec<u8>)> {
    match std::fs::read(path) {
        Ok(bytes) => Ok((CredentialSnapshot::of(kind, &bytes), bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok((CredentialSnapshot::absent(), Vec::new()))
        }
        Err(error) => Err(error).with_context(|| format!("read credentials {}", path.display())),
    }
}

/// Replace a credential file without exposing a partial write or widening its
/// permissions. Refuses a symlinked destination so a compromised session home
/// cannot redirect the write.
pub fn write_credential_file(path: &Path, bytes: &[u8]) -> Result<()> {
    validate_credential_payload(bytes)?;
    if std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        bail!(
            "credential destination {} is a symbolic link",
            path.display()
        );
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }
    // `atomic_write` creates the temporary file with mode 0600 and renames it
    // over the destination, so the installed file is never world-readable.
    crate::hel_config::atomic_write(path, bytes)
}

/// Full-phrase markers that a harness rejected the session's credentials.
/// Kept tight on purpose: a false positive costs one redundant sync and one
/// notice, but a noisy list would train operators to ignore both.
const AUTH_FAILURE_PHRASES: [&str; 5] = [
    "OAuth session expired and could not be refreshed",
    "Please run /login",
    "authentication_error",
    "invalid_grant",
    // Hel's own marker for a turn the bridge failed with ACP `auth_required`.
    // The bridge's wording ("Authentication required") is too generic to match.
    crate::hel_acp::PROMPT_AUTH_REQUIRED_MARKER,
];

fn contains_auth_failure_signature(text: &str) -> bool {
    AUTH_FAILURE_PHRASES
        .iter()
        .any(|phrase| text.contains(phrase))
}

pub fn auth_failure_signature(_kind: HarnessKind, text: &str) -> bool {
    contains_auth_failure_signature(text)
}

/// Detect an authentication failure only in relay observations originating
/// from the harness. Durable prompt commands are deliberately excluded, so a
/// user merely mentioning an auth error cannot trigger credential sync.
pub fn relay_event_reports_auth_failure(event: &RelayEvent) -> bool {
    match &event.observation {
        RelayObservation::Warning { message } => contains_auth_failure_signature(message),
        RelayObservation::SessionUpdate { update } => serde_json::to_string(update.as_ref())
            .is_ok_and(|payload| contains_auth_failure_signature(&payload)),
        _ => false,
    }
}

pub fn events_report_auth_failure(_kind: HarnessKind, events: &[RelayEvent]) -> bool {
    events.iter().any(relay_event_reports_auth_failure)
}

/// Build the harness's own interactive login command for a profile.
///
/// Verified against the locally installed CLIs with `--help`: `codex login`,
/// `claude auth login` (there is no bare `claude login`), `kimi login`, and
/// `grok login`.
///
/// `profile.executable` overrides the *ACP bridge*, not the harness CLI: for
/// Codex and Claude it names an adapter binary (`codex-acp`,
/// `claude-agent-acp`) that has no login command, so only Kimi and Grok —
/// whose bridges are the `kimi` and `grok` CLIs themselves — honor the
/// override here.
pub fn login_command(profile: &HarnessProfile) -> (String, Vec<String>) {
    let overridable = |fallback: &str| {
        profile
            .executable
            .as_ref()
            .map(|executable| executable.to_string_lossy().into_owned())
            .unwrap_or_else(|| fallback.to_owned())
    };
    match profile.kind {
        HarnessKind::Codex => ("codex".to_owned(), vec!["login".to_owned()]),
        HarnessKind::Claude => (
            "claude".to_owned(),
            vec!["auth".to_owned(), "login".to_owned()],
        ),
        HarnessKind::Kimi => (overridable("kimi"), vec!["login".to_owned()]),
        HarnessKind::Grok => (overridable("grok"), vec!["login".to_owned()]),
    }
}

/// One live session the coordinator may reconcile with its profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialSyncTarget {
    pub session_id: String,
    pub profile_id: String,
    pub harness: HarnessKind,
    /// Controller-side canonical home for the profile.
    pub profile_home: PathBuf,
    /// Reconnect command for the session's worker proxy.
    pub spec: CommandSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialSyncAction {
    /// The canonical copy replaced the session's copy.
    Pushed,
    /// The session's fresher copy became canonical.
    Pulled,
    /// The canonical skills trees replaced the session's trees. Skills sync
    /// is push-only: the controller-side profile home stays authoritative.
    SkillsPushed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialSyncOutcome {
    pub session_id: String,
    /// Every action taken for the session, or why the reconcile failed. An
    /// empty action list means the copies already agreed.
    pub outcome: std::result::Result<Vec<CredentialSyncAction>, String>,
}

/// Reported to the UI loops only when something happened: an action was taken,
/// a session failed, or an on-demand sync finished with nothing to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialSyncResult {
    pub profile_id: String,
    /// Session whose authentication failure asked for this sync, when any.
    pub triggered_by: Option<String>,
    /// The whole reconcile stopped before it could report per-session
    /// outcomes. Kept separate so the failure is reported, never dropped.
    pub failure: Option<String>,
    pub outcomes: Vec<CredentialSyncOutcome>,
}

impl CredentialSyncResult {
    pub fn pushed_to(&self, session_id: &str) -> bool {
        self.outcomes.iter().any(|outcome| {
            outcome.session_id == session_id
                && outcome
                    .outcome
                    .as_ref()
                    .is_ok_and(|actions| actions.contains(&CredentialSyncAction::Pushed))
        })
    }

    pub fn failures(&self) -> impl Iterator<Item = (&str, &str)> {
        self.outcomes
            .iter()
            .filter_map(|outcome| match &outcome.outcome {
                Err(detail) => Some((outcome.session_id.as_str(), detail.as_str())),
                Ok(_) => None,
            })
    }

    /// Sessions that took at least one action of any kind.
    pub fn actions(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| {
                outcome
                    .outcome
                    .as_ref()
                    .is_ok_and(|actions| !actions.is_empty())
            })
            .count()
    }

    /// Sessions whose harness credentials were pushed or pulled.
    pub fn credential_sessions(&self) -> usize {
        self.count_actions(|action| {
            matches!(
                action,
                CredentialSyncAction::Pushed | CredentialSyncAction::Pulled
            )
        })
    }

    /// Sessions whose skills trees were replaced.
    pub fn skills_sessions(&self) -> usize {
        self.count_actions(|action| action == CredentialSyncAction::SkillsPushed)
    }

    fn count_actions(&self, wanted: impl Fn(CredentialSyncAction) -> bool) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| {
                outcome
                    .outcome
                    .as_ref()
                    .is_ok_and(|actions| actions.iter().copied().any(&wanted))
            })
            .count()
    }
}

#[derive(Debug, Clone)]
struct SyncTrigger {
    profile_id: String,
    session_id: Option<String>,
}

/// Handle the UI loops keep. Publishing targets and asking for an immediate
/// sync are both non-blocking.
#[derive(Clone)]
pub struct CredentialSyncHandle {
    targets: Arc<watch::Sender<Vec<CredentialSyncTarget>>>,
    triggers: mpsc::UnboundedSender<SyncTrigger>,
}

impl CredentialSyncHandle {
    pub fn set_targets(&self, targets: Vec<CredentialSyncTarget>) {
        if *self.targets.borrow() != targets {
            self.targets.send_replace(targets);
        }
    }

    /// Reconcile one profile now instead of waiting for the next cycle.
    pub fn sync_profile_now(&self, profile_id: &str, session_id: Option<&str>) {
        let _ = self.triggers.send(SyncTrigger {
            profile_id: profile_id.to_owned(),
            session_id: session_id.map(ToOwned::to_owned),
        });
    }
}

pub struct CredentialSyncCoordinator {
    handle: CredentialSyncHandle,
    results: mpsc::UnboundedReceiver<CredentialSyncResult>,
}

impl CredentialSyncCoordinator {
    pub fn spawn() -> Self {
        let (targets_tx, targets_rx) = watch::channel(Vec::new());
        let (triggers_tx, mut triggers_rx) = mpsc::unbounded_channel::<SyncTrigger>();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel::<CredentialSyncResult>();
        let (results_tx, results_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(SYNC_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // A pull rewrites the canonical file, so one profile is never
            // reconciled twice at once.
            let mut busy = BTreeSet::<String>::new();
            let mut queue = VecDeque::<SyncTrigger>::new();
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        for profile_id in profiles_with_targets(&targets_rx.borrow()) {
                            enqueue(&mut queue, SyncTrigger { profile_id, session_id: None });
                        }
                    }
                    trigger = triggers_rx.recv() => {
                        let Some(trigger) = trigger else { break };
                        enqueue(&mut queue, trigger);
                    }
                    completed = completed_rx.recv() => {
                        let Some(result) = completed else { break };
                        busy.remove(&result.profile_id);
                        if result.triggered_by.is_some()
                            || result.failure.is_some()
                            || !result.outcomes.is_empty()
                        {
                            let _ = results_tx.send(result);
                        }
                    }
                }

                let mut deferred = VecDeque::new();
                while let Some(trigger) = queue.pop_front() {
                    if busy.contains(&trigger.profile_id) {
                        deferred.push_back(trigger);
                        continue;
                    }
                    let targets: Vec<_> = targets_rx
                        .borrow()
                        .iter()
                        .filter(|target| target.profile_id == trigger.profile_id)
                        .cloned()
                        .collect();
                    if targets.is_empty() {
                        if trigger.session_id.is_some() {
                            let _ = results_tx.send(CredentialSyncResult {
                                profile_id: trigger.profile_id,
                                triggered_by: trigger.session_id,
                                failure: None,
                                outcomes: Vec::new(),
                            });
                        }
                        continue;
                    }
                    busy.insert(trigger.profile_id.clone());
                    let completed_tx = completed_tx.clone();
                    let handle = tokio::runtime::Handle::current();
                    // The blocking join is awaited so a panicked reconcile is
                    // reported and its profile always leaves the busy set.
                    tokio::spawn(async move {
                        let joined = tokio::task::spawn_blocking(move || {
                            handle.block_on(reconcile_profile(&targets))
                        })
                        .await;
                        let (failure, outcomes) = match joined {
                            Ok(outcomes) => (None, outcomes),
                            Err(error) => (Some(format!("sync task stopped: {error}")), Vec::new()),
                        };
                        let _ = completed_tx.send(CredentialSyncResult {
                            profile_id: trigger.profile_id,
                            triggered_by: trigger.session_id,
                            failure,
                            outcomes,
                        });
                    });
                }
                queue = deferred;
            }
        });
        Self {
            handle: CredentialSyncHandle {
                targets: Arc::new(targets_tx),
                triggers: triggers_tx,
            },
            results: results_rx,
        }
    }

    pub fn handle(&self) -> CredentialSyncHandle {
        self.handle.clone()
    }

    pub fn try_result(&mut self) -> Option<CredentialSyncResult> {
        self.results.try_recv().ok()
    }

    /// Waits for the next finished sync.
    ///
    /// Event-driven loops select on this instead of polling; `None` means the
    /// coordinator task has stopped. Cancel-safe, so a lost `select!` race
    /// keeps the result queued.
    pub async fn result(&mut self) -> Option<CredentialSyncResult> {
        self.results.recv().await
    }
}

fn profiles_with_targets(targets: &[CredentialSyncTarget]) -> Vec<String> {
    let mut profiles = BTreeSet::new();
    for target in targets {
        profiles.insert(target.profile_id.clone());
    }
    profiles.into_iter().collect()
}

fn enqueue(queue: &mut VecDeque<SyncTrigger>, trigger: SyncTrigger) {
    if trigger.session_id.is_none()
        && queue
            .iter()
            .any(|queued| queued.profile_id == trigger.profile_id)
    {
        return;
    }
    queue.push_back(trigger);
}

/// Reconcile one profile with every live session that runs it.
///
/// A pull makes every other session's copy stale by definition, so the pass
/// runs again once with the new canonical bytes. Two passes are enough: the
/// second cannot pull anything the first did not already see unless a harness
/// refreshed mid-cycle, and that lands in the next cycle.
async fn reconcile_profile(targets: &[CredentialSyncTarget]) -> Vec<CredentialSyncOutcome> {
    let mut outcomes = BTreeMap::<String, CredentialSyncOutcome>::new();
    for pass in 0..2 {
        let mut pulled = false;
        for target in targets {
            match reconcile_session(target).await {
                Ok(actions) if actions.is_empty() => {}
                Ok(actions) => {
                    pulled |= actions.contains(&CredentialSyncAction::Pulled);
                    outcomes.insert(
                        target.session_id.clone(),
                        CredentialSyncOutcome {
                            session_id: target.session_id.clone(),
                            outcome: Ok(actions),
                        },
                    );
                }
                Err(error) => {
                    outcomes.insert(
                        target.session_id.clone(),
                        CredentialSyncOutcome {
                            session_id: target.session_id.clone(),
                            outcome: Err(format!("{error:#}")),
                        },
                    );
                }
            }
        }
        if !pulled || pass == 1 {
            break;
        }
    }
    outcomes.into_values().collect()
}

/// Returns every action taken; an empty list means the copies already agree.
async fn reconcile_session(target: &CredentialSyncTarget) -> Result<Vec<CredentialSyncAction>> {
    let canonical_path = harness_authentication_marker(target.harness, &target.profile_home);
    let (canonical, canonical_bytes) = read_credential_file(target.harness, &canonical_path)?;
    let canonical_skills = crate::hel_skills::collect_skills(target.harness, &target.profile_home)
        .with_context(|| {
            format!(
                "collect canonical skills for profile {} from {}",
                target.profile_id,
                target.profile_home.display()
            )
        })?;
    let mut client = RelayClient::connect(&target.spec, &target.session_id).await?;
    let result = reconcile_connected(
        &mut client,
        target,
        &canonical_path,
        &canonical,
        &canonical_bytes,
        &canonical_skills,
    )
    .await;
    // Detach even when the exchange failed; the worker and harness keep
    // running either way. A failed detach only leaks a short-lived proxy, so it
    // is reported rather than turned into a sync failure.
    if let Err(error) = client.detach().await {
        tracing::warn!(
            session_id = %target.session_id,
            "could not close the credential sync connection: {error:#}"
        );
    }
    result
}

async fn reconcile_connected(
    client: &mut RelayClient,
    target: &CredentialSyncTarget,
    canonical_path: &Path,
    canonical: &CredentialSnapshot,
    canonical_bytes: &[u8],
    canonical_skills: &crate::hel_skills::SkillsArchive,
) -> Result<Vec<CredentialSyncAction>> {
    let mut actions = Vec::new();
    let session = client.credential_state().await?;
    match reconcile(canonical, &session) {
        SyncAction::None => {
            if canonical.present
                && session.present
                && canonical.fingerprint != session.fingerprint
                && canonical.freshness_epoch_ms.is_none()
                && session.freshness_epoch_ms.is_none()
            {
                tracing::warn!(
                    session_id = %target.session_id,
                    profile_id = %target.profile_id,
                    "credential copies differ but neither reports a refresh time; leaving both alone"
                );
            }
        }
        SyncAction::Push => {
            client.install_credentials(canonical_bytes).await?;
            actions.push(CredentialSyncAction::Pushed);
        }
        SyncAction::Pull => {
            let bytes = client.read_credentials().await?;
            validate_credential_payload(&bytes).with_context(|| {
                format!(
                    "session {} returned an unusable credential file",
                    target.session_id
                )
            })?;
            write_credential_file(canonical_path, &bytes).with_context(|| {
                format!(
                    "install fresher credentials from session {} for profile {}",
                    target.session_id, target.profile_id
                )
            })?;
            actions.push(CredentialSyncAction::Pulled);
        }
    }
    if reconcile_skills(client, target, canonical_skills).await? {
        actions.push(CredentialSyncAction::SkillsPushed);
    }
    Ok(actions)
}

/// Converge the session's synced skills trees onto the canonical archive.
/// Returns true when a push happened. Workers old enough to predate skills
/// sync answer the unknown method with `InvalidRequest`; those sessions are
/// skipped quietly until their target is re-provisioned.
async fn reconcile_skills(
    client: &mut RelayClient,
    target: &CredentialSyncTarget,
    canonical: &crate::hel_skills::SkillsArchive,
) -> Result<bool> {
    let canonical_state = canonical.state();
    let session = match client.skills_state().await {
        Ok(state) => state,
        Err(error) if skills_sync_unsupported(&error) => {
            tracing::debug!(
                session_id = %target.session_id,
                profile_id = %target.profile_id,
                "worker predates skills sync; skipping until the target is re-provisioned"
            );
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    if session == canonical_state {
        return Ok(false);
    }
    let installed = client.install_skills(&canonical.encode()).await?;
    if installed != canonical_state {
        bail!(
            "session {} skills fingerprint {} does not match the canonical {} after install",
            target.session_id,
            installed.fingerprint,
            canonical_state.fingerprint
        );
    }
    Ok(true)
}

fn skills_sync_unsupported(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<crate::hel_worker_client::RelayRejected>()
        .is_some_and(|rejected| {
            rejected.0.code == crate::hel_worker::RelayErrorCode::InvalidRequest
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claude_credentials(expires_at: i64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "access",
                "refreshToken": "refresh",
                "expiresAt": expires_at,
                "refreshTokenExpiresAt": expires_at + 1_000,
                "scopes": ["user:inference"],
                "subscriptionType": "max",
                "rateLimitTier": "default",
            }
        }))
        .unwrap()
    }

    fn codex_credentials(last_refresh: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "access_token": "access",
                "refresh_token": "refresh",
                "id_token": "id",
                "account_id": "account",
            },
            "last_refresh": last_refresh,
        }))
        .unwrap()
    }

    fn kimi_credentials(expires_at: i64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "access_token": "access",
            "refresh_token": "refresh",
            "expires_at": expires_at,
            "expires_in": 900,
            "scope": "all",
            "token_type": "Bearer",
        }))
        .unwrap()
    }

    fn grok_credentials(expiries: &[&str]) -> Vec<u8> {
        let grants = expiries
            .iter()
            .enumerate()
            .map(|(index, expires_at)| {
                (
                    format!("https://auth.x.ai::grant-{index}"),
                    serde_json::json!({
                        "key": "access",
                        "auth_mode": "oidc",
                        "refresh_token": "refresh",
                        "expires_at": expires_at,
                        "oidc_issuer": "https://auth.x.ai",
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        serde_json::to_vec(&serde_json::Value::Object(grants)).unwrap()
    }

    fn snapshot(fingerprint: &str, freshness: Option<i64>) -> CredentialSnapshot {
        CredentialSnapshot {
            present: true,
            fingerprint: fingerprint.to_owned(),
            freshness_epoch_ms: freshness,
        }
    }

    #[test]
    fn claude_freshness_reads_oauth_expiry_milliseconds() {
        assert_eq!(
            credential_freshness(HarnessKind::Claude, &claude_credentials(1_755_000_000_000)),
            Some(1_755_000_000_000)
        );
    }

    #[test]
    fn codex_freshness_converts_last_refresh_to_milliseconds() {
        assert_eq!(
            credential_freshness(
                HarnessKind::Codex,
                &codex_credentials("2026-08-05T02:51:00.864587231Z")
            ),
            Some(1_785_898_260_864)
        );
    }

    #[test]
    fn kimi_freshness_converts_expiry_seconds_to_milliseconds() {
        assert_eq!(
            credential_freshness(HarnessKind::Kimi, &kimi_credentials(1_755_000_000)),
            Some(1_755_000_000_000)
        );
    }

    #[test]
    fn grok_freshness_reads_the_latest_rfc3339_grant_expiry() {
        assert_eq!(
            credential_freshness(
                HarnessKind::Grok,
                &grok_credentials(&["2026-08-17T02:19:01.724226598Z"])
            ),
            Some(1_786_933_141_724)
        );
        // A home may hold several grants; the newest expiry decides freshness.
        assert_eq!(
            credential_freshness(
                HarnessKind::Grok,
                &grok_credentials(&[
                    "2026-08-17T02:19:01.724226598Z",
                    "2026-08-17T04:19:01.724226598Z",
                ])
            ),
            Some(1_786_940_341_724)
        );
        // Non-UTC offsets normalize to the same instant.
        assert_eq!(
            credential_freshness(
                HarnessKind::Grok,
                &grok_credentials(&["2026-08-16T22:19:01.724226598-04:00"])
            ),
            Some(1_786_933_141_724)
        );
    }

    #[test]
    fn every_harness_reports_freshness_from_its_own_credential_shape() {
        let fixtures = [
            (HarnessKind::Claude, claude_credentials(1_755_000_000_000)),
            (
                HarnessKind::Codex,
                codex_credentials("2026-08-05T02:51:00.864587231Z"),
            ),
            (HarnessKind::Kimi, kimi_credentials(1_755_000_000)),
            (
                HarnessKind::Grok,
                grok_credentials(&["2026-08-17T02:19:01.724226598Z"]),
            ),
        ];
        for kind in HarnessKind::ALL {
            let (_, bytes) = fixtures
                .iter()
                .find(|(fixture, _)| *fixture == kind)
                .unwrap_or_else(|| panic!("{kind:?} needs a credential fixture"));
            assert!(
                credential_freshness(kind, bytes).is_some(),
                "{kind:?} freshness"
            );
        }
    }

    #[test]
    fn unparseable_credentials_report_no_freshness() {
        for kind in HarnessKind::ALL {
            assert_eq!(credential_freshness(kind, b"not json"), None);
            assert_eq!(credential_freshness(kind, b"{}"), None);
        }
        assert_eq!(
            credential_freshness(HarnessKind::Codex, &codex_credentials("yesterday")),
            None
        );
    }

    #[test]
    fn payload_validation_rejects_empty_oversized_and_non_json() {
        assert!(validate_credential_payload(b"").is_err());
        assert!(validate_credential_payload(b"not json").is_err());
        assert!(validate_credential_payload(&vec![b'a'; MAX_CREDENTIAL_BYTES + 1]).is_err());
        assert!(validate_credential_payload(&claude_credentials(1)).is_ok());
    }

    #[test]
    fn identical_or_absent_copies_need_no_sync() {
        assert_eq!(
            reconcile(&CredentialSnapshot::absent(), &CredentialSnapshot::absent()),
            SyncAction::None
        );
        assert_eq!(
            reconcile(&snapshot("a", Some(2)), &snapshot("a", Some(1))),
            SyncAction::None
        );
    }

    #[test]
    fn a_missing_side_takes_the_other_side_copy() {
        assert_eq!(
            reconcile(&snapshot("a", Some(1)), &CredentialSnapshot::absent()),
            SyncAction::Push
        );
        assert_eq!(
            reconcile(&CredentialSnapshot::absent(), &snapshot("b", Some(1))),
            SyncAction::Pull
        );
    }

    #[test]
    fn the_fresher_copy_wins_and_a_known_time_beats_an_unknown_one() {
        assert_eq!(
            reconcile(&snapshot("a", Some(2)), &snapshot("b", Some(1))),
            SyncAction::Push
        );
        assert_eq!(
            reconcile(&snapshot("a", Some(1)), &snapshot("b", Some(2))),
            SyncAction::Pull
        );
        assert_eq!(
            reconcile(&snapshot("a", Some(1)), &snapshot("b", None)),
            SyncAction::Push
        );
        assert_eq!(
            reconcile(&snapshot("a", None), &snapshot("b", Some(1))),
            SyncAction::Pull
        );
    }

    #[test]
    fn differing_copies_without_any_freshness_are_left_alone() {
        assert_eq!(
            reconcile(&snapshot("a", None), &snapshot("b", None)),
            SyncAction::None
        );
        assert_eq!(
            reconcile(&snapshot("a", Some(5)), &snapshot("b", Some(5))),
            SyncAction::None
        );
    }

    #[test]
    fn canonical_write_is_owner_only_and_replaces_the_previous_file() {
        let home = tempfile::tempdir().unwrap();
        let path = harness_authentication_marker(HarnessKind::Kimi, home.path());
        write_credential_file(&path, &kimi_credentials(1)).unwrap();
        write_credential_file(&path, &kimi_credentials(2)).unwrap();
        let (snapshot, bytes) = read_credential_file(HarnessKind::Kimi, &path).unwrap();
        assert!(snapshot.present);
        assert_eq!(bytes, kimi_credentials(2));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[cfg(unix)]
    #[test]
    fn canonical_write_refuses_a_symlinked_destination() {
        let home = tempfile::tempdir().unwrap();
        let elsewhere = home.path().join("elsewhere.json");
        std::fs::write(&elsewhere, b"{}").unwrap();
        let path = home.path().join("auth.json");
        std::os::unix::fs::symlink(&elsewhere, &path).unwrap();
        let error =
            write_credential_file(&path, &codex_credentials("2026-01-01T00:00:00Z")).unwrap_err();
        assert!(error.to_string().contains("symbolic link"));
        assert_eq!(std::fs::read(&elsewhere).unwrap(), b"{}");
    }

    #[test]
    fn a_missing_credential_file_reads_as_an_absent_snapshot() {
        let home = tempfile::tempdir().unwrap();
        let path = harness_authentication_marker(HarnessKind::Codex, home.path());
        let (snapshot, bytes) = read_credential_file(HarnessKind::Codex, &path).unwrap();
        assert!(!snapshot.present);
        assert!(bytes.is_empty());
    }

    #[test]
    fn auth_failure_phrases_match_and_near_misses_do_not() {
        assert!(auth_failure_signature(
            HarnessKind::Claude,
            "Error: OAuth session expired and could not be refreshed"
        ));
        assert!(auth_failure_signature(
            HarnessKind::Codex,
            "{\"error\":{\"type\":\"authentication_error\"}}"
        ));
        assert!(auth_failure_signature(
            HarnessKind::Kimi,
            "invalid_grant: refresh token rejected"
        ));
        assert!(!auth_failure_signature(
            HarnessKind::Claude,
            "the OAuth session expired last week, but we refreshed it"
        ));
        assert!(!auth_failure_signature(
            HarnessKind::Claude,
            "authentication succeeded"
        ));
    }

    #[test]
    fn only_relay_warnings_and_session_updates_report_auth_failures() {
        use agent_client_protocol::schema::v1::{ContentBlock, ContentChunk, SessionUpdate};

        let event = |observation| RelayEvent {
            ordinal: 1,
            previous_digest: crate::hel_worker::RELAY_EVENT_GENESIS_DIGEST.into(),
            digest: "a".repeat(64),
            recorded_at_ms: 1,
            command_id: None,
            observation,
        };
        assert!(events_report_auth_failure(
            HarnessKind::Claude,
            &[event(RelayObservation::Warning {
                message: "OAuth session expired and could not be refreshed".into(),
            })]
        ));
        assert!(events_report_auth_failure(
            HarnessKind::Claude,
            &[event(RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AgentMessageChunk(ContentChunk::new(
                    ContentBlock::from("Please run /login to continue"),
                ))),
            })]
        ));
        assert!(!events_report_auth_failure(
            HarnessKind::Claude,
            &[event(RelayObservation::CommandInterrupted {
                command_id: "command-1".into(),
                command: crate::hel_worker::RelayCommandKind::Prompt,
                message: "invalid_grant".into(),
            })]
        ));
        assert!(!events_report_auth_failure(
            HarnessKind::Claude,
            &[event(RelayObservation::CommandQueued {
                command_id: "command-1".into(),
                command: crate::hel_worker::RelayCommand::Prompt {
                    prompt: vec![ContentBlock::from("explain invalid_grant")],
                },
                created_at_ms: 1,
            })]
        ));
    }

    #[test]
    fn login_commands_match_each_harness_cli() {
        let profile = |kind: HarnessKind, executable: Option<&str>| HarnessProfile {
            kind,
            home: PathBuf::from("/home/user/.config"),
            executable: executable.map(PathBuf::from),
            environment: Default::default(),
            context_window_bytes: None,
        };
        assert_eq!(
            login_command(&profile(HarnessKind::Codex, None)),
            ("codex".to_owned(), vec!["login".to_owned()])
        );
        assert_eq!(
            login_command(&profile(HarnessKind::Claude, None)),
            (
                "claude".to_owned(),
                vec!["auth".to_owned(), "login".to_owned()]
            )
        );
        assert_eq!(
            login_command(&profile(HarnessKind::Kimi, None)),
            ("kimi".to_owned(), vec!["login".to_owned()])
        );
        assert_eq!(
            login_command(&profile(HarnessKind::Grok, None)),
            ("grok".to_owned(), vec!["login".to_owned()])
        );
    }

    #[test]
    fn only_a_cli_bridge_executable_override_names_the_harness_cli() {
        let profile = |kind: HarnessKind| HarnessProfile {
            kind,
            home: PathBuf::from("/home/user/.config"),
            executable: Some(PathBuf::from("/opt/bin/custom")),
            environment: Default::default(),
            context_window_bytes: None,
        };
        assert_eq!(
            login_command(&profile(HarnessKind::Kimi)),
            ("/opt/bin/custom".to_owned(), vec!["login".to_owned()])
        );
        assert_eq!(
            login_command(&profile(HarnessKind::Grok)),
            ("/opt/bin/custom".to_owned(), vec!["login".to_owned()])
        );
        assert_eq!(
            login_command(&profile(HarnessKind::Codex)).0,
            "codex".to_owned()
        );
        assert_eq!(
            login_command(&profile(HarnessKind::Claude)).0,
            "claude".to_owned()
        );
    }

    #[test]
    fn a_queued_periodic_sync_is_not_queued_twice() {
        let mut queue = VecDeque::new();
        enqueue(
            &mut queue,
            SyncTrigger {
                profile_id: "work".into(),
                session_id: None,
            },
        );
        enqueue(
            &mut queue,
            SyncTrigger {
                profile_id: "work".into(),
                session_id: None,
            },
        );
        enqueue(
            &mut queue,
            SyncTrigger {
                profile_id: "work".into(),
                session_id: Some("session".into()),
            },
        );
        assert_eq!(queue.len(), 2);
        assert_eq!(queue[1].session_id.as_deref(), Some("session"));
    }
}
