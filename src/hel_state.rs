//! Durable controller-side state for Hel-managed sessions.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::hel_config::{HarnessKind, HelConfig, atomic_write, data_dir, validate_id};
use crate::hel_targets::{AdditionalMount, validate_additional_mounts};
use crate::hel_worker::{SequencedEvent, WorkerEvent};

pub const STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionState {
    Provisioning,
    Running,
    Disconnected,
    Checkpointing,
    Closing,
    Archived,
    Lost,
    Error,
    DestroyedWithDataLoss,
}

impl SessionState {
    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Provisioning
                | Self::Running
                | Self::Disconnected
                | Self::Checkpointing
                | Self::Closing
                | Self::Error
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum TargetLocator {
    LocalBare {
        worker_root: PathBuf,
    },
    LocalPodman {
        container_id: String,
    },
    AppleContainer {
        container_id: String,
    },
    AwsEc2 {
        instance_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        address: Option<String>,
    },
    SshBare {
        host: String,
        workspace: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        worker_id: Option<String>,
    },
    SshPodman {
        host: String,
        container_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum SessionResourceAllocation {
    Container {
        cpus: u64,
        memory_bytes: u64,
    },
    AwsEc2 {
        instance_type: String,
        vcpus: u64,
        memory_bytes: u64,
    },
}

impl SessionResourceAllocation {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Container { cpus, memory_bytes } if *cpus == 0 || *memory_bytes == 0 => {
                bail!("container resource allocation must have non-zero CPU and memory")
            }
            Self::AwsEc2 {
                instance_type,
                vcpus,
                memory_bytes,
            } if instance_type.trim().is_empty() || *vcpus == 0 || *memory_bytes == 0 => {
                bail!("EC2 resource allocation must have an instance type, CPU, and memory")
            }
            _ => Ok(()),
        }
    }
}

impl TargetLocator {
    fn validate(&self, session_id: &str) -> Result<()> {
        match self {
            Self::LocalBare { worker_root } => {
                if !worker_root.is_absolute()
                    || worker_root
                        .components()
                        .any(|part| part == Component::ParentDir)
                    || !worker_root.ends_with(session_id)
                {
                    bail!(
                        "local bare worker root must be an absolute safe path ending in the session id"
                    );
                }
            }
            Self::LocalPodman { container_id }
            | Self::AppleContainer { container_id }
            | Self::SshPodman { container_id, .. }
                if container_id.trim().is_empty() =>
            {
                bail!("target locator has an empty container id")
            }
            Self::AwsEc2 { instance_id, .. } if instance_id.trim().is_empty() => {
                bail!("target locator has an empty AWS instance id")
            }
            Self::SshBare {
                host, workspace, ..
            } => {
                if host.trim().is_empty() {
                    bail!("bare SSH target locator has an empty host");
                }
                if workspace.as_os_str().is_empty()
                    || workspace
                        .components()
                        .any(|part| part == Component::ParentDir)
                    || !workspace.ends_with(session_id)
                {
                    bail!("bare SSH target locator must be a safe path ending in the session id");
                }
            }
            Self::SshPodman { host, .. } if host.trim().is_empty() => {
                bail!("SSH Podman target locator has an empty host")
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointMetadata {
    pub archive_path: PathBuf,
    /// Lowercase SHA-256 digest of the verified archive.
    pub sha256: String,
    pub created_at: String,
    #[serde(default)]
    pub event_sequence: u64,
}

impl CheckpointMetadata {
    fn validate(&self) -> Result<()> {
        if self.archive_path.as_os_str().is_empty() {
            bail!("checkpoint archive path is empty");
        }
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            bail!("checkpoint SHA-256 must be 64 lowercase hexadecimal characters");
        }
        if self.created_at.trim().is_empty() {
            bail!("checkpoint timestamp is empty");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionRecord {
    pub id: String,
    pub title: String,
    pub harness_kind: HarnessKind,
    pub last_profile: String,
    pub bundle_id: String,
    /// Existing project directory used directly by a local or SSH bare target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_directory: Option<PathBuf>,
    pub target_template_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_allocation: Option<SessionResourceAllocation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_mounts: Vec<AdditionalMount>,
    pub state: SessionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<TargetLocator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acp_session_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_title_override: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub last_viewed_event_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointMetadata>,
}

impl SessionRecord {
    /// User-visible session name, independent of the initial prompt stored in `title`.
    pub fn display_title(&self) -> &str {
        self.session_title_override
            .as_deref()
            .or(self.acp_session_title.as_deref())
            .unwrap_or(&self.id)
    }

    fn validate(&self, map_id: &str) -> Result<()> {
        validate_id("session", &self.id)?;
        if self.id != map_id {
            bail!(
                "session map key {map_id:?} does not match record id {:?}",
                self.id
            );
        }
        validate_id("profile", &self.last_profile)?;
        validate_id("bundle", &self.bundle_id)?;
        if let Some(project_directory) = &self.project_directory
            && (!project_directory.is_absolute()
                || project_directory
                    .components()
                    .any(|part| part == Component::ParentDir))
        {
            bail!("session {:?} has an unsafe project directory", self.id);
        }
        validate_id("target template", &self.target_template_id)?;
        if let Some(allocation) = &self.resource_allocation {
            allocation.validate()?;
        }
        validate_additional_mounts(&self.additional_mounts)?;
        if self.title.trim().is_empty() {
            bail!("session {:?} has an empty title", self.id);
        }
        if self
            .acp_session_title
            .as_ref()
            .is_some_and(|title| title.trim().is_empty())
            || self
                .session_title_override
                .as_ref()
                .is_some_and(|title| title.trim().is_empty())
        {
            bail!("session {:?} has an empty display title", self.id);
        }
        if self.created_at.trim().is_empty() || self.updated_at.trim().is_empty() {
            bail!("session {:?} has an empty timestamp", self.id);
        }
        if let Some(target) = &self.target {
            target.validate(&self.id)?;
        }
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelState {
    pub version: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sessions: BTreeMap<String, SessionRecord>,
    /// Recently used source directories, keyed by `local` or SSH host name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub mount_history: BTreeMap<String, Vec<PathBuf>>,
}

impl Default for HelState {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            sessions: BTreeMap::new(),
            mount_history: BTreeMap::new(),
        }
    }
}

impl HelState {
    pub fn validate(&self) -> Result<()> {
        if self.version != STATE_VERSION {
            bail!(
                "unsupported Hel state version {}; expected {STATE_VERSION}",
                self.version
            );
        }
        for (id, session) in &self.sessions {
            session.validate(id)?;
        }
        for (host, sources) in &self.mount_history {
            if host.trim().is_empty() {
                bail!("mount history contains an empty host key");
            }
            if sources.iter().any(|source| !source.is_absolute()) {
                bail!("mount history for {host:?} contains a non-absolute source path");
            }
        }
        Ok(())
    }

    pub fn remember_mount_sources(&mut self, host: &str, mounts: &[AdditionalMount]) {
        if mounts.is_empty() {
            return;
        }
        let sources = self.mount_history.entry(host.to_owned()).or_default();
        for mount in mounts.iter().rev() {
            sources.retain(|source| source != &mount.source);
            sources.insert(0, mount.source.clone());
        }
        sources.truncate(20);
    }

    pub fn project_directories(&self, host: &str) -> &[PathBuf] {
        self.mount_history
            .get(&project_history_key(host))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub fn remember_project_directory(&mut self, host: &str, directory: &Path) {
        let key = project_history_key(host);
        let directories = self.mount_history.entry(key).or_default();
        directories.retain(|existing| existing != directory);
        directories.insert(0, directory.to_path_buf());
        directories.truncate(20);
    }

    pub fn remove_archived_session(&mut self, session_id: &str) -> Result<SessionRecord> {
        let session = self
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        if session.state.is_active() {
            bail!("refusing to delete active session {session_id}");
        }
        Ok(self
            .sessions
            .remove(session_id)
            .expect("session checked above"))
    }

    /// Validate persisted foreign keys without preventing config entries from
    /// being renamed after a session is fully archived.
    pub fn validate_against_config(&self, config: &HelConfig) -> Result<()> {
        self.validate()?;
        config.validate()?;
        for session in self
            .sessions
            .values()
            .filter(|session| session.state.is_active())
        {
            let profile = config.profiles.get(&session.last_profile).ok_or_else(|| {
                anyhow::anyhow!(
                    "active session {:?} references missing profile {:?}",
                    session.id,
                    session.last_profile
                )
            })?;
            if profile.kind != session.harness_kind {
                bail!(
                    "active session {:?} expects {:?}, but profile {:?} is {:?}",
                    session.id,
                    session.harness_kind,
                    session.last_profile,
                    profile.kind
                );
            }
            if session.project_directory.is_none()
                && !config.bundles.contains_key(&session.bundle_id)
            {
                bail!(
                    "active session {:?} references missing bundle {:?}",
                    session.id,
                    session.bundle_id
                );
            }
            if !config.targets.contains_key(&session.target_template_id) {
                bail!(
                    "active session {:?} references missing target template {:?}",
                    session.id,
                    session.target_template_id
                );
            }
        }
        Ok(())
    }

    pub fn load() -> Result<Self> {
        crate::hel_database::migrate_legacy_state()?;
        crate::hel_database::load_state()
    }

    pub fn load_from(path: &Path) -> Result<Self> {
        Self::load_json_from(path)
    }

    pub(crate) fn load_json_from(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let body = fs::read(path).with_context(|| format!("read Hel state {}", path.display()))?;
        let state: Self = serde_json::from_slice(&body)
            .with_context(|| format!("parse Hel state {}", path.display()))?;
        state.validate()?;
        Ok(state)
    }

    pub fn save(&self) -> Result<()> {
        crate::hel_database::save_state(self)
    }

    pub fn save_to(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let body = serde_json::to_vec_pretty(self).context("serialize Hel state")?;
        atomic_write(path, &body)
    }
}

fn project_history_key(host: &str) -> String {
    format!("project:{host}")
}

pub fn state_path() -> PathBuf {
    data_dir().join("state.json")
}

/// Generate an opaque, filesystem-safe stable id for a new logical session.
pub fn new_session_id() -> Result<String> {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random)
        .map_err(|error| anyhow::anyhow!("generate Hel session id: {error}"))?;
    let mut encoded = String::with_capacity(32);
    for byte in random {
        use std::fmt::Write as _;
        write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(encoded)
}

/// Return the newest clean ACP session title from canonical worker events.
pub fn harness_session_title(events: &[SequencedEvent]) -> Option<String> {
    events.iter().rev().find_map(|event| {
        let WorkerEvent::Adapter { payload, .. } = &event.event else {
            return None;
        };
        let crate::hel_acp::RuntimeEvent::SessionUpdate { update } =
            serde_json::from_value(payload.clone()).ok()?
        else {
            return None;
        };
        let kind = update
            .get("sessionUpdate")
            .and_then(serde_json::Value::as_str)?;
        let title = match kind {
            "session_info_update" | "session_title" => {
                update.get("title").and_then(serde_json::Value::as_str)
            }
            _ => None,
        }?;
        normalize_session_title(title)
    })
}

pub(crate) fn normalize_session_title(title: &str) -> Option<String> {
    let normalized = title.split_whitespace().collect::<Vec<_>>().join(" ");
    (!normalized.is_empty()).then_some(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_config::{
        CONFIG_VERSION, ContainerTemplate, HarnessProfile, ProjectBundle, ProjectRepository,
        TargetTemplate,
    };

    fn sample_state() -> HelState {
        let session = SessionRecord {
            id: "0123456789abcdef".into(),
            title: "Build Hel".into(),
            harness_kind: HarnessKind::Codex,
            last_profile: "codex-1".into(),
            bundle_id: "hel".into(),
            project_directory: None,
            target_template_id: "podman".into(),
            resource_allocation: None,
            additional_mounts: vec![AdditionalMount {
                source: PathBuf::from("/home/test/cache"),
                destination: PathBuf::from("/mnt/cache"),
            }],
            state: SessionState::Running,
            target: Some(TargetLocator::LocalPodman {
                container_id: "afb67d".into(),
            }),
            native_session_id: Some("native-1".into()),
            acp_session_title: Some("Build Hel".into()),
            session_title_override: None,
            created_at: "2026-08-09T12:00:00Z".into(),
            updated_at: "2026-08-09T12:01:00Z".into(),
            last_viewed_event_sequence: 0,
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: Some(CheckpointMetadata {
                archive_path: PathBuf::from("sessions/0123456789abcdef.hel.zip"),
                sha256: "a".repeat(64),
                created_at: "2026-08-09T12:01:00Z".into(),
                event_sequence: 42,
            }),
        };
        HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([(session.id.clone(), session)]),
            mount_history: BTreeMap::from([(
                "local".into(),
                vec![PathBuf::from("/home/test/cache")],
            )]),
        }
    }

    fn sample_config() -> HelConfig {
        HelConfig {
            version: CONFIG_VERSION,
            profiles: BTreeMap::from([(
                "codex-1".into(),
                HarnessProfile {
                    context_window_bytes: None,
                    kind: HarnessKind::Codex,
                    home: PathBuf::from("/home/test/.codex"),
                    executable: None,
                    environment: BTreeMap::new(),
                },
            )]),
            bundles: BTreeMap::from([(
                "hel".into(),
                ProjectBundle {
                    primary_repo: "hel".into(),
                    repositories: vec![ProjectRepository {
                        id: "hel".into(),
                        github: Some("BrokkAi/hel".into()),
                        local: None,
                        destination: PathBuf::from("hel"),
                        git_ref: None,
                    }],
                },
            )]),
            targets: BTreeMap::from([(
                "podman".into(),
                TargetTemplate::LocalPodman {
                    container: ContainerTemplate {
                        image: "ubuntu:24.04".into(),
                        platform: None,
                        cpus: None,
                        memory: None,
                        environment: BTreeMap::new(),
                    },
                },
            )]),
        }
    }

    #[test]
    fn json_state_round_trip_is_atomic() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/state.json");
        let state = sample_state();
        state.save_to(&path).unwrap();
        assert_eq!(HelState::load_from(&path).unwrap(), state);
        assert!(
            fs::read_dir(directory.path().join("nested"))
                .unwrap()
                .all(|entry| {
                    !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".tmp")
                })
        );
    }

    #[test]
    fn mount_history_keeps_unique_recent_sources_per_host() {
        let mut state = HelState::default();
        state.remember_mount_sources(
            "builder.example.test",
            &[
                AdditionalMount {
                    source: "/srv/first".into(),
                    destination: "/mnt/first".into(),
                },
                AdditionalMount {
                    source: "/srv/second".into(),
                    destination: "/mnt/second".into(),
                },
            ],
        );
        state.remember_mount_sources(
            "builder.example.test",
            &[AdditionalMount {
                source: "/srv/first".into(),
                destination: "/mnt/again".into(),
            }],
        );

        assert_eq!(
            state.mount_history["builder.example.test"],
            vec![PathBuf::from("/srv/first"), PathBuf::from("/srv/second")]
        );
    }

    #[test]
    fn project_directory_history_is_recent_and_isolated_per_remote_host() {
        let mut state = HelState::default();
        state.remember_project_directory("builder-a", Path::new("/srv/one"));
        state.remember_project_directory("builder-a", Path::new("/srv/two"));
        state.remember_project_directory("builder-a", Path::new("/srv/one"));
        state.remember_project_directory("builder-b", Path::new("/work/other"));

        assert_eq!(
            state.project_directories("builder-a"),
            [PathBuf::from("/srv/one"), PathBuf::from("/srv/two")]
        );
        assert_eq!(
            state.project_directories("builder-b"),
            [PathBuf::from("/work/other")]
        );
    }

    #[test]
    fn active_state_validates_references_and_harness_kind() {
        let state = sample_state();
        state.validate_against_config(&sample_config()).unwrap();

        let mut config = sample_config();
        config.profiles.get_mut("codex-1").unwrap().kind = HarnessKind::Claude;
        assert!(
            state
                .validate_against_config(&config)
                .unwrap_err()
                .to_string()
                .contains("expects Codex")
        );
    }

    #[test]
    fn archived_session_does_not_pin_renamed_config_entries() {
        let mut state = sample_state();
        state.sessions.values_mut().next().unwrap().state = SessionState::Archived;
        state
            .validate_against_config(&HelConfig::default())
            .unwrap();
    }

    #[test]
    fn only_inactive_sessions_can_be_removed_from_the_archive() {
        let mut state = sample_state();
        assert!(
            state
                .remove_archived_session("0123456789abcdef")
                .unwrap_err()
                .to_string()
                .contains("active session")
        );
        assert!(state.sessions.contains_key("0123456789abcdef"));

        state.sessions.values_mut().next().unwrap().state = SessionState::Archived;
        let removed = state.remove_archived_session("0123456789abcdef").unwrap();
        assert_eq!(removed.id, "0123456789abcdef");
        assert!(state.sessions.is_empty());
    }

    #[test]
    fn harness_title_prefers_the_newest_session_info_update() {
        let events = vec![
            SequencedEvent {
                seq: 1,
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::json!({
                        "type": "session_update",
                        "update": {
                            "sessionUpdate": "session_info_update",
                            "title": "First title"
                        }
                    }),
                },
            },
            SequencedEvent {
                seq: 2,
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::json!({
                        "type": "session_update",
                        "update": {
                            "sessionUpdate": "session_summary",
                            "summary": "  Build   the dashboard  "
                        }
                    }),
                },
            },
        ];

        assert_eq!(
            harness_session_title(&events).as_deref(),
            Some("First title")
        );
    }

    #[test]
    fn extension_session_title_is_cleaned_without_losing_available_text() {
        let first_prompt = format!("{}overflow", "word ".repeat(20));
        let expected = first_prompt.trim().to_string();
        let events = vec![
            SequencedEvent {
                seq: 1,
                request_id: Some("prompt-1".into()),
                event: WorkerEvent::PromptAccepted {
                    request_id: "prompt-1".into(),
                    text: format!("  {first_prompt}\n"),
                    attachments: vec![],
                },
            },
            SequencedEvent {
                seq: 2,
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::json!({
                        "type": "session_update",
                        "update": {
                            "sessionUpdate": "session_title",
                            "title": first_prompt
                        }
                    }),
                },
            },
        ];

        assert_eq!(
            harness_session_title(&events).as_deref(),
            Some(expected.as_str())
        );
    }

    #[test]
    fn first_prompt_is_not_used_as_an_acp_session_title() {
        let events = vec![SequencedEvent {
            seq: 1,
            request_id: Some("prompt-1".into()),
            event: WorkerEvent::PromptAccepted {
                request_id: "prompt-1".into(),
                text: "Do not use me as a title".into(),
                attachments: vec![],
            },
        }];

        assert_eq!(harness_session_title(&events), None);
    }

    #[test]
    fn harness_titles_are_normalized_to_one_complete_line() {
        let events = vec![SequencedEvent {
            seq: 1,
            request_id: None,
            event: WorkerEvent::Adapter {
                kind: "session_update".into(),
                payload: serde_json::json!({
                    "type": "session_update",
                    "update": {
                        "sessionUpdate": "session_title",
                        "title": "first\nsecond\tthird fourth fifth sixth seventh eighth ninth tenth eleventh twelfth thirteenth"
                    }
                }),
            },
        }];

        assert_eq!(
            harness_session_title(&events).as_deref(),
            Some(
                "first second third fourth fifth sixth seventh eighth ninth tenth eleventh twelfth thirteenth"
            )
        );
    }

    #[test]
    fn locator_rejects_parent_traversal() {
        let mut state = sample_state();
        state.sessions.values_mut().next().unwrap().target = Some(TargetLocator::SshBare {
            host: "builder".into(),
            workspace: PathBuf::from("~/hel/../other"),
            worker_id: None,
        });
        assert!(
            state
                .validate()
                .unwrap_err()
                .to_string()
                .contains("safe path ending")
        );
    }

    #[test]
    fn generated_session_ids_are_valid_and_distinct() {
        let first = new_session_id().unwrap();
        let second = new_session_id().unwrap();
        validate_id("session", &first).unwrap();
        assert_eq!(first.len(), 32);
        assert_ne!(first, second);
    }
}
