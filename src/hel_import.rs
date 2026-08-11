//! Import native harness sessions into Hel's durable archive format.
//

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use rayon::prelude::*;
use serde_json::{Value, json};

use crate::hel_archive::{
    ArchiveInput, BundleManifest, GitCollectionSpec, SystemGit, TargetManifest,
    collect_git_snapshot, write_archive_atomic,
};
use crate::hel_checkpoint::{collect_import_native_artifacts, collect_native_artifacts};
use crate::hel_config::{HarnessKind, HelConfig, ProjectBundle, ProjectRepository, validate_id};
use crate::hel_setup::{GithubRepository, github_repository_from_origin};
use crate::hel_state::{
    CheckpointMetadata, HelState, SessionRecord, SessionState, harness_session_title,
    new_session_id,
};
use crate::hel_worker::{SequencedEvent, WorkerEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaudeSessionSelection {
    Session(String),
    Latest,
}

pub type CodexSessionSelection = ClaudeSessionSelection;
pub type KimiSessionSelection = ClaudeSessionSelection;

#[derive(Debug, Clone)]
pub struct LocatedClaudeSession {
    pub session_id: String,
    pub jsonl_path: PathBuf,
    pub modified_at: SystemTime,
}

pub type LocatedCodexSession = LocatedClaudeSession;

#[derive(Debug, Clone)]
pub struct LocatedKimiSession {
    pub session_id: String,
    pub session_path: PathBuf,
    pub modified_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeTranscript {
    pub cwd: PathBuf,
    pub events: Vec<SequencedEvent>,
}

pub type CodexTranscript = ClaudeTranscript;
pub type KimiTranscript = ClaudeTranscript;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BundleResolution {
    Existing(String),
    /// The caller must ask the user before adding this to their config.
    Synthesized {
        id: String,
        bundle: ProjectBundle,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedClaudeSession {
    pub session_id: String,
    pub native_session_id: String,
    pub source_jsonl: PathBuf,
    pub source_cwd: PathBuf,
    pub bundle_id: String,
    pub archive_path: PathBuf,
}

pub type ImportedCodexSession = ImportedClaudeSession;
pub type ImportedKimiSession = ImportedClaudeSession;

pub struct ClaudeImportRequest<'a> {
    pub claude_home: &'a Path,
    pub source: &'a LocatedClaudeSession,
    pub transcript: &'a ClaudeTranscript,
    pub bundle_id: &'a str,
    pub title: Option<&'a str>,
    pub archive_directory: &'a Path,
}

pub struct CodexImportRequest<'a> {
    pub codex_home: &'a Path,
    pub source: &'a LocatedCodexSession,
    pub transcript: &'a CodexTranscript,
    pub bundle_id: &'a str,
    pub title: Option<&'a str>,
    pub archive_directory: &'a Path,
}

pub struct KimiImportRequest<'a> {
    pub kimi_home: &'a Path,
    pub source: &'a LocatedKimiSession,
    pub transcript: &'a KimiTranscript,
    pub bundle_id: &'a str,
    pub title: Option<&'a str>,
    pub archive_directory: &'a Path,
}

/// Resolve the Claude configuration home without ever modifying it.
pub fn claude_config_home() -> Result<PathBuf> {
    let home = std::env::var_os(HarnessKind::Claude.home_env())
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".claude")))
        .context("cannot determine Claude config home; set CLAUDE_CONFIG_DIR")?;
    ensure!(
        home.is_dir(),
        "Claude config home is not a directory: {}",
        home.display()
    );
    Ok(home)
}

/// Resolve the Codex configuration home without ever modifying it.
pub fn codex_config_home() -> Result<PathBuf> {
    let home = std::env::var_os(HarnessKind::Codex.home_env())
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".codex")))
        .context("cannot determine Codex home; set CODEX_HOME")?;
    ensure!(
        home.is_dir(),
        "Codex home is not a directory: {}",
        home.display()
    );
    Ok(home)
}

/// Resolve the Kimi Code configuration home without ever modifying it.
pub fn kimi_config_home() -> Result<PathBuf> {
    let home = std::env::var_os(HarnessKind::Kimi.home_env())
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".kimi-code")))
        .context("cannot determine Kimi Code home; set KIMI_CODE_HOME")?;
    ensure!(
        home.is_dir(),
        "Kimi Code home is not a directory: {}",
        home.display()
    );
    Ok(home)
}

/// Locate a Codex rollout from its native session or archived-session trees.
pub fn locate_codex_session(
    home: &Path,
    selection: &CodexSessionSelection,
) -> Result<LocatedCodexSession> {
    let mut candidates = Vec::new();
    for root_name in ["sessions", "archived_sessions"] {
        let root = home.join(root_name);
        if root.is_dir() {
            collect_codex_candidates(&root, &mut candidates)?;
        }
    }
    select_jsonl_session(candidates, selection, "Codex")
}

fn collect_codex_candidates(root: &Path, candidates: &mut Vec<LocatedCodexSession>) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            collect_codex_candidates(&path, candidates)?;
            continue;
        }
        if !metadata.is_file() || path.extension().and_then(|value| value.to_str()) != Some("jsonl")
        {
            continue;
        }
        let Some(session_id) = codex_session_id(&path)? else {
            continue;
        };
        candidates.push(LocatedCodexSession {
            session_id,
            jsonl_path: path,
            modified_at: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        });
    }
    Ok(())
}
fn codex_session_id(path: &Path) -> Result<Option<String>> {
    let file =
        fs::File::open(path).with_context(|| format!("open Codex session {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    for _ in 0..8 {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let record: Value = serde_json::from_str(&line)
            .with_context(|| format!("parse Codex session {}", path.display()))?;
        if record.get("type").and_then(Value::as_str) != Some("session_meta") {
            continue;
        }
        // Codex ACP loads a rollout by its payload `id`, which is also the
        // UUID embedded in the rollout filename. `session_id` can name a
        // parent thread and therefore is not necessarily resumable itself.
        let id = record
            .pointer("/payload/id")
            .or_else(|| record.pointer("/payload/session_id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(ToOwned::to_owned);
        if let Some(id) = id {
            validate_id("Codex session", &id)?;
            return Ok(Some(id));
        }
    }
    Ok(None)
}

/// Locate a Kimi session directory. Its on-disk `session_<uuid>` name is the
/// native identifier required by Kimi ACP's `session/load`.
pub fn locate_kimi_session(
    home: &Path,
    selection: &KimiSessionSelection,
) -> Result<LocatedKimiSession> {
    let sessions = home.join("sessions");
    ensure!(
        sessions.is_dir(),
        "Kimi sessions directory is missing: {}",
        sessions.display()
    );
    let mut candidates = Vec::new();
    for workspace in fs::read_dir(&sessions)? {
        let workspace = workspace?;
        let workspace_path = workspace.path();
        let metadata = fs::symlink_metadata(&workspace_path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&workspace_path)? {
            let entry = entry?;
            let session_path = entry.path();
            let metadata = fs::symlink_metadata(&session_path)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                continue;
            }
            let Some(session_id) = session_path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| name.starts_with("session_") && name.len() > "session_".len())
            else {
                continue;
            };
            validate_id("Kimi session", session_id)?;
            candidates.push(LocatedKimiSession {
                session_id: session_id.to_owned(),
                session_path,
                modified_at: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            });
        }
    }
    match selection {
        KimiSessionSelection::Session(session_id) => candidates
            .into_iter()
            .find(|candidate| candidate.session_id == *session_id)
            .with_context(|| {
                format!(
                    "Kimi session {session_id:?} was not found under {}",
                    sessions.display()
                )
            }),
        KimiSessionSelection::Latest => candidates
            .into_iter()
            .max_by(|left, right| {
                left.modified_at
                    .cmp(&right.modified_at)
                    .then_with(|| left.session_path.cmp(&right.session_path))
            })
            .context("no Kimi session directories were found"),
    }
}

fn select_jsonl_session(
    candidates: Vec<LocatedCodexSession>,
    selection: &CodexSessionSelection,
    harness: &str,
) -> Result<LocatedCodexSession> {
    match selection {
        CodexSessionSelection::Session(session_id) => {
            validate_id(&format!("{harness} session"), session_id)?;
            candidates
                .into_iter()
                .filter(|candidate| candidate.session_id == *session_id)
                .max_by(|left, right| {
                    left.modified_at
                        .cmp(&right.modified_at)
                        .then_with(|| left.jsonl_path.cmp(&right.jsonl_path))
                })
                .with_context(|| format!("{harness} session {session_id:?} was not found"))
        }
        CodexSessionSelection::Latest => candidates
            .into_iter()
            .max_by(|left, right| {
                left.modified_at
                    .cmp(&right.modified_at)
                    .then_with(|| left.jsonl_path.cmp(&right.jsonl_path))
            })
            .context("no session JSONL files were found"),
    }
}

/// Locate one native Claude rollout. `Latest` compares modified time across
/// every immediate project directory, exactly as Claude's layout requires.
pub fn locate_claude_session(
    home: &Path,
    selection: &ClaudeSessionSelection,
) -> Result<LocatedClaudeSession> {
    let projects = home.join("projects");
    ensure!(
        projects.is_dir(),
        "Claude projects directory is missing: {}",
        projects.display()
    );
    let mut candidates = Vec::new();
    for project in fs::read_dir(&projects)
        .with_context(|| format!("read Claude projects directory {}", projects.display()))?
    {
        let project = project?;
        let project_path = project.path();
        let project_metadata = fs::symlink_metadata(&project_path)?;
        if project_metadata.file_type().is_symlink() || !project_metadata.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&project_path)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(session_id) = name.strip_suffix(".jsonl") else {
                continue;
            };
            if session_id.is_empty() {
                continue;
            }
            candidates.push(LocatedClaudeSession {
                session_id: session_id.to_owned(),
                jsonl_path: path,
                modified_at: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            });
        }
    }

    match selection {
        ClaudeSessionSelection::Session(session_id) => {
            validate_id("Claude session", session_id)?;
            let mut matches = candidates
                .into_iter()
                .filter(|candidate| candidate.session_id == *session_id)
                .collect::<Vec<_>>();
            match matches.len() {
                0 => bail!(
                    "Claude session {session_id:?} was not found under {}",
                    projects.display()
                ),
                1 => Ok(matches.remove(0)),
                _ => bail!("Claude session {session_id:?} occurs in multiple project directories"),
            }
        }
        ClaudeSessionSelection::Latest => candidates
            .into_iter()
            .max_by(|left, right| {
                left.modified_at
                    .cmp(&right.modified_at)
                    .then_with(|| left.jsonl_path.cmp(&right.jsonl_path))
            })
            .context("no Claude session JSONL files were found"),
    }
}

/// Read the native JSONL only far enough to recover a transcript suitable for
/// Hel's chat view. Full tool traffic and reasoning remain in the copied
/// native rollout, not in this lossy projection.
pub fn read_claude_transcript(path: &Path) -> Result<ClaudeTranscript> {
    let body = fs::read_to_string(path)
        .with_context(|| format!("read Claude session {}", path.display()))?;
    let mut cwd = None;
    let mut events = Vec::new();
    let mut saw_raw_user = false;

    for (index, line) in body.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(line).with_context(|| {
            format!("parse Claude session {} line {}", path.display(), index + 1)
        })?;
        if cwd.is_none() {
            cwd = record
                .get("cwd")
                .and_then(Value::as_str)
                .filter(|cwd| !cwd.trim().is_empty())
                .map(PathBuf::from);
        }
        if record.get("isMeta").and_then(Value::as_bool) == Some(true)
            || record.get("isSidechain").and_then(Value::as_bool) == Some(true)
        {
            continue;
        }
        let compaction_boundary = record.get("type").and_then(Value::as_str) == Some("system")
            && matches!(
                record.get("subtype").and_then(Value::as_str),
                Some("compact_boundary" | "compaction")
            );
        let compaction_summary = record
            .get("isCompactSummary")
            .or_else(|| record.pointer("/message/isCompactSummary"))
            .and_then(Value::as_bool)
            == Some(true);
        if compaction_boundary || compaction_summary {
            ensure!(
                saw_raw_user,
                "Claude session contains a compaction artifact before recoverable raw history"
            );
            continue;
        }
        match record.get("type").and_then(Value::as_str) {
            Some("user") => {
                let Some(text) = record
                    .pointer("/message/content")
                    .and_then(Value::as_str)
                    .filter(|text| !text.trim().is_empty())
                else {
                    continue;
                };
                let request_id = format!("import-{}", events.len() + 1);
                push_event(
                    &mut events,
                    WorkerEvent::PromptAccepted {
                        request_id,
                        text: text.to_owned(),
                        attachments: Vec::new(),
                    },
                );
                saw_raw_user = true;
            }
            Some("assistant") => {
                let Some(content) = record.pointer("/message/content").and_then(Value::as_array)
                else {
                    continue;
                };
                for block in content {
                    let Some(text) = block
                        .get("text")
                        .and_then(Value::as_str)
                        .filter(|text| !text.is_empty())
                    else {
                        continue;
                    };
                    if block.get("type").and_then(Value::as_str) != Some("text") {
                        continue;
                    }
                    push_event(
                        &mut events,
                        WorkerEvent::Adapter {
                            kind: "session_update".into(),
                            payload: json!({
                                "type": "session_update",
                                "update": {
                                    "sessionUpdate": "agent_message_chunk",
                                    "content": {"type": "text", "text": text},
                                },
                            }),
                        },
                    );
                }
                // Claude marks a completed model response independently of
                // its text/tool blocks. Preserve that lifecycle boundary so
                // the restored durable worker is idle and accepts the next
                // user prompt instead of treating the imported turn as live.
                if record
                    .pointer("/message/stop_reason")
                    .and_then(Value::as_str)
                    == Some("end_turn")
                {
                    push_event(&mut events, WorkerEvent::TurnCompleted);
                }
            }
            _ => {}
        }
    }

    let cwd = cwd.context("Claude session does not declare its original cwd")?;
    ensure!(
        cwd.is_absolute(),
        "Claude session cwd is not absolute: {}",
        cwd.display()
    );
    Ok(ClaudeTranscript { cwd, events })
}

/// Project a Codex rollout into the canonical transcript used by Hel chat.
pub fn read_codex_transcript(path: &Path) -> Result<CodexTranscript> {
    let body = fs::read_to_string(path)
        .with_context(|| format!("read Codex session {}", path.display()))?;
    let mut cwd = None;
    let mut events = Vec::new();
    let mut saw_raw_user = false;
    for (index, line) in body.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(line).with_context(|| {
            format!("parse Codex session {} line {}", path.display(), index + 1)
        })?;
        if record.get("type").and_then(Value::as_str) == Some("session_meta") && cwd.is_none() {
            cwd = record
                .pointer("/payload/cwd")
                .and_then(Value::as_str)
                .filter(|cwd| !cwd.trim().is_empty())
                .map(PathBuf::from);
        }
        let record_type = record.get("type").and_then(Value::as_str);
        let payload_type = record.pointer("/payload/type").and_then(Value::as_str);
        let compaction_artifact = matches!(
            record_type,
            Some("compacted" | "context_compaction" | "compaction_summary")
        ) || (record_type == Some("response_item")
            && matches!(
                payload_type,
                Some("compaction" | "compaction_summary" | "context_compaction")
            ));
        if compaction_artifact {
            ensure!(
                saw_raw_user,
                "Codex session contains a compaction artifact before recoverable raw history"
            );
            continue;
        }
        if record.get("type").and_then(Value::as_str) != Some("event_msg") {
            continue;
        }
        match record.pointer("/payload/type").and_then(Value::as_str) {
            Some("user_message") => {
                let Some(text) = record
                    .pointer("/payload/message")
                    .and_then(Value::as_str)
                    .filter(|text| !text.trim().is_empty())
                else {
                    continue;
                };
                let request_id = format!("import-{}", events.len() + 1);
                push_event(
                    &mut events,
                    WorkerEvent::PromptAccepted {
                        request_id,
                        text: text.to_owned(),
                        attachments: Vec::new(),
                    },
                );
                saw_raw_user = true;
            }
            Some("agent_message") => {
                let Some(text) = record
                    .pointer("/payload/message")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                else {
                    continue;
                };
                push_event(
                    &mut events,
                    WorkerEvent::Adapter {
                        kind: "session_update".into(),
                        payload: json!({
                            "type": "session_update",
                            "update": {
                                "sessionUpdate": "agent_message_chunk",
                                "content": {"type": "text", "text": text},
                            },
                        }),
                    },
                );
                if record.pointer("/payload/phase").and_then(Value::as_str) == Some("final_answer")
                {
                    push_event(&mut events, WorkerEvent::TurnCompleted);
                }
            }
            _ => {}
        }
    }
    finish_imported_turn(&mut events);
    let cwd = cwd.context("Codex session does not declare its original cwd")?;
    ensure!(
        cwd.is_absolute(),
        "Codex session cwd is not absolute: {}",
        cwd.display()
    );
    Ok(CodexTranscript { cwd, events })
}

/// Project a Kimi session directory. The main wire stream contains prompts and
/// generated text; tool traffic and thought blocks stay only in native files.
pub fn read_kimi_transcript(session_path: &Path) -> Result<KimiTranscript> {
    let state_path = session_path.join("state.json");
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)
        .with_context(|| format!("parse Kimi session state {}", state_path.display()))?;
    let cwd = state
        .get("workDir")
        .or_else(|| state.get("cwd"))
        .and_then(Value::as_str)
        .filter(|cwd| !cwd.trim().is_empty())
        .map(PathBuf::from)
        .context("Kimi session state does not declare workDir or cwd")?;
    ensure!(
        cwd.is_absolute(),
        "Kimi session workDir is not absolute: {}",
        cwd.display()
    );
    let wire_path = session_path.join("agents/main/wire.jsonl");
    let body = fs::read_to_string(&wire_path)
        .with_context(|| format!("read Kimi wire stream {}", wire_path.display()))?;
    let mut events = Vec::new();
    let mut saw_raw_user = false;
    for (index, line) in body.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(line).with_context(|| {
            format!(
                "parse Kimi wire stream {} line {}",
                wire_path.display(),
                index + 1
            )
        })?;
        if matches!(
            record.get("type").and_then(Value::as_str),
            Some("context.compaction" | "context.compacted" | "compaction")
        ) {
            ensure!(
                saw_raw_user,
                "Kimi session contains a compaction artifact before recoverable raw history"
            );
            continue;
        }
        match record.get("type").and_then(Value::as_str) {
            Some("turn.prompt" | "turn.steer")
                if record.pointer("/origin/kind").and_then(Value::as_str) == Some("user") =>
            {
                finish_imported_turn(&mut events);
                let text = record
                    .pointer("/input")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
                    .filter_map(|part| part.get("text").and_then(Value::as_str))
                    .filter(|text| !text.trim().is_empty())
                    .collect::<Vec<_>>()
                    .join("\n");
                if !text.is_empty() {
                    let request_id = format!("import-{}", events.len() + 1);
                    push_event(
                        &mut events,
                        WorkerEvent::PromptAccepted {
                            request_id,
                            text,
                            attachments: Vec::new(),
                        },
                    );
                    saw_raw_user = true;
                }
            }
            Some("context.append_loop_event")
                if record.pointer("/event/type").and_then(Value::as_str)
                    == Some("content.part")
                    && record.pointer("/event/part/type").and_then(Value::as_str)
                        == Some("text") =>
            {
                let Some(text) = record
                    .pointer("/event/part/text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                else {
                    continue;
                };
                push_event(
                    &mut events,
                    WorkerEvent::Adapter {
                        kind: "session_update".into(),
                        payload: json!({
                            "type": "session_update",
                            "update": {
                                "sessionUpdate": "agent_message_chunk",
                                "content": {"type": "text", "text": text},
                            },
                        }),
                    },
                );
            }
            _ => {}
        }
    }
    finish_imported_turn(&mut events);
    Ok(KimiTranscript { cwd, events })
}

fn finish_imported_turn(events: &mut Vec<SequencedEvent>) {
    if !events.is_empty()
        && !matches!(
            events.last().map(|event| &event.event),
            Some(WorkerEvent::TurnCompleted)
        )
    {
        push_event(events, WorkerEvent::TurnCompleted);
    }
}

fn push_event(events: &mut Vec<SequencedEvent>, event: WorkerEvent) {
    events.push(SequencedEvent {
        seq: events.len() as u64 + 1,
        request_id: None,
        event,
    });
}

/// Find a configured bundle by its primary GitHub repository, or describe a
/// one-repository bundle that can be added after the CLI asks for consent.
pub fn resolve_bundle(
    config: &HelConfig,
    cwd: &Path,
    requested_bundle: Option<&str>,
) -> Result<BundleResolution> {
    if let Some(bundle_id) = requested_bundle {
        ensure!(
            config.bundles.contains_key(bundle_id),
            "unknown bundle {bundle_id:?}"
        );
        return Ok(BundleResolution::Existing(bundle_id.to_owned()));
    }

    let origin = git_text(cwd, ["remote", "get-url", "origin"])?;
    let github = github_repository_from_origin(&origin).context(
        "the original cwd's Git origin is not a GitHub repository; pass --bundle for a configured bundle",
    )?;
    if let Some(id) = configured_bundle_for_origin(config, &github) {
        return Ok(BundleResolution::Existing(id));
    }

    let repository_id = setup_style_id(&github.repository);
    let bundle_id = unique_bundle_id(config, &repository_id);
    Ok(BundleResolution::Synthesized {
        id: bundle_id,
        bundle: ProjectBundle {
            primary_repo: repository_id.clone(),
            repositories: vec![ProjectRepository {
                id: repository_id.clone(),
                github: format!("{}/{}", github.owner, github.repository),
                destination: PathBuf::from(repository_id),
                git_ref: None,
            }],
        },
    })
}

/// Return the matching configured bundle for an origin. It accepts setup's
/// `owner/repository` shorthand as well as normal GitHub remote URLs.
pub fn configured_bundle_for_origin(
    config: &HelConfig,
    origin: &GithubRepository,
) -> Option<String> {
    config.bundles.iter().find_map(|(id, bundle)| {
        let primary = bundle.primary()?;
        let configured = github_repository_from_origin(&primary.github)?;
        same_github_repository(&configured, origin).then(|| id.clone())
    })
}

fn same_github_repository(left: &GithubRepository, right: &GithubRepository) -> bool {
    left.owner.eq_ignore_ascii_case(&right.owner)
        && left.repository.eq_ignore_ascii_case(&right.repository)
}

fn setup_style_id(value: &str) -> String {
    let mut id = value
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
        .take(64)
        .collect::<String>();
    if id.is_empty() || matches!(id.as_str(), "." | "..") {
        id = "repository".into();
    }
    id
}

fn unique_bundle_id(config: &HelConfig, base: &str) -> String {
    if !config.bundles.contains_key(base) {
        return base.into();
    }
    let base = format!("import-{base}");
    if !config.bundles.contains_key(&base) {
        return base;
    }
    for suffix in 2_u32.. {
        let candidate = format!("{base}-{suffix}");
        if !config.bundles.contains_key(&candidate) {
            return candidate;
        }
    }
    unreachable!("u32 bundle suffixes are finite")
}

/// Build, verify, and install a local archive, then update the in-memory state.
/// The caller saves `state` only after this returns successfully.
pub fn import_claude_session(
    config: &HelConfig,
    state: &mut HelState,
    request: ClaudeImportRequest<'_>,
) -> Result<ImportedClaudeSession> {
    let ClaudeImportRequest {
        claude_home,
        source,
        transcript,
        bundle_id,
        title,
        archive_directory,
    } = request;
    let bundle = config
        .bundles
        .get(bundle_id)
        .with_context(|| format!("unknown bundle {bundle_id:?}"))?;
    let session_title_override = title.map(str::to_owned);
    let title = match session_title_override.as_deref() {
        Some(title) if !title.trim().is_empty() => title.to_owned(),
        Some(_) => bail!("import title must not be empty"),
        None => harness_session_title(&transcript.events)
            .unwrap_or_else(|| format!("Imported Claude session {}", source.session_id)),
    };
    let repositories = collect_local_repositories(bundle, &transcript.cwd)?;
    let native_artifacts =
        collect_native_artifacts(HarnessKind::Claude, claude_home, &source.session_id, false)?;
    let canonical_events = encode_events(&transcript.events)?;
    let session_id = new_session_id()?;
    let timestamp = timestamp();
    let profile_id = default_profile(config, HarnessKind::Claude, claude_home);
    let target_id = config
        .targets
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| "import".into());
    let archive_path = archive_directory.join(format!("{session_id}.hel.zip"));
    let verified = write_archive_atomic(
        &archive_path,
        &ArchiveInput {
            session: crate::hel_archive::SessionManifest {
                id: session_id.clone(),
                title: title.clone(),
                harness_kind: HarnessKind::Claude,
                profile_id: profile_id.clone(),
                native_session_id: source.session_id.clone(),
                created_at: timestamp.clone(),
                checkpointed_at: timestamp.clone(),
                hel_version: env!("CARGO_PKG_VERSION").into(),
                worker_version: env!("CARGO_PKG_VERSION").into(),
                adapter_version: "acp-v1".into(),
            },
            target: TargetManifest {
                template_id: target_id.clone(),
                target_kind: "import".into(),
                details: BTreeMap::from([("source".into(), "claude-import".into())]),
            },
            bundle: BundleManifest {
                id: bundle_id.to_owned(),
                primary_repository: bundle.primary_repo.clone(),
            },
            canonical_events,
            native_artifacts,
            repositories,
        },
    )?;
    let checkpoint = CheckpointMetadata {
        archive_path: archive_path.clone(),
        sha256: verified.archive_sha256,
        created_at: timestamp.clone(),
        event_sequence: transcript.events.last().map_or(0, |event| event.seq),
    };
    state.sessions.insert(
        session_id.clone(),
        SessionRecord {
            id: session_id.clone(),
            title,
            harness_kind: HarnessKind::Claude,
            last_profile: profile_id,
            bundle_id: bundle_id.to_owned(),
            target_template_id: target_id,
            additional_mounts: Vec::new(),
            state: SessionState::Archived,
            target: None,
            native_session_id: Some(source.session_id.clone()),
            acp_session_title: None,
            session_title_override,
            created_at: timestamp.clone(),
            updated_at: timestamp,
            last_viewed_event_sequence: 0,
            last_error: None,
            checkpoint: Some(checkpoint),
        },
    );
    Ok(ImportedClaudeSession {
        session_id,
        native_session_id: source.session_id.clone(),
        source_jsonl: source.jsonl_path.clone(),
        source_cwd: transcript.cwd.clone(),
        bundle_id: bundle_id.to_owned(),
        archive_path,
    })
}

pub fn import_codex_session(
    config: &HelConfig,
    state: &mut HelState,
    request: CodexImportRequest<'_>,
) -> Result<ImportedCodexSession> {
    let CodexImportRequest {
        codex_home,
        source,
        transcript,
        bundle_id,
        title,
        archive_directory,
    } = request;
    import_native_session(
        config,
        state,
        NativeImportRequest {
            harness: HarnessKind::Codex,
            harness_home: codex_home,
            native_session_id: &source.session_id,
            source_path: &source.jsonl_path,
            transcript,
            bundle_id,
            title,
            archive_directory,
        },
    )
}

pub fn import_kimi_session(
    config: &HelConfig,
    state: &mut HelState,
    request: KimiImportRequest<'_>,
) -> Result<ImportedKimiSession> {
    let KimiImportRequest {
        kimi_home,
        source,
        transcript,
        bundle_id,
        title,
        archive_directory,
    } = request;
    import_native_session(
        config,
        state,
        NativeImportRequest {
            harness: HarnessKind::Kimi,
            harness_home: kimi_home,
            native_session_id: &source.session_id,
            source_path: &source.session_path,
            transcript,
            bundle_id,
            title,
            archive_directory,
        },
    )
}

struct NativeImportRequest<'a> {
    harness: HarnessKind,
    harness_home: &'a Path,
    native_session_id: &'a str,
    source_path: &'a Path,
    transcript: &'a ClaudeTranscript,
    bundle_id: &'a str,
    title: Option<&'a str>,
    archive_directory: &'a Path,
}

fn import_native_session(
    config: &HelConfig,
    state: &mut HelState,
    request: NativeImportRequest<'_>,
) -> Result<ImportedClaudeSession> {
    let NativeImportRequest {
        harness,
        harness_home,
        native_session_id,
        source_path,
        transcript,
        bundle_id,
        title,
        archive_directory,
    } = request;
    let bundle = config
        .bundles
        .get(bundle_id)
        .with_context(|| format!("unknown bundle {bundle_id:?}"))?;
    let session_title_override = title.map(str::to_owned);
    let title = match session_title_override.as_deref() {
        Some(title) if !title.trim().is_empty() => title.to_owned(),
        Some(_) => bail!("import title must not be empty"),
        None => harness_session_title(&transcript.events).unwrap_or_else(|| {
            format!(
                "Imported {} session {native_session_id}",
                harness_name(harness)
            )
        }),
    };
    let repositories = collect_local_repositories(bundle, &transcript.cwd)?;
    let native_artifacts =
        collect_import_native_artifacts(harness, harness_home, native_session_id, source_path)?;
    let canonical_events = encode_events(&transcript.events)?;
    let session_id = new_session_id()?;
    let timestamp = timestamp();
    let profile_id = default_profile(config, harness, harness_home);
    let target_id = config
        .targets
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| "import".into());
    let archive_path = archive_directory.join(format!("{session_id}.hel.zip"));
    let verified = write_archive_atomic(
        &archive_path,
        &ArchiveInput {
            session: crate::hel_archive::SessionManifest {
                id: session_id.clone(),
                title: title.clone(),
                harness_kind: harness,
                profile_id: profile_id.clone(),
                native_session_id: native_session_id.to_owned(),
                created_at: timestamp.clone(),
                checkpointed_at: timestamp.clone(),
                hel_version: env!("CARGO_PKG_VERSION").into(),
                worker_version: env!("CARGO_PKG_VERSION").into(),
                adapter_version: "acp-v1".into(),
            },
            target: TargetManifest {
                template_id: target_id.clone(),
                target_kind: "import".into(),
                details: BTreeMap::from([(
                    "source".into(),
                    format!("{}-import", harness_name(harness).to_ascii_lowercase()),
                )]),
            },
            bundle: BundleManifest {
                id: bundle_id.to_owned(),
                primary_repository: bundle.primary_repo.clone(),
            },
            canonical_events,
            native_artifacts,
            repositories,
        },
    )?;
    let checkpoint = CheckpointMetadata {
        archive_path: archive_path.clone(),
        sha256: verified.archive_sha256,
        created_at: timestamp.clone(),
        event_sequence: transcript.events.last().map_or(0, |event| event.seq),
    };
    state.sessions.insert(
        session_id.clone(),
        SessionRecord {
            id: session_id.clone(),
            title,
            harness_kind: harness,
            last_profile: profile_id,
            bundle_id: bundle_id.to_owned(),
            target_template_id: target_id,
            additional_mounts: Vec::new(),
            state: SessionState::Archived,
            target: None,
            native_session_id: Some(native_session_id.to_owned()),
            acp_session_title: None,
            session_title_override,
            created_at: timestamp.clone(),
            updated_at: timestamp,
            last_viewed_event_sequence: 0,
            last_error: None,
            checkpoint: Some(checkpoint),
        },
    );
    Ok(ImportedClaudeSession {
        session_id,
        native_session_id: native_session_id.to_owned(),
        source_jsonl: source_path.to_path_buf(),
        source_cwd: transcript.cwd.clone(),
        bundle_id: bundle_id.to_owned(),
        archive_path,
    })
}

fn harness_name(harness: HarnessKind) -> &'static str {
    match harness {
        HarnessKind::Codex => "Codex",
        HarnessKind::Claude => "Claude",
        HarnessKind::Kimi => "Kimi",
    }
}
fn collect_local_repositories(
    bundle: &ProjectBundle,
    cwd: &Path,
) -> Result<Vec<crate::hel_archive::RepositorySnapshot>> {
    let primary = bundle
        .primary()
        .context("bundle primary repository is missing")?;
    let primary_path = PathBuf::from(git_text(cwd, ["rev-parse", "--show-toplevel"])?);
    let workspace_root = workspace_root_for_primary(&primary_path, &primary.destination);
    let git = SystemGit;
    bundle
        .repositories
        // Indexed parallel iteration keeps repository and manifest order
        // identical to the configured bundle.
        .par_iter()
        .map(|repository| {
            let path = if repository.id == primary.id {
                primary_path.clone()
            } else {
                workspace_root.join(&repository.destination)
            };
            ensure!(
                path.is_dir(),
                "local repository {:?} is missing at {}",
                repository.id,
                path.display()
            );
            let base_commit = import_base_commit(&path)?;
            collect_git_snapshot(
                &git,
                &path,
                &GitCollectionSpec {
                    id: repository.id.clone(),
                    relative_destination: repository.destination.clone(),
                    // Import starts from the common ancestor of the local
                    // checkout and the tracked remote, so unpushed commits
                    // are included in the committed delta bundle.
                    base_commit,
                },
            )
            .with_context(|| format!("collect local repository {:?}", repository.id))
        })
        .collect()
}

fn workspace_root_for_primary(primary_path: &Path, destination: &Path) -> PathBuf {
    let mut root = primary_path.to_path_buf();
    for _ in 0..destination.components().count() {
        if !root.pop() {
            return primary_path.parent().unwrap_or(primary_path).to_path_buf();
        }
    }
    if root.join(destination) == primary_path {
        root
    } else {
        primary_path.parent().unwrap_or(primary_path).to_path_buf()
    }
}

fn encode_events(events: &[SequencedEvent]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for event in events {
        serde_json::to_writer(&mut bytes, event)?;
        bytes.push(b'\n');
    }
    Ok(bytes)
}

fn default_profile(config: &HelConfig, harness: HarnessKind, home: &Path) -> String {
    let source = fs::canonicalize(home).unwrap_or_else(|_| home.to_path_buf());
    config
        .profiles
        .iter()
        .find(|(_, profile)| {
            profile.kind == harness
                && fs::canonicalize(&profile.home).unwrap_or_else(|_| profile.home.clone())
                    == source
        })
        .or_else(|| {
            config
                .profiles
                .iter()
                .find(|(_, profile)| profile.kind == harness)
        })
        .map(|(id, _)| id.clone())
        .unwrap_or_else(|| format!("{}-import", harness_name(harness).to_ascii_lowercase()))
}

fn import_base_commit(path: &Path) -> Result<String> {
    let upstream = git_optional_text(path, ["rev-parse", "--verify", "--quiet", "@{upstream}"])?
        .or(git_optional_text(
            path,
            [
                "rev-parse",
                "--verify",
                "--quiet",
                "refs/remotes/origin/HEAD",
            ],
        )?);
    let Some(upstream) = upstream else {
        // A repository without remote refs cannot tell us which ancestry a
        // newly provisioned clone has. Preserve the previous HEAD-only
        // behavior; normal tracked checkouts take the bundle-preserving path.
        return git_text(path, ["rev-parse", "HEAD"]);
    };
    git_text(path, ["merge-base", "HEAD", &upstream])
}

fn git_optional_text<const N: usize>(cwd: &Path, arguments: [&str; N]) -> Result<Option<String>> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(cwd)
        .output()
        .with_context(|| format!("start git in {}", cwd.display()))?;
    if !output.status.success() {
        return Ok(None);
    }
    let text = String::from_utf8(output.stdout).context("decode Git output")?;
    Ok((!text.trim().is_empty()).then(|| text.trim().to_owned()))
}

fn git_text<const N: usize>(cwd: &Path, arguments: [&str; N]) -> Result<String> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(cwd)
        .output()
        .with_context(|| format!("start git in {}", cwd.display()))?;
    if !output.status.success() {
        bail!(
            "git in {} failed: {}",
            cwd.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let text = String::from_utf8(output.stdout).context("decode Git output")?;
    let text = text.trim();
    ensure!(
        !text.is_empty(),
        "git in {} returned no output",
        cwd.display()
    );
    Ok(text.into())
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initialize_repository(path: &Path, id: &str) {
        fs::create_dir_all(path).unwrap();
        for arguments in [
            vec!["init", "-q", "-b", "main"],
            vec!["config", "user.name", "Hel Test"],
            vec!["config", "user.email", "hel@example.test"],
            vec![
                "remote",
                "add",
                "origin",
                &format!("https://github.com/example/{id}.git"),
            ],
        ] {
            let output = Command::new("git")
                .args(arguments)
                .current_dir(path)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        fs::write(path.join("README.md"), id).unwrap();
        let output = Command::new("git")
            .args(["add", "."])
            .current_dir(path)
            .output()
            .unwrap();
        assert!(output.status.success());
        let output = Command::new("git")
            .args(["commit", "-qm", "base"])
            .current_dir(path)
            .output()
            .unwrap();
        assert!(output.status.success());
    }

    #[test]
    fn projects_jsonl_projects_user_and_assistant_text_in_source_order() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"user","cwd":"/work/app","message":{"content":"first prompt"}}"#,
                "\n",
                r#"{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"hidden"},{"type":"text","text":"first reply"}]}}"#,
                "\n",
                r#"{"type":"user","message":{"content":[{"type":"tool_result","content":"ignored"}]}}"#,
                "\n",
                r#"{"type":"user","isMeta":true,"message":{"content":"ignored meta"}}"#,
                "\n",
                r#"{"type":"user","message":{"content":"second prompt"}}"#,
                "\n",
                r#"{"type":"assistant","message":{"stop_reason":"end_turn","content":[{"type":"text","text":"second "},{"type":"text","text":"reply"}]}}"#,
                "\n",
            ),
        )
        .unwrap();

        let transcript = read_claude_transcript(&path).unwrap();
        assert_eq!(transcript.cwd, PathBuf::from("/work/app"));
        assert_eq!(
            transcript
                .events
                .iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6]
        );
        assert!(matches!(
            &transcript.events[0].event,
            WorkerEvent::PromptAccepted { text, .. } if text == "first prompt"
        ));
        assert_eq!(
            agent_text(&transcript.events[1].event).as_deref(),
            Some("first reply")
        );
        assert!(matches!(
            &transcript.events[2].event,
            WorkerEvent::PromptAccepted { text, .. } if text == "second prompt"
        ));
        assert_eq!(
            agent_text(&transcript.events[4].event).as_deref(),
            Some("reply")
        );
        assert!(matches!(
            transcript.events[5].event,
            WorkerEvent::TurnCompleted
        ));
    }

    #[test]
    fn bundle_origin_mapping_matches_configured_primary_repository() {
        let mut config = HelConfig::default();
        config.bundles.insert(
            "hel".into(),
            ProjectBundle {
                primary_repo: "hel".into(),
                repositories: vec![ProjectRepository {
                    id: "hel".into(),
                    github: "BrokkAi/hel".into(),
                    destination: "hel".into(),
                    git_ref: None,
                }],
            },
        );
        let origin = github_repository_from_origin("git@github.com:brokkai/HEL.git").unwrap();
        assert_eq!(
            configured_bundle_for_origin(&config, &origin).as_deref(),
            Some("hel")
        );
    }

    #[test]
    fn local_repository_collection_preserves_bundle_order() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        let app = workspace.join("app");
        initialize_repository(&app, "app");
        initialize_repository(&workspace.join("worker"), "worker");
        let bundle = ProjectBundle {
            primary_repo: "app".into(),
            repositories: vec![
                ProjectRepository {
                    id: "worker".into(),
                    github: "example/worker".into(),
                    destination: "worker".into(),
                    git_ref: None,
                },
                ProjectRepository {
                    id: "app".into(),
                    github: "example/app".into(),
                    destination: "app".into(),
                    git_ref: None,
                },
            ],
        };

        let snapshots = collect_local_repositories(&bundle, &app).unwrap();
        let ids = snapshots
            .iter()
            .map(|snapshot| snapshot.metadata.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["worker", "app"]);
    }

    #[test]
    fn codex_jsonl_projects_user_and_agent_messages() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rollout.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"session_meta","payload":{"session_id":"019feb6c-5ffc-7c12-ad99-bdeaeb6be79d","cwd":"/work/app"}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"user_message","message":"first prompt"}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"agent_message","message":"first reply","phase":"final_answer"}}"#,
                "\n",
            ),
        )
        .unwrap();

        let transcript = read_codex_transcript(&path).unwrap();
        assert_eq!(transcript.cwd, PathBuf::from("/work/app"));
        assert!(matches!(
            &transcript.events[0].event,
            WorkerEvent::PromptAccepted { text, .. } if text == "first prompt"
        ));
        assert_eq!(
            agent_text(&transcript.events[1].event).as_deref(),
            Some("first reply")
        );
        assert!(matches!(
            transcript.events[2].event,
            WorkerEvent::TurnCompleted
        ));
    }

    #[test]
    fn codex_import_omits_compaction_blobs_but_requires_prior_raw_history() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rollout.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"session_meta","payload":{"cwd":"/work/app"}}"#,
                "\n",
                r#"{"type":"response_item","payload":{"type":"compaction","encrypted_content":"opaque"}}"#,
                "\n",
            ),
        )
        .unwrap();
        assert!(
            read_codex_transcript(&path)
                .unwrap_err()
                .to_string()
                .contains("before recoverable raw history")
        );

        fs::write(
            &path,
            concat!(
                r#"{"type":"session_meta","payload":{"cwd":"/work/app"}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"user_message","message":"raw prompt"}}"#,
                "\n",
                r#"{"type":"response_item","payload":{"type":"compaction","encrypted_content":"opaque"}}"#,
                "\n",
            ),
        )
        .unwrap();
        let transcript = read_codex_transcript(&path).unwrap();
        assert_eq!(transcript.events.len(), 2);
    }

    #[test]
    fn claude_import_rejects_a_compaction_summary_without_raw_history() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"system","subtype":"compact_boundary","cwd":"/work/app"}"#,
                "\n",
                r#"{"type":"user","isCompactSummary":true,"message":{"content":"summary"}}"#,
                "\n",
            ),
        )
        .unwrap();
        assert!(
            read_claude_transcript(&path)
                .unwrap_err()
                .to_string()
                .contains("before recoverable raw history")
        );
    }

    #[test]
    fn codex_locator_uses_rollout_id_when_session_id_names_a_parent() {
        let directory = tempfile::tempdir().unwrap();
        let rollout = directory
            .path()
            .join("sessions/2026/08/10/rollout-2026-08-10T00-00-00-019feb6c-6b55-7111-a210-6d85ee0772cd.jsonl");
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        fs::write(
            &rollout,
            r#"{"type":"session_meta","payload":{"session_id":"019feb6c-5ffc-7c12-ad99-bdeaeb6be79d","id":"019feb6c-6b55-7111-a210-6d85ee0772cd"}}"#,
        )
        .unwrap();

        let located = locate_codex_session(
            directory.path(),
            &CodexSessionSelection::Session("019feb6c-6b55-7111-a210-6d85ee0772cd".into()),
        )
        .unwrap();
        assert_eq!(located.session_id, "019feb6c-6b55-7111-a210-6d85ee0772cd");
        assert_eq!(located.jsonl_path, rollout);
    }

    #[test]
    fn kimi_wire_projects_user_prompt_and_text_without_thought() {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir_all(directory.path().join("agents/main")).unwrap();
        fs::write(
            directory.path().join("state.json"),
            r#"{"workDir":"/work/app"}"#,
        )
        .unwrap();
        fs::write(
            directory.path().join("agents/main/wire.jsonl"),
            concat!(
                r#"{"type":"turn.prompt","origin":{"kind":"user"},"input":[{"type":"text","text":"first prompt"}]}"#,
                "\n",
                r#"{"type":"context.append_loop_event","event":{"type":"content.part","part":{"type":"think","think":"hidden"}}}"#,
                "\n",
                r#"{"type":"context.append_loop_event","event":{"type":"content.part","part":{"type":"text","text":"first reply"}}}"#,
                "\n",
                r#"{"type":"turn.steer","origin":{"kind":"user"},"input":[{"type":"text","text":"follow up"}]}"#,
                "\n",
                r#"{"type":"context.append_loop_event","event":{"type":"content.part","part":{"type":"text","text":"second reply"}}}"#,
                "\n",
            ),
        )
        .unwrap();

        let transcript = read_kimi_transcript(directory.path()).unwrap();
        assert_eq!(transcript.cwd, PathBuf::from("/work/app"));
        assert!(matches!(
            &transcript.events[0].event,
            WorkerEvent::PromptAccepted { text, .. } if text == "first prompt"
        ));
        assert_eq!(
            agent_text(&transcript.events[1].event).as_deref(),
            Some("first reply")
        );
        assert!(matches!(
            transcript.events[2].event,
            WorkerEvent::TurnCompleted
        ));
        assert!(matches!(
            &transcript.events[3].event,
            WorkerEvent::PromptAccepted { text, .. } if text == "follow up"
        ));
        assert_eq!(
            agent_text(&transcript.events[4].event).as_deref(),
            Some("second reply")
        );
        assert!(matches!(
            transcript.events[5].event,
            WorkerEvent::TurnCompleted
        ));
    }

    #[test]
    fn kimi_locator_retains_its_native_session_directory_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let id = "90c30a64-54f7-4261-90f1-e75b1c14311c";
        let session = directory
            .path()
            .join("sessions/project")
            .join(format!("session_{id}"));
        fs::create_dir_all(&session).unwrap();

        let located = locate_kimi_session(
            directory.path(),
            &KimiSessionSelection::Session(format!("session_{id}")),
        )
        .unwrap();
        assert_eq!(located.session_id, format!("session_{id}"));
        assert_eq!(located.session_path, session);
    }

    fn agent_text(event: &WorkerEvent) -> Option<String> {
        let WorkerEvent::Adapter { payload, .. } = event else {
            return None;
        };
        payload
            .pointer("/update/content/text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    }
}
