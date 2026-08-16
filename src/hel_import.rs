//! Import native harness sessions into Hel's durable archive format.
//

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use rayon::prelude::*;
use serde_json::{Value, json};

use crate::hel_archive::{
    ArchiveInput, BundleManifest, GitCollectionSpec, GitHistoryMode, GitSnapshotProgress,
    SystemGit, TargetManifest, collect_git_snapshot_with_progress, write_archive_atomic,
};
use crate::hel_chat::ChatState;
use crate::hel_checkpoint::{collect_import_native_artifacts, collect_native_artifacts};
use crate::hel_config::{
    HarnessKind, HelConfig, ProjectBundle, ProjectRepository, TargetTemplate, validate_id,
};
use crate::hel_local_git::main_worktree_root;
use crate::hel_projection::canonical_session_from_materialized;
use crate::hel_setup::{GithubRepository, github_repository_from_origin};
use crate::hel_state::{
    CheckpointMetadata, HelState, SessionRecord, SessionState, harness_session_title,
    new_session_id,
};
use crate::hel_worker::{SequencedEvent, WorkerEvent, WorkerPhase, WorkerSnapshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaudeSessionSelection {
    NativeSessionId(String),
    Latest,
}

pub type CodexSessionSelection = ClaudeSessionSelection;
pub type KimiSessionSelection = ClaudeSessionSelection;

#[derive(Debug, Clone)]
pub struct LocatedClaudeSession {
    pub native_session_id: String,
    pub jsonl_path: PathBuf,
    pub modified_at: SystemTime,
    pub title: String,
    pub cwd: PathBuf,
    pub git_branch: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexHistoryMode {
    Legacy,
    Paginated,
}

pub const CODEX_LEGACY_IMPORT_ISSUE: &str = "Legacy Codex history cannot be imported. Run codex migrate-rollouts --apply, then reopen \
     this dialog.";

impl CodexHistoryMode {
    pub fn import_issue(self) -> Option<&'static str> {
        match self {
            Self::Legacy => Some(CODEX_LEGACY_IMPORT_ISSUE),
            Self::Paginated => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocatedCodexSession {
    pub native_session_id: String,
    pub jsonl_path: PathBuf,
    pub modified_at: SystemTime,
    pub title: String,
    pub cwd: PathBuf,
    pub git_branch: String,
    pub size_bytes: u64,
    pub history_mode: CodexHistoryMode,
}

#[derive(Debug, Clone)]
pub struct LocatedKimiSession {
    pub native_session_id: String,
    pub session_path: PathBuf,
    pub modified_at: SystemTime,
    pub title: String,
    pub cwd: PathBuf,
    pub git_branch: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct SessionScanProgress<T> {
    pub scanned: usize,
    pub total: usize,
    pub session: Option<T>,
}

#[derive(Debug)]
struct FileScanCandidate {
    path: PathBuf,
    modified_at: SystemTime,
    size_bytes: u64,
}

#[derive(Debug)]
struct KimiScanCandidate {
    native_session_id: String,
    session_path: PathBuf,
    modified_at: SystemTime,
    title: String,
    cwd: PathBuf,
}

#[derive(Debug)]
struct CodexSessionMetadata {
    id: String,
    cwd: PathBuf,
    git_branch: String,
    history_mode: CodexHistoryMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeTranscript {
    pub cwd: PathBuf,
    /// Files reliably reported as edited by the native harness.
    pub edited_paths: Vec<PathBuf>,
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
pub struct SessionEditTargets {
    pub git_roots: Vec<PathBuf>,
    pub non_git_dirs: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportSafetyIssues {
    pub dirty_git_roots: Vec<(PathBuf, String)>,
    pub omitted_non_git_dirs: Vec<PathBuf>,
    pub has_untracked_files: bool,
}

pub fn import_safety_issues(targets: &SessionEditTargets) -> Result<ImportSafetyIssues> {
    let mut dirty_git_roots = Vec::new();
    let mut has_untracked_files = false;
    for root in &targets.git_roots {
        let output = Command::new("git")
            .args(["status", "--porcelain=v1", "--untracked-files=normal"])
            .current_dir(root)
            .output()
            .with_context(|| format!("inspect Git status in {}", root.display()))?;
        ensure!(
            output.status.success(),
            "could not inspect Git status in {}",
            root.display()
        );
        let (tracked, untracked) = String::from_utf8_lossy(&output.stdout).lines().fold(
            (0_usize, 0_usize),
            |(tracked, untracked), line| {
                if line.starts_with("??") {
                    (tracked, untracked + 1)
                } else {
                    (tracked + 1, untracked)
                }
            },
        );
        has_untracked_files |= untracked > 0;
        if tracked + untracked > 0 {
            let mut parts = Vec::new();
            if tracked > 0 {
                parts.push(format!(
                    "{tracked} tracked change{}",
                    if tracked == 1 { "" } else { "s" }
                ));
            }
            if untracked > 0 {
                parts.push(format!(
                    "{untracked} untracked path{}",
                    if untracked == 1 { "" } else { "s" }
                ));
            }
            dirty_git_roots.push((root.clone(), parts.join(" · ")));
        }
    }
    Ok(ImportSafetyIssues {
        dirty_git_roots,
        omitted_non_git_dirs: targets.non_git_dirs.clone(),
        has_untracked_files,
    })
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportArchiveProgress {
    Repository {
        current: usize,
        total: usize,
        id: String,
    },
    UntrackedFile {
        repository_id: String,
        current: usize,
        total: usize,
        path: PathBuf,
    },
    WritingArchive,
}

pub struct ImportControl<'a> {
    pub cancelled: &'a AtomicBool,
    pub progress: &'a (dyn Fn(ImportArchiveProgress) + Sync),
    pub include_untracked: bool,
}

impl ImportControl<'_> {
    fn check_cancelled(&self) -> Result<()> {
        ensure!(!self.cancelled.load(Ordering::Acquire), "import cancelled");
        Ok(())
    }

    fn report(&self, progress: ImportArchiveProgress) -> Result<()> {
        self.check_cancelled()?;
        (self.progress)(progress);
        Ok(())
    }
}

pub struct ClaudeImportRequest<'a> {
    pub claude_home: &'a Path,
    pub source: &'a LocatedClaudeSession,
    pub transcript: &'a ClaudeTranscript,
    pub bundle_id: &'a str,
    pub profile_id: Option<&'a str>,
    pub title: Option<&'a str>,
    pub archive_directory: &'a Path,
}

pub struct CodexImportRequest<'a> {
    pub codex_home: &'a Path,
    pub source: &'a LocatedCodexSession,
    pub transcript: &'a CodexTranscript,
    pub bundle_id: &'a str,
    pub profile_id: Option<&'a str>,
    pub title: Option<&'a str>,
    pub archive_directory: &'a Path,
}

pub struct KimiImportRequest<'a> {
    pub kimi_home: &'a Path,
    pub source: &'a LocatedKimiSession,
    pub transcript: &'a KimiTranscript,
    pub bundle_id: &'a str,
    pub profile_id: Option<&'a str>,
    pub title: Option<&'a str>,
    pub archive_directory: &'a Path,
}

/// Resolve a harness's configuration home without ever modifying it.
///
/// The environment override wins; otherwise the harness's default directory
/// beneath the user's home is used, the same pair `hel setup` discovers.
pub fn harness_config_home(kind: HarnessKind) -> Result<PathBuf> {
    let name = kind.display_name();
    let home = std::env::var_os(kind.home_env())
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(kind.default_home_leaf())))
        .with_context(|| format!("cannot determine {name} home; set {}", kind.home_env()))?;
    ensure!(
        home.is_dir(),
        "{name} home is not a directory: {}",
        home.display()
    );
    Ok(home)
}

/// Resolve the Claude configuration home without ever modifying it.
pub fn claude_config_home() -> Result<PathBuf> {
    harness_config_home(HarnessKind::Claude)
}

/// Resolve the Codex configuration home without ever modifying it.
pub fn codex_config_home() -> Result<PathBuf> {
    harness_config_home(HarnessKind::Codex)
}

/// Resolve the Kimi Code configuration home without ever modifying it.
pub fn kimi_config_home() -> Result<PathBuf> {
    harness_config_home(HarnessKind::Kimi)
}

/// Locate a Codex rollout exposed by its native interactive resume picker.
pub fn locate_codex_session(
    home: &Path,
    selection: &CodexSessionSelection,
) -> Result<LocatedCodexSession> {
    let listed = list_codex_sessions(home)?;
    if let CodexSessionSelection::NativeSessionId(session_id) = selection
        && !listed
            .iter()
            .any(|session| session.native_session_id == *session_id)
    {
        return locate_unindexed_codex_session(home, session_id);
    }
    select_jsonl_session(listed, selection, "Codex")
}

fn locate_unindexed_codex_session(home: &Path, session_id: &str) -> Result<LocatedCodexSession> {
    validate_id("Codex session", session_id)?;
    let mut requested = BTreeMap::new();
    requested.insert(session_id.to_owned(), session_id.to_owned());
    let mut candidates = Vec::new();
    let root = home.join("sessions");
    if root.is_dir() {
        collect_codex_candidate_paths(&root, &requested, &mut candidates)?;
    }
    let titles = codex_native_titles(home)?;
    let mut matches = Vec::new();
    for candidate in candidates {
        let Some(metadata) = codex_session_metadata(&candidate.path)? else {
            continue;
        };
        if metadata.id == session_id {
            matches.push(LocatedCodexSession {
                title: titles
                    .get(session_id)
                    .cloned()
                    .unwrap_or_else(|| session_id.to_owned()),
                native_session_id: metadata.id,
                jsonl_path: candidate.path,
                modified_at: candidate.modified_at,
                cwd: metadata.cwd,
                git_branch: metadata.git_branch,
                size_bytes: candidate.size_bytes,
                history_mode: metadata.history_mode,
            });
        }
    }
    select_jsonl_session(
        matches,
        &CodexSessionSelection::NativeSessionId(session_id.to_owned()),
        "Codex",
    )
}

/// List native Codex sessions newest first.
pub fn list_codex_sessions(home: &Path) -> Result<Vec<LocatedCodexSession>> {
    let mut sessions = Vec::new();
    scan_codex_sessions(home, |progress| {
        if let Some(session) = progress.session {
            sessions.push(session);
        }
    })?;
    Ok(sessions)
}

/// Scan native Codex sessions newest first, reporting after every candidate file.
pub fn scan_codex_sessions(
    home: &Path,
    mut report: impl FnMut(SessionScanProgress<LocatedCodexSession>),
) -> Result<()> {
    if let Some(sessions) = codex_indexed_sessions(home)? {
        let total = sessions.len();
        report(SessionScanProgress {
            scanned: 0,
            total,
            session: None,
        });
        for (index, session) in sessions.into_iter().enumerate() {
            report(SessionScanProgress {
                scanned: index + 1,
                total,
                session: Some(session),
            });
        }
        return Ok(());
    }

    // Native Codex only indexes threads with a non-empty preview/name. Its
    // history and session-name index provide the same compact set of IDs,
    // avoiding an expensive parse of every exec and subagent rollout.
    let titles = codex_native_titles(home)?;
    let mut candidates = Vec::new();
    let root = home.join("sessions");
    if root.is_dir() {
        collect_codex_candidate_paths(&root, &titles, &mut candidates)?;
    }
    candidates.sort_by(|left, right| {
        right
            .modified_at
            .cmp(&left.modified_at)
            .then_with(|| right.path.cmp(&left.path))
    });
    let total = candidates.len();
    report(SessionScanProgress {
        scanned: 0,
        total,
        session: None,
    });
    for (index, candidate) in candidates.into_iter().enumerate() {
        let session = codex_session_metadata(&candidate.path)?.map(|metadata| {
            let session_id = metadata.id;
            LocatedCodexSession {
                title: titles
                    .get(&session_id)
                    .cloned()
                    .unwrap_or_else(|| session_id.clone()),
                native_session_id: session_id,
                jsonl_path: candidate.path,
                modified_at: candidate.modified_at,
                cwd: metadata.cwd,
                git_branch: metadata.git_branch,
                size_bytes: candidate.size_bytes,
                history_mode: metadata.history_mode,
            }
        });
        report(SessionScanProgress {
            scanned: index + 1,
            total,
            session,
        });
    }
    Ok(())
}

fn codex_indexed_sessions(home: &Path) -> Result<Option<Vec<LocatedCodexSession>>> {
    let database = home.join("state_5.sqlite");
    if !database.is_file() {
        return Ok(None);
    }
    let connection = rusqlite::Connection::open_with_flags(
        database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let has_history_mode = connection
        .prepare("SELECT history_mode FROM threads LIMIT 0")
        .is_ok();
    let history_mode_column = if has_history_mode {
        "history_mode"
    } else {
        "'legacy'"
    };
    let query = format!(
        "SELECT id, rollout_path, updated_at, COALESCE(NULLIF(name, ''), NULLIF(title, ''), id), cwd, \
         COALESCE(NULLIF(git_branch, ''), 'HEAD'), {history_mode_column} \
         FROM threads \
         WHERE archived = 0 \
           AND source IN ('cli', 'vscode') \
           AND preview <> '' \
           AND rollout_path IS NOT NULL \
         ORDER BY updated_at DESC, id DESC"
    );
    let Ok(mut statement) = connection.prepare(&query) else {
        return Ok(None);
    };
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
        ))
    })?;
    let mut sessions = Vec::new();
    for row in rows {
        let (session_id, path, updated_at, title, cwd, git_branch, history_mode) = row?;
        let path = PathBuf::from(path);
        if validate_id("Codex session", &session_id).is_err() || updated_at.is_negative() {
            continue;
        }
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            continue;
        }
        sessions.push(LocatedCodexSession {
            native_session_id: session_id.clone(),
            jsonl_path: path,
            modified_at: SystemTime::UNIX_EPOCH + Duration::from_secs(updated_at as u64),
            title: if title.trim().is_empty() {
                session_id
            } else {
                single_line_title(&title)
            },
            cwd: PathBuf::from(cwd),
            git_branch,
            size_bytes: metadata.len(),
            history_mode: parse_codex_history_mode(&history_mode)?,
        });
    }
    Ok(Some(sessions))
}

fn collect_codex_candidate_paths(
    root: &Path,
    native_titles: &BTreeMap<String, String>,
    candidates: &mut Vec<FileScanCandidate>,
) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            collect_codex_candidate_paths(&path, native_titles, candidates)?;
            continue;
        }
        if !metadata.is_file() || path.extension().and_then(|value| value.to_str()) != Some("jsonl")
        {
            continue;
        }
        if let Some(session_id) = codex_rollout_id_from_path(&path)
            && !native_titles.contains_key(session_id)
        {
            continue;
        }
        candidates.push(FileScanCandidate {
            path,
            modified_at: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            size_bytes: metadata.len(),
        });
    }
    Ok(())
}

fn codex_rollout_id_from_path(path: &Path) -> Option<&str> {
    let stem = path.file_stem()?.to_str()?;
    let id = stem.get(stem.len().checked_sub(36)?..)?;
    (id.as_bytes().get(8) == Some(&b'-')
        && id.as_bytes().get(13) == Some(&b'-')
        && id.as_bytes().get(18) == Some(&b'-')
        && id.as_bytes().get(23) == Some(&b'-'))
    .then_some(id)
}

fn codex_session_metadata(path: &Path) -> Result<Option<CodexSessionMetadata>> {
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
        if !codex_source_is_interactive(record.pointer("/payload/source")) {
            return Ok(None);
        }
        // Ephemeral Codex threads normally have no rollout path at all. Keep
        // this defensive check so a future writer cannot expose one here.
        if record
            .pointer("/payload/ephemeral")
            .and_then(Value::as_bool)
            == Some(true)
        {
            return Ok(None);
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
            let cwd = record
                .pointer("/payload/cwd")
                .and_then(Value::as_str)
                .filter(|cwd| !cwd.trim().is_empty())
                .map(PathBuf::from)
                .unwrap_or_default();
            let git_branch = record
                .pointer("/payload/git/branch")
                .and_then(Value::as_str)
                .filter(|branch| !branch.trim().is_empty())
                .unwrap_or("HEAD")
                .to_owned();
            let history_mode = record
                .pointer("/payload/history_mode")
                .and_then(Value::as_str)
                .map(parse_codex_history_mode)
                .transpose()?
                .unwrap_or(CodexHistoryMode::Legacy);
            return Ok(Some(CodexSessionMetadata {
                id,
                cwd,
                git_branch,
                history_mode,
            }));
        }
    }
    Ok(None)
}

fn parse_codex_history_mode(value: &str) -> Result<CodexHistoryMode> {
    match value {
        "legacy" => Ok(CodexHistoryMode::Legacy),
        "paginated" => Ok(CodexHistoryMode::Paginated),
        other => bail!("unsupported Codex history mode {other:?}"),
    }
}

fn codex_source_is_interactive(source: Option<&Value>) -> bool {
    match source {
        // Older rollouts predate the source field and came from the TUI.
        None => true,
        Some(Value::String(source)) => matches!(source.as_str(), "cli" | "vscode"),
        // Structured sources identify subagents. Other unexpected shapes are
        // not sessions offered by the normal interactive resume picker.
        Some(_) => false,
    }
}

fn codex_native_titles(home: &Path) -> Result<BTreeMap<String, String>> {
    let mut titles = BTreeMap::new();
    let history = home.join("history.jsonl");
    if history.is_file() {
        for line in BufReader::new(fs::File::open(&history)?).lines() {
            let record: Value = serde_json::from_str(&line?)?;
            if let (Some(session_id), Some(text)) = (
                record.get("session_id").and_then(Value::as_str),
                record.get("text").and_then(Value::as_str),
            ) && !text.trim().is_empty()
            {
                titles
                    .entry(session_id.to_owned())
                    .or_insert_with(|| single_line_title(text));
            }
        }
    }
    let index = home.join("session_index.jsonl");
    if index.is_file() {
        for line in BufReader::new(fs::File::open(&index)?).lines() {
            let record: Value = serde_json::from_str(&line?)?;
            if let (Some(session_id), Some(title)) = (
                record.get("id").and_then(Value::as_str),
                record.get("thread_name").and_then(Value::as_str),
            ) && !title.trim().is_empty()
            {
                titles.insert(session_id.to_owned(), single_line_title(title));
            }
        }
    }
    Ok(titles)
}

fn single_line_title(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Locate a Kimi session directory. Its on-disk `session_<uuid>` name is the
/// native identifier required by Kimi ACP's `session/load`.
pub fn locate_kimi_session(
    home: &Path,
    selection: &KimiSessionSelection,
) -> Result<LocatedKimiSession> {
    let candidates = list_kimi_sessions(home)?;
    let sessions = home.join("sessions");
    match selection {
        KimiSessionSelection::NativeSessionId(native_session_id) => candidates
            .into_iter()
            .find(|candidate| candidate.native_session_id == *native_session_id)
            .with_context(|| {
                format!(
                    "Kimi session {native_session_id:?} was not found under {}",
                    sessions.display()
                )
            }),
        KimiSessionSelection::Latest => candidates
            .into_iter()
            .next()
            .context("no Kimi session directories were found"),
    }
}

/// List native Kimi sessions newest first.
pub fn list_kimi_sessions(home: &Path) -> Result<Vec<LocatedKimiSession>> {
    let mut sessions = Vec::new();
    scan_kimi_sessions(home, |progress| {
        if let Some(session) = progress.session {
            sessions.push(session);
        }
    })?;
    Ok(sessions)
}

/// Scan native Kimi sessions newest first, reporting after every candidate directory.
pub fn scan_kimi_sessions(
    home: &Path,
    mut report: impl FnMut(SessionScanProgress<LocatedKimiSession>),
) -> Result<()> {
    let sessions = home.join("sessions");
    ensure!(
        sessions.is_dir(),
        "Kimi sessions directory is missing: {}",
        sessions.display()
    );
    let mut candidates = kimi_indexed_candidates(home, &sessions)?;
    candidates.sort_by(|left, right| {
        right
            .modified_at
            .cmp(&left.modified_at)
            .then_with(|| right.session_path.cmp(&left.session_path))
    });
    let total = candidates.len();
    report(SessionScanProgress {
        scanned: 0,
        total,
        session: None,
    });
    for (index, candidate) in candidates.into_iter().enumerate() {
        let size_bytes = directory_size(&candidate.session_path)?;
        let native_session_id = candidate.native_session_id;
        let session = LocatedKimiSession {
            title: candidate.title,
            native_session_id,
            session_path: candidate.session_path,
            modified_at: candidate.modified_at,
            git_branch: git_branch_or_head(&candidate.cwd),
            size_bytes,
            cwd: candidate.cwd,
        };
        report(SessionScanProgress {
            scanned: index + 1,
            total,
            session: Some(session),
        });
    }
    Ok(())
}

fn kimi_indexed_candidates(home: &Path, sessions: &Path) -> Result<Vec<KimiScanCandidate>> {
    let index_path = home.join("session_index.jsonl");
    if !index_path.is_file() {
        return Ok(Vec::new());
    }

    let mut indexed = BTreeMap::<String, (PathBuf, PathBuf)>::new();
    for line in BufReader::new(fs::File::open(&index_path)?).lines() {
        let Ok(record) = serde_json::from_str::<Value>(&line?) else {
            continue;
        };
        let Some(session_id) = record
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|session_id| !session_id.is_empty())
        else {
            continue;
        };
        if record.get("deleted").and_then(Value::as_bool) == Some(true) {
            indexed.remove(session_id);
            continue;
        }
        let (Some(session_path), Some(work_dir)) = (
            record
                .get("sessionDir")
                .and_then(Value::as_str)
                .map(PathBuf::from),
            record
                .get("workDir")
                .and_then(Value::as_str)
                .map(PathBuf::from),
        ) else {
            continue;
        };
        if validate_id("Kimi session", session_id).is_err()
            || !session_path.is_absolute()
            || session_path.file_name().and_then(|name| name.to_str()) != Some(session_id)
        {
            continue;
        }
        indexed.insert(session_id.to_owned(), (session_path, work_dir));
    }

    let sessions = sessions.canonicalize()?;
    let mut candidates = Vec::new();
    for (native_session_id, (session_path, indexed_work_dir)) in indexed {
        let Ok(metadata) = fs::symlink_metadata(&session_path) else {
            continue;
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            continue;
        }
        let Ok(canonical_session_path) = session_path.canonicalize() else {
            continue;
        };
        if !canonical_session_path.starts_with(&sessions) {
            continue;
        }
        let Some((title, cwd, archived)) =
            kimi_state_listing_metadata(&canonical_session_path, &indexed_work_dir)?
        else {
            continue;
        };
        if archived {
            continue;
        }
        candidates.push(KimiScanCandidate {
            native_session_id,
            modified_at: kimi_session_modified_at(&canonical_session_path, &metadata),
            session_path: canonical_session_path,
            title,
            cwd,
        });
    }
    Ok(candidates)
}

fn kimi_state_listing_metadata(
    session_path: &Path,
    indexed_work_dir: &Path,
) -> Result<Option<(String, PathBuf, bool)>> {
    let state_path = session_path.join("state.json");
    let state = if state_path.is_file() {
        match serde_json::from_slice::<Value>(&fs::read(&state_path)?) {
            Ok(state) => state,
            Err(_) => return Ok(None),
        }
    } else {
        Value::Object(Default::default())
    };
    let string = |key: &str| {
        state
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
    };
    let title = if state.get("isCustomTitle").is_some_and(Value::is_boolean) {
        string("title")
    } else {
        string("customTitle").or_else(|| string("title"))
    }
    .map(single_line_title);
    let cwd = string("workDir")
        .or_else(|| string("cwd"))
        .map(PathBuf::from)
        .filter(|cwd| cwd.is_absolute())
        .or_else(|| {
            indexed_work_dir
                .is_absolute()
                .then(|| indexed_work_dir.to_path_buf())
        })
        .unwrap_or_default();
    let title = title.unwrap_or_else(|| {
        session_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("Untitled session")
            .to_owned()
    });
    Ok(Some((
        title,
        cwd,
        state
            .get("archived")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    )))
}

fn kimi_session_modified_at(session_path: &Path, metadata: &fs::Metadata) -> SystemTime {
    let mut modified_at = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let mut consider = |path: &Path| {
        if let Ok(modified) = fs::metadata(path).and_then(|metadata| metadata.modified()) {
            modified_at = modified_at.max(modified);
        }
    };
    consider(&session_path.join("state.json"));
    consider(&session_path.join("wire.jsonl"));
    let agents = session_path.join("agents");
    if let Ok(entries) = fs::read_dir(agents) {
        for entry in entries.flatten() {
            consider(&entry.path().join("wire.jsonl"));
        }
    }
    modified_at
}

fn select_jsonl_session(
    candidates: Vec<LocatedCodexSession>,
    selection: &CodexSessionSelection,
    harness: &str,
) -> Result<LocatedCodexSession> {
    match selection {
        CodexSessionSelection::NativeSessionId(native_session_id) => {
            validate_id(&format!("{harness} session"), native_session_id)?;
            candidates
                .into_iter()
                .filter(|candidate| candidate.native_session_id == *native_session_id)
                .max_by(|left, right| {
                    left.modified_at
                        .cmp(&right.modified_at)
                        .then_with(|| left.jsonl_path.cmp(&right.jsonl_path))
                })
                .with_context(|| format!("{harness} session {native_session_id:?} was not found"))
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
    let candidates = list_claude_sessions(home)?;
    let projects = home.join("projects");
    match selection {
        ClaudeSessionSelection::NativeSessionId(native_session_id) => {
            validate_id("Claude session", native_session_id)?;
            let mut matches = candidates
                .into_iter()
                .filter(|candidate| candidate.native_session_id == *native_session_id)
                .collect::<Vec<_>>();
            if matches.is_empty() {
                matches = locate_unlisted_claude_sessions(home, native_session_id)?;
            }
            match matches.len() {
                0 => bail!(
                    "Claude session {native_session_id:?} was not found under {}",
                    projects.display()
                ),
                1 => Ok(matches.remove(0)),
                _ => bail!(
                    "Claude session {native_session_id:?} occurs in multiple project directories"
                ),
            }
        }
        ClaudeSessionSelection::Latest => candidates
            .into_iter()
            .next()
            .context("no Claude session JSONL files were found"),
    }
}

fn locate_unlisted_claude_sessions(
    home: &Path,
    native_session_id: &str,
) -> Result<Vec<LocatedClaudeSession>> {
    let projects = home.join("projects");
    let mut matches = Vec::new();
    for project in fs::read_dir(&projects)? {
        let project = project?;
        let project_path = project.path();
        let project_metadata = fs::symlink_metadata(&project_path)?;
        if project_metadata.file_type().is_symlink() || !project_metadata.is_dir() {
            continue;
        }
        let path = project_path.join(format!("{native_session_id}.jsonl"));
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            continue;
        }
        let Some((title, cwd, git_branch)) = claude_native_metadata(&path)? else {
            continue;
        };
        matches.push(LocatedClaudeSession {
            native_session_id: native_session_id.to_owned(),
            jsonl_path: path,
            modified_at: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            title,
            cwd,
            git_branch,
            size_bytes: metadata.len(),
        });
    }
    Ok(matches)
}

/// List native Claude sessions newest first.
pub fn list_claude_sessions(home: &Path) -> Result<Vec<LocatedClaudeSession>> {
    let mut sessions = Vec::new();
    scan_claude_sessions(home, |progress| {
        if let Some(session) = progress.session {
            sessions.push(session);
        }
    })?;
    Ok(sessions)
}

/// Scan native Claude sessions newest first, reporting after every candidate file.
pub fn scan_claude_sessions(
    home: &Path,
    mut report: impl FnMut(SessionScanProgress<LocatedClaudeSession>),
) -> Result<()> {
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
            candidates.push(FileScanCandidate {
                path,
                modified_at: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                size_bytes: metadata.len(),
            });
        }
    }

    candidates.sort_by(|left, right| {
        right
            .modified_at
            .cmp(&left.modified_at)
            .then_with(|| right.path.cmp(&left.path))
    });
    let total = candidates.len();
    report(SessionScanProgress {
        scanned: 0,
        total,
        session: None,
    });
    let mut visible = 0_usize;
    for (index, candidate) in candidates.into_iter().enumerate() {
        if visible == 50 {
            report(SessionScanProgress {
                scanned: index + 1,
                total,
                session: None,
            });
            continue;
        }
        let session_id = candidate
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(".jsonl"))
            .expect("Claude candidates were validated during enumeration")
            .to_owned();
        let metadata = match claude_native_metadata(&candidate.path) {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                report(SessionScanProgress {
                    scanned: index + 1,
                    total,
                    session: None,
                });
                continue;
            }
            Err(_) => (session_id.clone(), PathBuf::new(), "HEAD".to_owned()),
        };
        let (title, cwd, git_branch) = metadata;
        visible += 1;
        report(SessionScanProgress {
            scanned: index + 1,
            total,
            session: Some(LocatedClaudeSession {
                native_session_id: session_id,
                jsonl_path: candidate.path,
                modified_at: candidate.modified_at,
                title,
                cwd,
                git_branch,
                size_bytes: candidate.size_bytes,
            }),
        });
    }
    Ok(())
}

fn claude_native_metadata(path: &Path) -> Result<Option<(String, PathBuf, String)>> {
    let mut custom_title = None;
    let mut agent_name = None;
    let mut ai_title = None;
    let mut fallback_title = None;
    let mut cwd = None;
    let mut git_branch = None;
    let mut entrypoint = None;
    let mut filtered = false;
    for line in BufReader::new(fs::File::open(path)?).lines() {
        let record: Value = serde_json::from_str(&line?)?;
        if record
            .get("isSidechain")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || record
                .get("teamName")
                .and_then(Value::as_str)
                .is_some_and(|name| !name.trim().is_empty())
            || record.get("sessionKind").and_then(Value::as_str) == Some("daemon-worker")
        {
            filtered = true;
        }
        if entrypoint.is_none() {
            entrypoint = record
                .get("entrypoint")
                .and_then(Value::as_str)
                .filter(|entrypoint| !entrypoint.trim().is_empty())
                .map(str::to_owned);
        }
        if cwd.is_none() {
            cwd = record
                .get("cwd")
                .and_then(Value::as_str)
                .filter(|cwd| !cwd.trim().is_empty())
                .map(PathBuf::from);
        }
        if git_branch.is_none() {
            git_branch = record
                .get("gitBranch")
                .and_then(Value::as_str)
                .filter(|branch| !branch.trim().is_empty())
                .map(str::to_owned);
        }
        match record.get("type").and_then(Value::as_str) {
            Some("custom-title") => {
                if let Some(native_title) = record
                    .get("customTitle")
                    .and_then(Value::as_str)
                    .filter(|title| !title.trim().is_empty())
                {
                    custom_title = Some(single_line_title(native_title));
                }
            }
            Some("ai-title") => {
                if let Some(native_title) = record
                    .get("aiTitle")
                    .and_then(Value::as_str)
                    .filter(|title| !title.trim().is_empty())
                {
                    ai_title = Some(single_line_title(native_title));
                }
            }
            Some("agent-name") => {
                agent_name = record
                    .get("agentName")
                    .and_then(Value::as_str)
                    .filter(|title| !title.trim().is_empty())
                    .map(single_line_title);
            }
            Some("user") if fallback_title.is_none() => {
                let content = record.pointer("/message/content").and_then(Value::as_str);
                if content
                    .is_some_and(|content| content.contains("<command-name>/loop</command-name>"))
                {
                    filtered = true;
                }
                fallback_title = content
                    .filter(|title| !title.trim().is_empty())
                    .map(single_line_title);
            }
            _ => {}
        }
    }
    // Claude's native resume picker is for interactive CLI conversations. In
    // particular, its print/SDK entrypoints include the tiny rollouts created
    // by `claude -p /usage`, which must not displace real sessions here.
    if filtered || entrypoint.as_deref().is_some_and(|value| value != "cli") {
        return Ok(None);
    }
    let cwd = cwd.with_context(|| format!("Claude session {} has no cwd", path.display()))?;
    Ok(Some((
        custom_title
            .or(agent_name)
            .or(ai_title)
            .or(fallback_title)
            .unwrap_or_else(|| "Untitled session".into()),
        cwd,
        git_branch.unwrap_or_else(|| "HEAD".into()),
    )))
}

fn git_branch_or_head(cwd: &Path) -> String {
    if cwd.as_os_str().is_empty() {
        return "HEAD".into();
    }
    git_optional_text(cwd, ["branch", "--show-current"])
        .ok()
        .flatten()
        .filter(|branch| !branch.is_empty())
        .unwrap_or_else(|| "HEAD".into())
}

fn directory_size(path: &Path) -> Result<u64> {
    let mut size = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.is_file() {
            size = size.saturating_add(metadata.len());
        } else if metadata.is_dir() && !metadata.file_type().is_symlink() {
            size = size.saturating_add(directory_size(&entry.path())?);
        }
    }
    Ok(size)
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
        let recorded_at_ms = native_recorded_at_ms(&record);
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
                    recorded_at_ms,
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
                        recorded_at_ms,
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
                    push_event(&mut events, recorded_at_ms, WorkerEvent::TurnCompleted);
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
    finalize_import_event_times(&mut events, path)?;
    let edited_paths = claude_edited_paths(path)?;
    Ok(ClaudeTranscript {
        cwd,
        edited_paths,
        events,
    })
}

/// Project a Codex rollout into the canonical transcript used by Hel chat.
pub fn read_codex_transcript(path: &Path) -> Result<CodexTranscript> {
    let body = fs::read_to_string(path)
        .with_context(|| format!("read Codex session {}", path.display()))?;
    let mut cwd = None;
    let mut history_mode = None;
    let mut events = Vec::new();
    let mut edited_paths = BTreeSet::new();
    let mut saw_user = false;
    for (index, line) in body.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(line).with_context(|| {
            format!("parse Codex session {} line {}", path.display(), index + 1)
        })?;
        let recorded_at_ms = native_recorded_at_ms(&record);
        if record.get("type").and_then(Value::as_str) == Some("session_meta") {
            if cwd.is_none() {
                cwd = record
                    .pointer("/payload/cwd")
                    .and_then(Value::as_str)
                    .filter(|cwd| !cwd.trim().is_empty())
                    .map(PathBuf::from);
            }
            if history_mode.is_none() {
                history_mode = Some(
                    record
                        .pointer("/payload/history_mode")
                        .and_then(Value::as_str)
                        .map(parse_codex_history_mode)
                        .transpose()?
                        .unwrap_or(CodexHistoryMode::Legacy),
                );
            }
            continue;
        }
        if record.get("type").and_then(Value::as_str) != Some("event_msg") {
            continue;
        }
        if record.pointer("/payload/type").and_then(Value::as_str) == Some("item_completed")
            && record.pointer("/payload/item/type").and_then(Value::as_str) == Some("FileChange")
            && record
                .pointer("/payload/item/status")
                .and_then(Value::as_str)
                == Some("completed")
            && let Some(changes) = record
                .pointer("/payload/item/changes")
                .and_then(Value::as_object)
        {
            edited_paths.extend(changes.keys().map(PathBuf::from));
        }
        match record.pointer("/payload/type").and_then(Value::as_str) {
            Some("item_completed")
                if record.pointer("/payload/item/type").and_then(Value::as_str)
                    == Some("UserMessage") =>
            {
                let Some(text) = codex_completed_item_text(&record) else {
                    continue;
                };
                finish_imported_turn(&mut events, None);
                let request_id = format!("import-{}", events.len() + 1);
                push_event(
                    &mut events,
                    recorded_at_ms,
                    WorkerEvent::PromptAccepted {
                        request_id,
                        text: text.to_owned(),
                        attachments: Vec::new(),
                    },
                );
                saw_user = true;
            }
            Some("item_completed")
                if record.pointer("/payload/item/type").and_then(Value::as_str)
                    == Some("AgentMessage") =>
            {
                let Some(text) = codex_completed_item_text(&record) else {
                    continue;
                };
                push_event(
                    &mut events,
                    recorded_at_ms,
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
            Some("turn_complete" | "turn_aborted") => {
                finish_imported_turn(&mut events, recorded_at_ms)
            }
            _ => {}
        }
    }
    ensure!(
        history_mode == Some(CodexHistoryMode::Paginated),
        "{CODEX_LEGACY_IMPORT_ISSUE}"
    );
    ensure!(
        saw_user,
        "Codex paginated session contains no importable user messages"
    );
    finish_imported_turn(&mut events, None);
    let cwd = cwd.context("Codex session does not declare its original cwd")?;
    ensure!(
        cwd.is_absolute(),
        "Codex session cwd is not absolute: {}",
        cwd.display()
    );
    finalize_import_event_times(&mut events, path)?;
    Ok(CodexTranscript {
        cwd,
        edited_paths: edited_paths.into_iter().collect(),
        events,
    })
}

fn codex_completed_item_text(record: &Value) -> Option<String> {
    let parts = record
        .pointer("/payload/item/content")?
        .as_array()?
        .iter()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| parts.join("\n"))
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
        let recorded_at_ms = native_recorded_at_ms(&record);
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
                finish_imported_turn(&mut events, None);
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
                        recorded_at_ms,
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
                    recorded_at_ms,
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
    finish_imported_turn(&mut events, None);
    finalize_import_event_times(&mut events, &wire_path)?;
    let edited_paths = kimi_edited_paths(session_path)?;
    Ok(KimiTranscript {
        cwd,
        edited_paths,
        events,
    })
}

fn claude_edited_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let mut files = vec![path.to_path_buf()];
    if let (Some(parent), Some(session_id)) = (
        path.parent(),
        path.file_stem().and_then(|value| value.to_str()),
    ) {
        let subagents = parent.join(session_id).join("subagents");
        if subagents.is_dir() {
            collect_files_named(&subagents, "jsonl", &mut files)?;
        }
    }
    let mut edited = BTreeSet::new();
    for file in files {
        let body = fs::read_to_string(&file)?;
        let mut calls = BTreeMap::<String, PathBuf>::new();
        let mut completed = BTreeSet::new();
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            let record: Value = serde_json::from_str(line)?;
            if record.get("type").and_then(Value::as_str) == Some("file-history-delta") {
                let Some(tracking) = record.get("trackingPath").and_then(Value::as_str) else {
                    continue;
                };
                let tracking = PathBuf::from(tracking);
                let path = if tracking.is_absolute() {
                    tracking
                } else if let Some(parent) = record
                    .pointer("/backup/realParentDir")
                    .and_then(Value::as_str)
                {
                    PathBuf::from(parent).join(
                        tracking
                            .file_name()
                            .expect("non-empty tracking path has a file name"),
                    )
                } else {
                    tracking
                };
                edited.insert(path);
            }
            if record.get("type").and_then(Value::as_str) == Some("assistant") {
                for block in record
                    .pointer("/message/content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if block.get("type").and_then(Value::as_str) != Some("tool_use")
                        || !matches!(
                            block.get("name").and_then(Value::as_str),
                            Some("Edit" | "Write" | "NotebookEdit")
                        )
                    {
                        continue;
                    }
                    let Some(id) = block.get("id").and_then(Value::as_str) else {
                        continue;
                    };
                    if let Some(path) = block
                        .pointer("/input/file_path")
                        .or_else(|| block.pointer("/input/notebook_path"))
                        .or_else(|| block.pointer("/input/path"))
                        .and_then(Value::as_str)
                    {
                        calls.insert(id.to_owned(), PathBuf::from(path));
                    }
                }
            }
            if record.get("type").and_then(Value::as_str) == Some("user") {
                for block in record
                    .pointer("/message/content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if block.get("type").and_then(Value::as_str) == Some("tool_result")
                        && block.get("is_error").and_then(Value::as_bool) != Some(true)
                        && let Some(id) = block.get("tool_use_id").and_then(Value::as_str)
                    {
                        completed.insert(id.to_owned());
                    }
                }
            }
        }
        edited.extend(
            calls
                .into_iter()
                .filter(|(id, _)| completed.contains(id))
                .map(|(_, path)| path),
        );
    }
    Ok(edited.into_iter().collect())
}

fn kimi_edited_paths(session_path: &Path) -> Result<Vec<PathBuf>> {
    let agents = session_path.join("agents");
    if !agents.is_dir() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    collect_files_named(&agents, "jsonl", &mut files)?;
    let mut edited = BTreeSet::new();
    for file in files {
        let body = fs::read_to_string(file)?;
        let mut calls = BTreeMap::<String, PathBuf>::new();
        let mut completed = BTreeSet::new();
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            let record: Value = serde_json::from_str(line)?;
            if record.get("type").and_then(Value::as_str) != Some("context.append_loop_event") {
                continue;
            }
            let event = &record["event"];
            if event.get("type").and_then(Value::as_str) == Some("tool.call")
                && matches!(
                    event.get("name").and_then(Value::as_str),
                    Some("Edit" | "Write")
                )
                && let (Some(id), Some(path)) = (
                    event.get("toolCallId").and_then(Value::as_str),
                    event
                        .pointer("/args/path")
                        .or_else(|| event.pointer("/args/file_path"))
                        .and_then(Value::as_str),
                )
            {
                calls.insert(id.to_owned(), PathBuf::from(path));
            }
            if event.get("type").and_then(Value::as_str) == Some("tool.result")
                && event.pointer("/result/isError").and_then(Value::as_bool) != Some(true)
                && let Some(id) = event.get("toolCallId").and_then(Value::as_str)
            {
                completed.insert(id.to_owned());
            }
        }
        edited.extend(
            calls
                .into_iter()
                .filter(|(id, _)| completed.contains(id))
                .map(|(_, path)| path),
        );
    }
    Ok(edited.into_iter().collect())
}

fn collect_files_named(root: &Path, extension: &str, output: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            collect_files_named(&path, extension, output)?;
        } else if metadata.is_file()
            && path.extension().and_then(|value| value.to_str()) == Some(extension)
        {
            output.push(path);
        }
    }
    Ok(())
}

fn finish_imported_turn(events: &mut Vec<SequencedEvent>, recorded_at_ms: Option<i64>) {
    if !events.is_empty()
        && !matches!(
            events.last().map(|event| &event.event),
            Some(WorkerEvent::TurnCompleted)
        )
    {
        push_event(events, recorded_at_ms, WorkerEvent::TurnCompleted);
    }
}

fn push_event(events: &mut Vec<SequencedEvent>, recorded_at_ms: Option<i64>, event: WorkerEvent) {
    events.push(SequencedEvent {
        seq: events.len() as u64 + 1,
        recorded_at_ms,
        request_id: None,
        event,
    });
}

fn native_recorded_at_ms(record: &Value) -> Option<i64> {
    record
        .get("timestamp")
        .or_else(|| record.get("time"))
        .and_then(Value::as_str)
        .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
        .map(|timestamp| timestamp.timestamp_millis())
}

/// Native streams predate Hel's durable event clock in some harness versions.
/// Preserve their record timestamps when available; otherwise use the source
/// artifact's modification time. Clamping regressions keeps the imported
/// sequence and its activity watermark monotonic even if the native clock
/// moved backwards while the session was being recorded.
fn finalize_import_event_times(events: &mut [SequencedEvent], source_path: &Path) -> Result<()> {
    let Some(first) = events.first() else {
        return Ok(());
    };
    let mut last_recorded_at_ms = match events.iter().find_map(|event| event.recorded_at_ms) {
        Some(recorded_at_ms) => recorded_at_ms,
        None => DateTime::<Utc>::from(
            fs::metadata(source_path)
                .with_context(|| format!("stat import source {}", source_path.display()))?
                .modified()
                .with_context(|| format!("read import source mtime {}", source_path.display()))?,
        )
        .timestamp_millis(),
    };
    last_recorded_at_ms = first
        .recorded_at_ms
        .unwrap_or(last_recorded_at_ms)
        .max(last_recorded_at_ms);
    for event in events {
        last_recorded_at_ms = event
            .recorded_at_ms
            .unwrap_or(last_recorded_at_ms)
            .max(last_recorded_at_ms);
        event.recorded_at_ms = Some(last_recorded_at_ms);
    }
    Ok(())
}

pub fn session_edit_targets(
    transcript: &ClaudeTranscript,
    profile_home: &Path,
) -> Result<SessionEditTargets> {
    let profile_home =
        fs::canonicalize(profile_home).unwrap_or_else(|_| profile_home.to_path_buf());
    let mut paths = transcript
        .edited_paths
        .iter()
        .map(|path| {
            if path.is_absolute() {
                path.clone()
            } else {
                transcript.cwd.join(path)
            }
        })
        .filter(|path| {
            let comparable = canonicalize_existing_ancestor(path);
            !comparable.starts_with(&profile_home)
        })
        .collect::<Vec<_>>();
    if paths.is_empty() {
        paths.push(transcript.cwd.clone());
    }

    let cwd_root = git_root_for_path(&transcript.cwd)?.with_context(|| {
        format!(
            "session cwd is not in a usable Git worktree: {}",
            transcript.cwd.display()
        )
    })?;
    let mut git_roots = BTreeSet::from([cwd_root]);
    let mut non_git_dirs = BTreeSet::new();
    for path in paths {
        if let Some(root) = git_root_for_path(&path)? {
            git_roots.insert(root);
        } else {
            non_git_dirs.insert(edited_directory(&path));
        }
    }
    Ok(SessionEditTargets {
        git_roots: git_roots.into_iter().collect(),
        non_git_dirs: non_git_dirs.into_iter().collect(),
    })
}

fn canonicalize_existing_ancestor(path: &Path) -> PathBuf {
    let mut existing = path;
    let mut suffix = Vec::new();
    loop {
        if let Ok(mut canonical) = fs::canonicalize(existing) {
            for component in suffix.iter().rev() {
                canonical.push(component);
            }
            return canonical;
        }
        let Some(name) = existing.file_name() else {
            return path.to_path_buf();
        };
        suffix.push(name.to_os_string());
        let Some(parent) = existing.parent() else {
            return path.to_path_buf();
        };
        existing = parent;
    }
}

fn edited_directory(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent().unwrap_or(path).to_path_buf()
    }
}

fn git_root_for_path(path: &Path) -> Result<Option<PathBuf>> {
    let mut probe = edited_directory(path);
    while !probe.is_dir() {
        if !probe.pop() {
            return Ok(None);
        }
    }
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(&probe)
        .output()
        .with_context(|| format!("start git in {}", probe.display()))?;
    if !output.status.success() {
        return Ok(None);
    }
    let root = String::from_utf8(output.stdout).context("decode Git repository root")?;
    let root = PathBuf::from(root.trim());
    Ok(Some(fs::canonicalize(&root).unwrap_or(root)))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum RepositoryIdentity {
    Github(String, String),
    Local(PathBuf),
}

fn root_identity(root: &Path) -> Result<RepositoryIdentity> {
    let origin = git_optional_text(root, ["remote", "get-url", "origin"])?;
    if let Some(github) = origin.as_deref().and_then(github_repository_from_origin) {
        return Ok(RepositoryIdentity::Github(
            github.owner.to_ascii_lowercase(),
            github.repository.to_ascii_lowercase(),
        ));
    }
    // A linked worktree shares the identity of its main working tree.
    let root = main_worktree_root(root)?;
    Ok(RepositoryIdentity::Local(
        fs::canonicalize(&root).unwrap_or(root),
    ))
}

fn configured_repository_identity(repository: &ProjectRepository) -> Option<RepositoryIdentity> {
    if let Some(source) = repository.github.as_deref() {
        let github = github_repository_from_origin(source)?;
        return Some(RepositoryIdentity::Github(
            github.owner.to_ascii_lowercase(),
            github.repository.to_ascii_lowercase(),
        ));
    }
    repository.local.as_ref().map(|path| {
        RepositoryIdentity::Local(fs::canonicalize(path).unwrap_or_else(|_| path.clone()))
    })
}

/// Reuse an exact configured bundle or synthesize one from all detected roots.
pub fn resolve_bundle(
    config: &HelConfig,
    cwd: &Path,
    targets: &SessionEditTargets,
    requested_bundle: Option<&str>,
) -> Result<BundleResolution> {
    let cwd_root = git_root_for_path(cwd)?.context("session cwd is not in a Git worktree")?;
    // A linked worktree stands in for its main repository, so bundles are named
    // after and point at the main working tree.
    let cwd_root = main_worktree_root(&cwd_root)?;
    let primary_identity = root_identity(&cwd_root)?;
    let detected = targets
        .git_roots
        .iter()
        .map(|root| root_identity(root))
        .collect::<Result<BTreeSet<_>>>()?;

    if let Some(bundle_id) = requested_bundle {
        let bundle = config
            .bundles
            .get(bundle_id)
            .with_context(|| format!("unknown bundle {bundle_id:?}"))?;
        ensure!(
            bundle_matches(bundle, &detected, &primary_identity),
            "bundle {bundle_id:?} does not exactly match the session's edited Git roots and cwd primary repository"
        );
        return Ok(BundleResolution::Existing(bundle_id.to_owned()));
    }
    if let Some(id) = config.bundles.iter().find_map(|(id, bundle)| {
        bundle_matches(bundle, &detected, &primary_identity).then(|| id.clone())
    }) {
        return Ok(BundleResolution::Existing(id));
    }

    let primary_name = cwd_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("repository");
    let bundle_id = unique_bundle_id(config, &setup_style_id(primary_name));
    let mut used_ids = BTreeSet::new();
    let mut repositories = Vec::new();
    let mut primary_repo = None;
    let mut roots = targets
        .git_roots
        .iter()
        .map(|root| main_worktree_root(root))
        .collect::<Result<Vec<_>>>()?;
    roots.sort_by_key(|root| root != &cwd_root);
    let mut seen = BTreeSet::new();
    roots.retain(|root| seen.insert(root.clone()));
    for root in roots {
        let base = setup_style_id(
            root.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("repository"),
        );
        let mut id = base.clone();
        for suffix in 2_u32.. {
            if used_ids.insert(id.clone()) {
                break;
            }
            id = format!("{base}-{suffix}");
        }
        if root == cwd_root {
            primary_repo = Some(id.clone());
        }
        let origin = git_optional_text(&root, ["remote", "get-url", "origin"])?;
        let github = origin
            .as_deref()
            .and_then(github_repository_from_origin)
            .map(|source| format!("{}/{}", source.owner, source.repository));
        repositories.push(ProjectRepository {
            id: id.clone(),
            local: github.is_none().then_some(root),
            github,
            destination: PathBuf::from(id),
            git_ref: None,
        });
    }
    Ok(BundleResolution::Synthesized {
        id: bundle_id,
        bundle: ProjectBundle {
            primary_repo: primary_repo.context("detected roots omitted the cwd repository")?,
            repositories,
        },
    })
}

fn bundle_matches(
    bundle: &ProjectBundle,
    detected: &BTreeSet<RepositoryIdentity>,
    primary: &RepositoryIdentity,
) -> bool {
    let identities = bundle
        .repositories
        .iter()
        .filter_map(configured_repository_identity)
        .collect::<BTreeSet<_>>();
    identities.len() == bundle.repositories.len()
        && &identities == detected
        && bundle
            .primary()
            .and_then(configured_repository_identity)
            .as_ref()
            == Some(primary)
}

/// Return the matching configured bundle for an origin. It accepts setup's
/// `owner/repository` shorthand as well as normal GitHub remote URLs.
pub fn configured_bundle_for_origin(
    config: &HelConfig,
    origin: &GithubRepository,
) -> Option<String> {
    config.bundles.iter().find_map(|(id, bundle)| {
        let primary = bundle.primary()?;
        let configured = github_repository_from_origin(primary.github.as_deref()?)?;
        same_github_repository(&configured, origin).then(|| id.clone())
    })
}

pub fn configured_bundle_for_local(config: &HelConfig, local: &Path) -> Option<String> {
    let local = fs::canonicalize(local).unwrap_or_else(|_| local.to_path_buf());
    config.bundles.iter().find_map(|(id, bundle)| {
        let configured = bundle.primary()?.local.as_ref()?;
        let configured = fs::canonicalize(configured).unwrap_or_else(|_| configured.to_path_buf());
        (configured == local).then(|| id.clone())
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
    import_claude_session_inner(config, state, request, None)
}

pub fn import_claude_session_with_control(
    config: &HelConfig,
    state: &mut HelState,
    request: ClaudeImportRequest<'_>,
    control: &ImportControl<'_>,
) -> Result<ImportedClaudeSession> {
    import_claude_session_inner(config, state, request, Some(control))
}

fn import_claude_session_inner(
    config: &HelConfig,
    state: &mut HelState,
    request: ClaudeImportRequest<'_>,
    control: Option<&ImportControl<'_>>,
) -> Result<ImportedClaudeSession> {
    let ClaudeImportRequest {
        claude_home,
        source,
        transcript,
        bundle_id,
        profile_id,
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
            .unwrap_or_else(|| format!("Imported Claude session {}", source.native_session_id)),
    };
    let targets = session_edit_targets(transcript, claude_home)?;
    let repositories = collect_local_repositories(bundle, &targets.git_roots, control)?;
    let native_artifacts = collect_native_artifacts(
        HarnessKind::Claude,
        claude_home,
        &source.native_session_id,
        false,
    )?;
    let session_id = new_session_id()?;
    let canonical_session =
        canonical_import_session(&session_id, &transcript.events, &source.jsonl_path)?;
    let timestamp = timestamp();
    let profile_id = import_profile_id(config, profile_id, HarnessKind::Claude, claude_home)?;
    let target_id = default_import_target_id(config);
    let archive_path = archive_directory.join(format!("{session_id}.hel.zip"));
    if let Some(control) = control {
        control.report(ImportArchiveProgress::WritingArchive)?;
    }
    let verified = write_archive_atomic(
        &archive_path,
        &ArchiveInput {
            session: crate::hel_archive::SessionManifest {
                id: session_id.clone(),
                title: title.clone(),
                harness_kind: HarnessKind::Claude,
                profile_id: profile_id.clone(),
                native_session_id: source.native_session_id.clone(),
                created_at: timestamp.clone(),
                checkpointed_at: timestamp.clone(),
                hel_version: env!("CARGO_PKG_VERSION").into(),
                relay_version: env!("CARGO_PKG_VERSION").into(),
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
            canonical_session,
            native_artifacts,
            repositories,
        },
    )?;
    if let Some(control) = control
        && let Err(error) = control.check_cancelled()
    {
        let _ = fs::remove_file(&archive_path);
        return Err(error);
    }
    let checkpoint = CheckpointMetadata {
        archive_path: archive_path.clone(),
        sha256: verified.archive_sha256,
        created_at: timestamp.clone(),
        event_frontier: transcript.events.last().map_or(0, |event| event.seq),
    };
    state.sessions.insert(
        session_id.clone(),
        SessionRecord {
            id: session_id.clone(),
            title,
            harness_kind: HarnessKind::Claude,
            last_profile: profile_id,
            bundle_id: bundle_id.to_owned(),
            project_directory: None,
            managed_worktree: None,
            target_template_id: target_id,
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Archived,
            target: None,
            native_session_id: Some(source.native_session_id.clone()),
            acp_session_title: None,
            session_title_override,
            created_at: timestamp.clone(),
            updated_at: timestamp,
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: Some(checkpoint),
        },
    );
    Ok(ImportedClaudeSession {
        session_id,
        native_session_id: source.native_session_id.clone(),
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
    import_codex_session_inner(config, state, request, None)
}

pub fn import_codex_session_with_control(
    config: &HelConfig,
    state: &mut HelState,
    request: CodexImportRequest<'_>,
    control: &ImportControl<'_>,
) -> Result<ImportedCodexSession> {
    import_codex_session_inner(config, state, request, Some(control))
}

fn import_codex_session_inner(
    config: &HelConfig,
    state: &mut HelState,
    request: CodexImportRequest<'_>,
    control: Option<&ImportControl<'_>>,
) -> Result<ImportedCodexSession> {
    let CodexImportRequest {
        codex_home,
        source,
        transcript,
        bundle_id,
        profile_id,
        title,
        archive_directory,
    } = request;
    import_native_session(
        config,
        state,
        NativeImportRequest {
            harness: HarnessKind::Codex,
            harness_home: codex_home,
            native_session_id: &source.native_session_id,
            source_path: &source.jsonl_path,
            transcript,
            bundle_id,
            profile_id,
            title,
            archive_directory,
        },
        control,
    )
}

pub fn import_kimi_session(
    config: &HelConfig,
    state: &mut HelState,
    request: KimiImportRequest<'_>,
) -> Result<ImportedKimiSession> {
    import_kimi_session_inner(config, state, request, None)
}

pub fn import_kimi_session_with_control(
    config: &HelConfig,
    state: &mut HelState,
    request: KimiImportRequest<'_>,
    control: &ImportControl<'_>,
) -> Result<ImportedKimiSession> {
    import_kimi_session_inner(config, state, request, Some(control))
}

fn import_kimi_session_inner(
    config: &HelConfig,
    state: &mut HelState,
    request: KimiImportRequest<'_>,
    control: Option<&ImportControl<'_>>,
) -> Result<ImportedKimiSession> {
    let KimiImportRequest {
        kimi_home,
        source,
        transcript,
        bundle_id,
        profile_id,
        title,
        archive_directory,
    } = request;
    import_native_session(
        config,
        state,
        NativeImportRequest {
            harness: HarnessKind::Kimi,
            harness_home: kimi_home,
            native_session_id: &source.native_session_id,
            source_path: &source.session_path,
            transcript,
            bundle_id,
            profile_id,
            title,
            archive_directory,
        },
        control,
    )
}

struct NativeImportRequest<'a> {
    harness: HarnessKind,
    harness_home: &'a Path,
    native_session_id: &'a str,
    source_path: &'a Path,
    transcript: &'a ClaudeTranscript,
    bundle_id: &'a str,
    profile_id: Option<&'a str>,
    title: Option<&'a str>,
    archive_directory: &'a Path,
}

fn import_native_session(
    config: &HelConfig,
    state: &mut HelState,
    request: NativeImportRequest<'_>,
    control: Option<&ImportControl<'_>>,
) -> Result<ImportedClaudeSession> {
    let NativeImportRequest {
        harness,
        harness_home,
        native_session_id,
        source_path,
        transcript,
        bundle_id,
        profile_id,
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
                harness.display_name()
            )
        }),
    };
    let targets = session_edit_targets(transcript, harness_home)?;
    let repositories = collect_local_repositories(bundle, &targets.git_roots, control)?;
    let native_artifacts =
        collect_import_native_artifacts(harness, harness_home, native_session_id, source_path)?;
    let session_id = new_session_id()?;
    let canonical_session =
        canonical_import_session(session_id.as_str(), &transcript.events, source_path)?;
    let timestamp = timestamp();
    let profile_id = import_profile_id(config, profile_id, harness, harness_home)?;
    let target_id = default_import_target_id(config);
    let archive_path = archive_directory.join(format!("{session_id}.hel.zip"));
    if let Some(control) = control {
        control.report(ImportArchiveProgress::WritingArchive)?;
    }
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
                relay_version: env!("CARGO_PKG_VERSION").into(),
                adapter_version: "acp-v1".into(),
            },
            target: TargetManifest {
                template_id: target_id.clone(),
                target_kind: "import".into(),
                details: BTreeMap::from([("source".into(), format!("{}-import", harness.id()))]),
            },
            bundle: BundleManifest {
                id: bundle_id.to_owned(),
                primary_repository: bundle.primary_repo.clone(),
            },
            canonical_session,
            native_artifacts,
            repositories,
        },
    )?;
    if let Some(control) = control
        && let Err(error) = control.check_cancelled()
    {
        let _ = fs::remove_file(&archive_path);
        return Err(error);
    }
    let checkpoint = CheckpointMetadata {
        archive_path: archive_path.clone(),
        sha256: verified.archive_sha256,
        created_at: timestamp.clone(),
        event_frontier: transcript.events.last().map_or(0, |event| event.seq),
    };
    state.sessions.insert(
        session_id.clone(),
        SessionRecord {
            id: session_id.clone(),
            title,
            harness_kind: harness,
            last_profile: profile_id,
            bundle_id: bundle_id.to_owned(),
            project_directory: None,
            managed_worktree: None,
            target_template_id: target_id,
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Archived,
            target: None,
            native_session_id: Some(native_session_id.to_owned()),
            acp_session_title: None,
            session_title_override,
            created_at: timestamp.clone(),
            updated_at: timestamp,
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
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

fn default_import_target_id(config: &HelConfig) -> String {
    config
        .targets
        .get_key_value("podman")
        .map(|(id, _)| id)
        .or_else(|| {
            config.targets.iter().find_map(|(id, target)| {
                matches!(target, TargetTemplate::LocalPodman { .. }).then_some(id)
            })
        })
        .or_else(|| config.targets.keys().next())
        .cloned()
        .unwrap_or_else(|| "import".into())
}

fn collect_local_repositories(
    bundle: &ProjectBundle,
    detected_roots: &[PathBuf],
    control: Option<&ImportControl<'_>>,
) -> Result<Vec<crate::hel_archive::RepositorySnapshot>> {
    let detected = detected_roots
        .iter()
        .map(|root| Ok((root_identity(root)?, root.clone())))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let repository_paths = bundle
        .repositories
        .iter()
        .map(|repository| {
            let identity = configured_repository_identity(repository)
                .with_context(|| format!("repository {:?} has no usable source", repository.id))?;
            let path = detected.get(&identity).cloned().with_context(|| {
                format!(
                    "repository {:?} was not detected in the native session",
                    repository.id
                )
            })?;
            Ok((repository.id.clone(), path))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let git = SystemGit;
    let repository_count = bundle.repositories.len();
    bundle
        .repositories
        // Indexed parallel iteration keeps repository and manifest order
        // identical to the configured bundle.
        .par_iter()
        .enumerate()
        .map(|(index, repository)| {
            if let Some(control) = control {
                control.report(ImportArchiveProgress::Repository {
                    current: index + 1,
                    total: repository_count,
                    id: repository.id.clone(),
                })?;
            }
            let path = repository_paths
                .get(&repository.id)
                .expect("repository paths cover the validated bundle")
                .clone();
            ensure!(
                path.is_dir(),
                "local repository {:?} is missing at {}",
                repository.id,
                path.display()
            );
            let history = if repository.is_local() {
                // The local-repository proxy serves committed history and
                // provisioning fetches it, so the archive only has to carry
                // identity and dirty state.
                GitHistoryMode::NoBundle
            } else {
                // Import starts from the common ancestor of the local checkout
                // and the tracked remote, so unpushed commits are included in
                // the committed delta bundle.
                GitHistoryMode::DeltaFrom(import_delta_base(&path)?)
            };
            collect_git_snapshot_with_progress(
                &git,
                &path,
                &GitCollectionSpec {
                    id: repository.id.clone(),
                    relative_destination: repository.destination.clone(),
                    history,
                    origin_override: repository
                        .is_local()
                        .then(|| format!("hel-local:{}", repository.id)),
                },
                control.is_none_or(|control| control.include_untracked),
                &|progress| {
                    let Some(control) = control else {
                        return Ok(());
                    };
                    match progress {
                        GitSnapshotProgress::UntrackedFile {
                            current,
                            total,
                            path,
                        } => control.report(ImportArchiveProgress::UntrackedFile {
                            repository_id: repository.id.clone(),
                            current,
                            total,
                            path,
                        }),
                    }
                },
            )
            .with_context(|| format!("collect local repository {:?}", repository.id))
        })
        .collect()
}

fn canonical_import_session(
    session_id: &str,
    events: &[SequencedEvent],
    source_path: &Path,
) -> Result<crate::hel_archive::CanonicalSessionSnapshot> {
    let mut events = events.to_vec();
    finalize_import_event_times(&mut events, source_path)?;
    let latest = events.last().map_or(0, |event| event.seq);
    let snapshot = WorkerSnapshot::summary(session_id.to_owned(), WorkerPhase::Idle, latest);
    let mut materialized = ChatState::new(&snapshot, &events).materialized_session();
    materialized.session_title = harness_session_title(&events);
    if let Some(last_activity_at_ms) = events.iter().filter_map(|event| event.recorded_at_ms).max()
    {
        materialized.last_activity_at_ms = Some(
            materialized
                .last_activity_at_ms
                .map_or(last_activity_at_ms, |current| {
                    current.max(last_activity_at_ms)
                }),
        );
    }
    canonical_session_from_materialized(&materialized)
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
        .unwrap_or_else(|| format!("{}-import", harness.id()))
}

fn import_profile_id(
    config: &HelConfig,
    requested: Option<&str>,
    harness: HarnessKind,
    home: &Path,
) -> Result<String> {
    let Some(requested) = requested else {
        return Ok(default_profile(config, harness, home));
    };
    let profile = config
        .profiles
        .get(requested)
        .with_context(|| format!("unknown import profile {requested:?}"))?;
    ensure!(
        profile.kind == harness,
        "import profile {requested:?} does not use {harness:?}"
    );
    Ok(requested.to_owned())
}

/// The upstream revision an imported repository deltas from. A repository
/// without remote-tracking refs cannot tell us which ancestry a newly
/// provisioned clone has, and Hel never bundles full history, so it fails here.
fn import_delta_base(path: &Path) -> Result<String> {
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
    upstream.with_context(|| {
        format!(
            "repository {} has no remote-tracking refs to import against; fetch its remote first",
            path.display()
        )
    })
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

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn container_template() -> crate::hel_config::ContainerTemplate {
        crate::hel_config::ContainerTemplate {
            image: "agent-dev:latest".into(),
            platform: None,
            cpus: None,
            memory: None,
            environment: BTreeMap::new(),
        }
    }

    #[test]
    fn imported_sessions_default_to_podman() {
        let mut config = HelConfig::default();
        config.targets.insert(
            "apple".into(),
            TargetTemplate::AppleContainer {
                container: container_template(),
            },
        );
        config.targets.insert(
            "podman".into(),
            TargetTemplate::LocalPodman {
                container: container_template(),
            },
        );

        assert_eq!(default_import_target_id(&config), "podman");
    }

    #[test]
    fn imported_sessions_prefer_a_custom_named_local_podman_target() {
        let mut config = HelConfig::default();
        config.targets.insert(
            "apple".into(),
            TargetTemplate::AppleContainer {
                container: container_template(),
            },
        );
        config.targets.insert(
            "workstation".into(),
            TargetTemplate::LocalPodman {
                container: container_template(),
            },
        );

        assert_eq!(default_import_target_id(&config), "workstation");
    }

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
        // Import deltas against the tracked remote, so a realistic checkout
        // needs a remote-tracking ref.
        for arguments in [
            vec!["update-ref", "refs/remotes/origin/main", "HEAD"],
            vec!["branch", "--set-upstream-to", "origin/main", "main"],
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
    }

    #[test]
    fn session_targets_include_edited_roots_and_keep_cwd_primary() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        let sibling = directory.path().join("sibling");
        initialize_repository(&app, "app");
        initialize_repository(&sibling, "sibling");
        let transcript = ClaudeTranscript {
            cwd: app.clone(),
            edited_paths: vec![sibling.join("src/lib.rs")],
            events: Vec::new(),
        };

        let targets = session_edit_targets(&transcript, &directory.path().join("profile")).unwrap();
        assert_eq!(targets.git_roots.len(), 2);
        assert!(targets.git_roots.contains(&fs::canonicalize(app).unwrap()));
        assert!(
            targets
                .git_roots
                .contains(&fs::canonicalize(sibling).unwrap())
        );
        assert!(targets.non_git_dirs.is_empty());
    }

    #[test]
    fn codex_extracts_only_completed_file_changes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rollout.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"session_meta","payload":{"cwd":"/work/app","history_mode":"paginated"}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"item_completed","item":{"type":"UserMessage","content":[{"text":"edit"}]}}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"item_completed","item":{"type":"FileChange","status":"completed","changes":{"/work/a.txt":{"type":"add"}}}}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"item_completed","item":{"type":"FileChange","status":"failed","changes":{"/work/b.txt":{"type":"add"}}}}}"#,
                "\n",
            ),
        )
        .unwrap();
        let transcript = read_codex_transcript(&path).unwrap();
        assert_eq!(transcript.edited_paths, [PathBuf::from("/work/a.txt")]);
    }

    #[test]
    fn claude_prefers_file_history_and_accepts_successful_edit_fallback() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"user","cwd":"/work/app","message":{"content":"edit"}}"#,
                "\n",
                r#"{"type":"file-history-delta","trackingPath":"src/lib.rs","backup":{"realParentDir":"/work/app/src"}}"#,
                "\n",
                r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"ok","name":"Write","input":{"file_path":"relative.txt"}},{"type":"tool_use","id":"bad","name":"Edit","input":{"file_path":"bad.txt"}}]}}"#,
                "\n",
                r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"ok","content":"done"},{"type":"tool_result","tool_use_id":"bad","is_error":true,"content":"failed"}]}}"#,
                "\n",
            ),
        )
        .unwrap();
        let transcript = read_claude_transcript(&path).unwrap();
        assert_eq!(
            transcript.edited_paths,
            [
                PathBuf::from("/work/app/src/lib.rs"),
                PathBuf::from("relative.txt")
            ]
        );
    }

    #[test]
    fn kimi_extracts_successful_edits_from_all_agents() {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir_all(directory.path().join("agents/main")).unwrap();
        fs::write(
            directory.path().join("agents/main/wire.jsonl"),
            concat!(
                r#"{"type":"context.append_loop_event","event":{"type":"tool.call","toolCallId":"ok","name":"Write","args":{"path":"one.txt"}}}"#,
                "\n",
                r#"{"type":"context.append_loop_event","event":{"type":"tool.result","toolCallId":"ok","result":{"output":"done"}}}"#,
                "\n",
                r#"{"type":"context.append_loop_event","event":{"type":"tool.call","toolCallId":"bad","name":"Edit","args":{"path":"bad.txt"}}}"#,
                "\n",
                r#"{"type":"context.append_loop_event","event":{"type":"tool.result","toolCallId":"bad","result":{"isError":true}}}"#,
                "\n",
            ),
        )
        .unwrap();
        assert_eq!(
            kimi_edited_paths(directory.path()).unwrap(),
            [PathBuf::from("one.txt")]
        );
    }

    #[test]
    fn resolve_bundle_requires_an_exact_root_set() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        let sibling = directory.path().join("sibling");
        initialize_repository(&app, "app");
        initialize_repository(&sibling, "sibling");
        let targets = SessionEditTargets {
            git_roots: vec![
                fs::canonicalize(&app).unwrap(),
                fs::canonicalize(&sibling).unwrap(),
            ],
            non_git_dirs: Vec::new(),
        };
        let config = HelConfig::default();
        let BundleResolution::Synthesized { bundle, .. } =
            resolve_bundle(&config, &app, &targets, None).unwrap()
        else {
            panic!("expected synthesized bundle");
        };
        assert_eq!(bundle.repositories.len(), 2);
        assert_eq!(bundle.primary_repo, "app");

        let mut config = HelConfig::default();
        config.bundles.insert("multi".into(), bundle);
        assert_eq!(
            resolve_bundle(&config, &app, &targets, None).unwrap(),
            BundleResolution::Existing("multi".into())
        );
        let app_only = SessionEditTargets {
            git_roots: vec![fs::canonicalize(&app).unwrap()],
            non_git_dirs: Vec::new(),
        };
        assert!(resolve_bundle(&config, &app, &app_only, Some("multi")).is_err());
    }

    /// A repository without a remote takes the `Local` identity, which is the
    /// case a linked worktree used to split into its own project.
    fn initialize_local_repository(path: &Path, id: &str) {
        initialize_repository(path, id);
        run_git(path, &["remote", "remove", "origin"]);
    }

    fn run_git(path: &Path, arguments: &[&str]) {
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

    fn add_worktree(repository: &Path, worktree: &Path, branch: &str) -> PathBuf {
        run_git(
            repository,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                branch,
                worktree.to_str().unwrap(),
            ],
        );
        fs::canonicalize(worktree).unwrap()
    }

    #[test]
    fn resolve_bundle_matches_an_existing_bundle_from_a_linked_worktree() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        initialize_local_repository(&app, "app");
        let worktree = add_worktree(&app, &directory.path().join("app2"), "side");
        let app = fs::canonicalize(&app).unwrap();

        let mut config = HelConfig::default();
        config.bundles.insert(
            "app".into(),
            ProjectBundle {
                primary_repo: "app".into(),
                repositories: vec![ProjectRepository {
                    id: "app".into(),
                    local: Some(app.clone()),
                    github: None,
                    destination: PathBuf::from("app"),
                    git_ref: None,
                }],
            },
        );
        let targets = SessionEditTargets {
            git_roots: vec![worktree.clone()],
            non_git_dirs: Vec::new(),
        };

        assert_eq!(
            resolve_bundle(&config, &worktree, &targets, None).unwrap(),
            BundleResolution::Existing("app".into())
        );
    }

    #[test]
    fn resolve_bundle_synthesizes_a_worktree_session_as_its_main_repository() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        initialize_local_repository(&app, "app");
        let worktree = add_worktree(&app, &directory.path().join("app2"), "side");
        let app = fs::canonicalize(&app).unwrap();
        let targets = SessionEditTargets {
            git_roots: vec![worktree.clone(), app.clone()],
            non_git_dirs: Vec::new(),
        };

        let BundleResolution::Synthesized { id, bundle } =
            resolve_bundle(&HelConfig::default(), &worktree, &targets, None).unwrap()
        else {
            panic!("expected synthesized bundle");
        };
        assert_eq!(id, "app");
        assert_eq!(bundle.primary_repo, "app");
        assert_eq!(bundle.repositories.len(), 1);
        assert_eq!(bundle.repositories[0].id, "app");
        assert_eq!(bundle.repositories[0].local.as_deref(), Some(app.as_path()));
    }

    #[test]
    fn edit_targets_filter_profile_state_and_report_non_git_directories() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        let profile = directory.path().join("profile");
        let outside = directory.path().join("notes");
        initialize_repository(&app, "app");
        fs::create_dir_all(&profile).unwrap();
        fs::create_dir_all(&outside).unwrap();
        let transcript = ClaudeTranscript {
            cwd: app.clone(),
            edited_paths: vec![profile.join("memory.md"), outside.join("draft.md")],
            events: Vec::new(),
        };

        let targets = session_edit_targets(&transcript, &profile).unwrap();
        assert_eq!(targets.git_roots, [fs::canonicalize(app).unwrap()]);
        assert_eq!(targets.non_git_dirs, [outside]);
    }

    #[cfg(unix)]
    #[test]
    fn edit_targets_compare_profile_paths_after_resolving_symlinks() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let actual = directory.path().join("actual");
        let alias = directory.path().join("alias");
        let app = actual.join("app");
        let profile = actual.join("profile");
        let outside = actual.join("notes");
        initialize_repository(&app, "app");
        fs::create_dir_all(&profile).unwrap();
        fs::create_dir_all(&outside).unwrap();
        symlink(&actual, &alias).unwrap();
        let transcript = ClaudeTranscript {
            cwd: alias.join("app"),
            edited_paths: vec![
                alias.join("profile/memory.md"),
                alias.join("notes/draft.md"),
            ],
            events: Vec::new(),
        };

        let targets = session_edit_targets(&transcript, &alias.join("profile")).unwrap();

        assert_eq!(targets.git_roots, [fs::canonicalize(app).unwrap()]);
        assert_eq!(targets.non_git_dirs, [alias.join("notes")]);
    }

    #[test]
    fn import_safety_reports_dirty_roots_and_non_git_omissions() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        initialize_repository(&app, "app");
        fs::write(app.join("README.md"), "dirty").unwrap();
        fs::write(app.join("untracked.txt"), "new").unwrap();
        let omitted = directory.path().join("notes");
        let issues = import_safety_issues(&SessionEditTargets {
            git_roots: vec![app.clone()],
            non_git_dirs: vec![omitted.clone()],
        })
        .unwrap();

        assert_eq!(issues.dirty_git_roots.len(), 1);
        assert_eq!(issues.dirty_git_roots[0].0, app);
        assert_eq!(
            issues.dirty_git_roots[0].1,
            "1 tracked change · 1 untracked path"
        );
        assert!(issues.has_untracked_files);
        assert_eq!(issues.omitted_non_git_dirs, [omitted]);
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
                    github: Some("BrokkAi/hel".into()),
                    local: None,
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
                    github: Some("example/worker".into()),
                    local: None,
                    destination: "worker".into(),
                    git_ref: None,
                },
                ProjectRepository {
                    id: "app".into(),
                    github: Some("example/app".into()),
                    local: None,
                    destination: "app".into(),
                    git_ref: None,
                },
            ],
        };

        let snapshots =
            collect_local_repositories(&bundle, &[app.clone(), workspace.join("worker")], None)
                .unwrap();
        let ids = snapshots
            .iter()
            .map(|snapshot| snapshot.metadata.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["worker", "app"]);
    }

    #[test]
    fn local_source_repository_import_carries_no_committed_bundle() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        initialize_repository(&app, "app");
        let output = Command::new("git")
            .args(["remote", "remove", "origin"])
            .current_dir(&app)
            .output()
            .unwrap();
        assert!(output.status.success());
        fs::write(app.join("dirty.txt"), "dirty").unwrap();
        let bundle = ProjectBundle {
            primary_repo: "app".into(),
            repositories: vec![ProjectRepository {
                id: "app".into(),
                github: None,
                local: Some(app.clone()),
                destination: "app".into(),
                git_ref: None,
            }],
        };

        let snapshots = collect_local_repositories(&bundle, &[app], None).unwrap();

        assert!(snapshots[0].committed_bundle.is_empty());
        assert_eq!(snapshots[0].metadata.origin, "hel-local:app");
        assert!(!snapshots[0].untracked_tar.is_empty());
    }

    #[test]
    fn import_without_remote_tracking_refs_reports_how_to_recover() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        initialize_repository(&app, "app");
        let output = Command::new("git")
            .args(["update-ref", "-d", "refs/remotes/origin/main"])
            .current_dir(&app)
            .output()
            .unwrap();
        assert!(output.status.success());
        let bundle = ProjectBundle {
            primary_repo: "app".into(),
            repositories: vec![ProjectRepository {
                id: "app".into(),
                github: Some("example/app".into()),
                local: None,
                destination: "app".into(),
                git_ref: None,
            }],
        };

        let error = collect_local_repositories(&bundle, &[app], None).unwrap_err();

        assert!(
            format!("{error:#}").contains("has no remote-tracking refs to import against"),
            "{error:#}"
        );
    }

    #[test]
    fn codex_jsonl_projects_user_and_agent_messages() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rollout.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"session_meta","payload":{"session_id":"019feb6c-5ffc-7c12-ad99-bdeaeb6be79d","cwd":"/work/app","history_mode":"paginated"}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"item_completed","item":{"type":"UserMessage","id":"user-1","content":[{"type":"text","text":"first prompt","text_elements":[]}]}}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"item_completed","item":{"type":"AgentMessage","id":"agent-1","content":[{"type":"Text","text":"first reply"}],"phase":"final_answer"}}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"turn_complete","turn_id":"turn-1"}}"#,
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
    fn nonempty_codex_import_materializes_and_validates_canonical_archive() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        initialize_repository(&app, "app");

        let codex_home = directory.path().join("codex");
        let native_session_id = "019feb6c-5ffc-7c12-ad99-bdeaeb6be79d";
        let rollout = codex_home
            .join("sessions/2026/08/14")
            .join(format!("rollout-{native_session_id}.jsonl"));
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        let records = [
            json!({
                "timestamp": "2026-08-14T12:00:00.000Z",
                "type": "session_meta",
                "payload": {
                    "id": native_session_id,
                    "cwd": app,
                    "history_mode": "paginated"
                }
            }),
            json!({
                "timestamp": "2026-08-14T12:00:01.250Z",
                "type": "event_msg",
                "payload": {
                    "type": "item_completed",
                    "item": {
                        "type": "UserMessage",
                        "content": [{"type": "text", "text": "import this"}]
                    }
                }
            }),
            json!({
                "timestamp": "2026-08-14T12:00:02.500Z",
                "type": "event_msg",
                "payload": {
                    "type": "item_completed",
                    "item": {
                        "type": "AgentMessage",
                        "content": [{"type": "Text", "text": "imported"}]
                    }
                }
            }),
            json!({
                "timestamp": "2026-08-14T12:00:03.750Z",
                "type": "event_msg",
                "payload": {"type": "turn_complete", "turn_id": "turn-1"}
            }),
        ];
        fs::write(
            &rollout,
            records
                .into_iter()
                .map(|record| record.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let transcript = read_codex_transcript(&rollout).unwrap();
        let expected_user_time = DateTime::parse_from_rfc3339("2026-08-14T12:00:01.250Z")
            .unwrap()
            .timestamp_millis();
        let expected_agent_time = DateTime::parse_from_rfc3339("2026-08-14T12:00:02.500Z")
            .unwrap()
            .timestamp_millis();
        let expected_activity = DateTime::parse_from_rfc3339("2026-08-14T12:00:03.750Z")
            .unwrap()
            .timestamp_millis();
        assert_eq!(
            transcript
                .events
                .iter()
                .map(|event| event.recorded_at_ms)
                .collect::<Vec<_>>(),
            [
                Some(expected_user_time),
                Some(expected_agent_time),
                Some(expected_activity)
            ]
        );
        let metadata = fs::metadata(&rollout).unwrap();
        let source = LocatedCodexSession {
            native_session_id: native_session_id.into(),
            jsonl_path: rollout,
            modified_at: metadata.modified().unwrap(),
            title: "Imported Codex session".into(),
            cwd: app,
            git_branch: "main".into(),
            size_bytes: metadata.len(),
            history_mode: CodexHistoryMode::Paginated,
        };
        let mut config = HelConfig::default();
        config.bundles.insert(
            "app".into(),
            ProjectBundle {
                primary_repo: "app".into(),
                repositories: vec![ProjectRepository {
                    id: "app".into(),
                    github: Some("example/app".into()),
                    local: None,
                    destination: "app".into(),
                    git_ref: None,
                }],
            },
        );
        let archive_directory = directory.path().join("archives");
        fs::create_dir_all(&archive_directory).unwrap();
        let mut state = HelState::default();

        let imported = import_codex_session(
            &config,
            &mut state,
            CodexImportRequest {
                codex_home: &codex_home,
                source: &source,
                transcript: &transcript,
                bundle_id: "app",
                profile_id: None,
                title: None,
                archive_directory: &archive_directory,
            },
        )
        .unwrap();
        let verified =
            crate::hel_archive::verify_archive_streaming(&imported.archive_path).unwrap();
        assert_eq!(verified.canonical_session.event_frontier, 3);
        assert_eq!(
            verified.canonical_session.session.last_activity_at_ms,
            Some(expected_activity)
        );
        assert_eq!(verified.canonical_session.transcript.len(), 2);
        assert_eq!(
            verified.canonical_session.transcript[0].created_at_ms,
            expected_user_time
        );
        assert_eq!(
            verified.canonical_session.transcript[1].created_at_ms,
            expected_agent_time
        );
        assert!(state.sessions.contains_key(&imported.session_id));
    }

    #[test]
    fn codex_paginated_import_ignores_compaction_artifacts() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rollout.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"session_meta","payload":{"cwd":"/work/app","history_mode":"paginated"}}"#,
                "\n",
                r#"{"type":"response_item","payload":{"type":"compaction","encrypted_content":"opaque"}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"item_completed","item":{"type":"UserMessage","id":"user-1","content":[{"type":"text","text":"prompt after compaction"}]}}}"#,
                "\n",
            ),
        )
        .unwrap();
        let transcript = read_codex_transcript(&path).unwrap();
        assert!(matches!(
            &transcript.events[0].event,
            WorkerEvent::PromptAccepted { text, .. } if text == "prompt after compaction"
        ));
        assert_eq!(transcript.events.len(), 2);
    }

    #[test]
    fn codex_import_rejects_legacy_history_with_migration_guidance() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rollout.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"session_meta","payload":{"cwd":"/work/app"}}"#,
                "\n",
                r#"{"type":"event_msg","payload":{"type":"user_message","message":"raw prompt"}}"#,
                "\n",
            ),
        )
        .unwrap();
        let error = read_codex_transcript(&path).unwrap_err().to_string();
        assert!(error.contains("Legacy Codex history cannot be imported"));
        assert!(error.contains("codex migrate-rollouts --apply"));
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
            &CodexSessionSelection::NativeSessionId("019feb6c-6b55-7111-a210-6d85ee0772cd".into()),
        )
        .unwrap();
        assert_eq!(
            located.native_session_id,
            "019feb6c-6b55-7111-a210-6d85ee0772cd"
        );
        assert_eq!(located.jsonl_path, rollout);
    }

    #[test]
    fn codex_listing_uses_native_title_and_session_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let session_id = "019feb6c-6b55-7111-a210-6d85ee0772cd";
        let rollout = directory.path().join("sessions/rollout.jsonl");
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        fs::write(
            &rollout,
            format!(
                r#"{{"type":"session_meta","payload":{{"id":"{session_id}","cwd":"/work/app","git":{{"branch":"feature"}}}}}}"#
            ),
        )
        .unwrap();
        fs::write(
            directory.path().join("history.jsonl"),
            format!(r#"{{"session_id":"{session_id}","text":"native title\ncontinued"}}"#),
        )
        .unwrap();

        let sessions = list_codex_sessions(directory.path()).unwrap();
        assert_eq!(sessions[0].title, "native title continued");
        assert_eq!(sessions[0].cwd, PathBuf::from("/work/app"));
        assert_eq!(sessions[0].git_branch, "feature");
        assert!(sessions[0].size_bytes > 0);
        assert_eq!(sessions[0].history_mode, CodexHistoryMode::Legacy);
    }

    #[test]
    fn codex_listing_matches_native_interactive_visibility_and_title_priority() {
        let directory = tempfile::tempdir().unwrap();
        let sessions = directory.path().join("sessions");
        let archived = directory.path().join("archived_sessions");
        fs::create_dir_all(&sessions).unwrap();
        fs::create_dir_all(&archived).unwrap();
        let interactive_id = "019feb6c-6b55-7111-a210-6d85ee0772cd";
        for (path, id, source) in [
            (
                sessions.join("interactive.jsonl"),
                interactive_id,
                json!("cli"),
            ),
            (
                sessions.join("exec.jsonl"),
                "019feb6c-6b55-7111-a210-6d85ee0772ce",
                json!("exec"),
            ),
            (
                sessions.join("subagent.jsonl"),
                "019feb6c-6b55-7111-a210-6d85ee0772cf",
                json!({"subagent": {"thread_spawn": {"parent_thread_id": interactive_id}}}),
            ),
            (
                sessions.join("ephemeral.jsonl"),
                "019feb6c-6b55-7111-a210-6d85ee0772d1",
                json!("cli"),
            ),
            (
                archived.join("archived.jsonl"),
                "019feb6c-6b55-7111-a210-6d85ee0772d0",
                json!("cli"),
            ),
        ] {
            let ephemeral = path.ends_with("ephemeral.jsonl");
            fs::write(
                &path,
                json!({
                    "type": "session_meta",
                    "payload": {
                        "id": id,
                        "source": source,
                        "cwd": "/work/app",
                        "ephemeral": ephemeral
                    }
                })
                .to_string(),
            )
            .unwrap();
        }
        fs::write(
            directory.path().join("history.jsonl"),
            json!({"session_id": interactive_id, "text": "Generated history title"}).to_string(),
        )
        .unwrap();
        fs::write(
            directory.path().join("session_index.jsonl"),
            json!({"id": interactive_id, "thread_name": "Explicit native title"}).to_string(),
        )
        .unwrap();

        let listed = list_codex_sessions(directory.path()).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].native_session_id, interactive_id);
        assert_eq!(listed[0].title, "Explicit native title");
    }

    #[test]
    fn codex_listing_uses_native_index_order_and_includes_all_persisted_threads() {
        let directory = tempfile::tempdir().unwrap();
        let sessions = directory.path().join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let connection =
            rusqlite::Connection::open(directory.path().join("state_5.sqlite")).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    rollout_path TEXT,
                    updated_at INTEGER,
                    name TEXT,
                    title TEXT,
                    cwd TEXT,
                    git_branch TEXT,
                    archived INTEGER,
                    source TEXT,
                    preview TEXT,
                    history_mode TEXT
                );",
            )
            .unwrap();
        for index in 0..30_u64 {
            let id = format!("019feb6c-6b55-7111-a210-{index:012x}");
            let rollout = sessions.join(format!("rollout-{id}.jsonl"));
            fs::write(&rollout, "indexed").unwrap();
            connection
                .execute(
                    "INSERT INTO threads VALUES \
                     (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 'cli', 'visible', 'paginated')",
                    rusqlite::params![
                        id,
                        rollout.to_string_lossy(),
                        index as i64,
                        (index == 29).then_some("Explicit newest title"),
                        format!("Generated {index}"),
                        "/work/app",
                        "feature"
                    ],
                )
                .unwrap();
        }
        connection
            .execute(
                "INSERT INTO threads VALUES \
                 (?1, NULL, 100, 'Ephemeral', 'Ephemeral', '/work/app', 'HEAD', 0, 'cli', \
                  'visible', 'legacy')",
                ["019feb6c-6b55-7111-a210-ffffffffffff"],
            )
            .unwrap();
        drop(connection);

        let listed = list_codex_sessions(directory.path()).unwrap();
        assert_eq!(listed.len(), 30);
        assert_eq!(listed[0].title, "Explicit newest title");
        assert_eq!(listed[0].history_mode, CodexHistoryMode::Paginated);
        assert!(listed[0].modified_at > listed[1].modified_at);
        assert_eq!(listed[29].title, "Generated 0");
        assert!(listed.iter().all(|session| session.title != "Ephemeral"));
    }

    #[test]
    fn codex_scan_reports_progress_and_emits_newest_first() {
        let directory = tempfile::tempdir().unwrap();
        let sessions = directory.path().join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        for (name, session_id) in [
            ("first.jsonl", "019feb6c-6b55-7111-a210-6d85ee0772cd"),
            ("second.jsonl", "019feb6c-6b55-7111-a210-6d85ee0772ce"),
        ] {
            fs::write(
                sessions.join(name),
                format!(r#"{{"type":"session_meta","payload":{{"id":"{session_id}"}}}}"#),
            )
            .unwrap();
        }
        let mut updates = Vec::new();
        scan_codex_sessions(directory.path(), |progress| {
            updates.push((
                progress.scanned,
                progress.total,
                progress.session.map(|session| session.modified_at),
            ));
        })
        .unwrap();

        assert_eq!(updates.len(), 3);
        assert_eq!((updates[0].0, updates[0].1), (0, 2));
        assert!(updates[0].2.is_none());
        assert_eq!((updates[1].0, updates[1].1), (1, 2));
        assert_eq!((updates[2].0, updates[2].1), (2, 2));
        assert!(updates[1].2 >= updates[2].2);
    }

    #[test]
    fn claude_listing_uses_ai_title_and_native_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let rollout = directory
            .path()
            .join("projects/work/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl");
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        fs::write(
            &rollout,
            concat!(
                r#"{"type":"user","cwd":"/work/app","gitBranch":"feature","message":{"content":"fallback"}}"#,
                "\n",
                r#"{"type":"ai-title","aiTitle":"Native Claude title"}"#,
            ),
        )
        .unwrap();

        let sessions = list_claude_sessions(directory.path()).unwrap();
        assert_eq!(sessions[0].title, "Native Claude title");
        assert_eq!(sessions[0].cwd, PathBuf::from("/work/app"));
        assert_eq!(sessions[0].git_branch, "feature");
        assert!(sessions[0].size_bytes > 0);
    }

    #[test]
    fn claude_listing_matches_native_title_priority_and_visibility() {
        let directory = tempfile::tempdir().unwrap();
        let project = directory.path().join("projects/work");
        fs::create_dir_all(&project).unwrap();
        fs::write(
            project.join("interactive.jsonl"),
            concat!(
                r#"{"type":"user","entrypoint":"cli","cwd":"/work/app","message":{"content":"fallback"}}"#,
                "\n",
                r#"{"type":"agent-name","agentName":"renamed-agent"}"#,
                "\n",
                r#"{"type":"custom-title","customTitle":"Native custom title"}"#,
                "\n",
                r#"{"type":"ai-title","aiTitle":"Generated title"}"#,
            ),
        )
        .unwrap();
        fs::write(
            project.join("print-mode.jsonl"),
            r#"{"type":"user","entrypoint":"sdk-cli","cwd":"/work/app","message":{"content":"<local-command-caveat>usage poll</local-command-caveat>"}}"#,
        )
        .unwrap();
        fs::write(
            project.join("sidechain.jsonl"),
            r#"{"type":"user","entrypoint":"cli","isSidechain":true,"cwd":"/work/app","message":{"content":"subagent"}}"#,
        )
        .unwrap();

        let sessions = list_claude_sessions(directory.path()).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].title, "Native custom title");
        assert_eq!(sessions[0].native_session_id, "interactive");
    }

    #[test]
    fn claude_listing_prefers_agent_name_to_generated_title() {
        let directory = tempfile::tempdir().unwrap();
        let rollout = directory.path().join("session.jsonl");
        fs::write(
            &rollout,
            concat!(
                r#"{"type":"user","entrypoint":"cli","cwd":"/work/app","message":{"content":"fallback"}}"#,
                "\n",
                r#"{"type":"agent-name","agentName":"restic-cleanup"}"#,
                "\n",
                r#"{"type":"ai-title","aiTitle":"Clean up fulldata directory organization"}"#,
            ),
        )
        .unwrap();

        let (title, _, _) = claude_native_metadata(&rollout).unwrap().unwrap();
        assert_eq!(title, "restic-cleanup");
    }

    #[test]
    fn claude_listing_uses_native_all_projects_limit() {
        let directory = tempfile::tempdir().unwrap();
        let project = directory.path().join("projects/work");
        fs::create_dir_all(&project).unwrap();
        for index in 0..51 {
            fs::write(
                project.join(format!("session-{index:02}.jsonl")),
                format!(
                    r#"{{"type":"user","entrypoint":"cli","cwd":"/work/app","message":{{"content":"Session {index}"}}}}"#
                ),
            )
            .unwrap();
        }

        assert_eq!(list_claude_sessions(directory.path()).unwrap().len(), 50);
        let oldest = locate_claude_session(
            directory.path(),
            &ClaudeSessionSelection::NativeSessionId("session-00".into()),
        )
        .unwrap();
        assert_eq!(oldest.native_session_id, "session-00");
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
        let wire_path = directory.path().join("agents/main/wire.jsonl");
        fs::write(
            &wire_path,
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
        let expected_fallback =
            DateTime::<Utc>::from(fs::metadata(&wire_path).unwrap().modified().unwrap())
                .timestamp_millis();

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
        assert!(
            transcript
                .events
                .iter()
                .all(|event| event.recorded_at_ms == Some(expected_fallback))
        );
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
        fs::write(
            directory.path().join("session_index.jsonl"),
            json!({
                "sessionId": format!("session_{id}"),
                "sessionDir": session,
                "workDir": "/work/app"
            })
            .to_string(),
        )
        .unwrap();

        let located = locate_kimi_session(
            directory.path(),
            &KimiSessionSelection::NativeSessionId(format!("session_{id}")),
        )
        .unwrap();
        assert_eq!(located.native_session_id, format!("session_{id}"));
        assert_eq!(
            located.session_path.canonicalize().unwrap(),
            session.canonicalize().unwrap()
        );
    }

    #[test]
    fn kimi_listing_matches_native_index_visibility_and_title() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("sessions/project");
        fs::create_dir_all(&workspace).unwrap();
        let visible = workspace.join("session_visible");
        let archived = workspace.join("session_archived");
        let deleted = workspace.join("session_deleted");
        let unindexed = workspace.join("session_unindexed");
        for session in [&visible, &archived, &deleted, &unindexed] {
            fs::create_dir_all(session).unwrap();
        }
        fs::write(
            visible.join("state.json"),
            r#"{"workDir":"/work/native","title":"Generated","customTitle":"Native custom title"}"#,
        )
        .unwrap();
        fs::write(
            archived.join("state.json"),
            r#"{"workDir":"/work/app","title":"Archived","archived":true}"#,
        )
        .unwrap();
        let index = [
            json!({"sessionId":"session_visible","sessionDir":visible,"workDir":"/work/index"}),
            json!({"sessionId":"session_archived","sessionDir":archived,"workDir":"/work/app"}),
            json!({"sessionId":"session_deleted","sessionDir":deleted,"workDir":"/work/app"}),
            json!({"sessionId":"session_deleted","deleted":true}),
        ]
        .into_iter()
        .map(|record| record.to_string())
        .collect::<Vec<_>>()
        .join("\n");
        fs::write(directory.path().join("session_index.jsonl"), index).unwrap();

        let listed = list_kimi_sessions(directory.path()).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].native_session_id, "session_visible");
        assert_eq!(listed[0].title, "Native custom title");
        assert_eq!(listed[0].cwd, PathBuf::from("/work/native"));
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
