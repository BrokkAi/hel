//! Normalized controller state and composer history stored in SQLite.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::hel_config::data_dir;
use crate::hel_state::{
    CheckpointMetadata, HelState, ManagedWorktree, MaterializedExecutionState,
    MaterializedQueuedPrompt, MaterializedSession, MaterializedSessionSummary, SessionRecord,
    SessionResourceAllocation, SessionState, TargetLocator, TranscriptBody, TranscriptItem,
    validate_relay_event_digest, validate_relay_event_frontier,
};
use crate::hel_targets::AdditionalMount;
use crate::hel_worker::RELAY_EVENT_GENESIS_DIGEST;

const SCHEMA_VERSION: i64 = 12;

/// A deterministic projection integrity violation. Retrying cannot fix it, so
/// callers must report it separately from transport failures.
#[derive(Debug)]
pub struct ProjectionIntegrityError(pub String);

impl std::fmt::Display for ProjectionIntegrityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ProjectionIntegrityError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryScope {
    Project,
    Session,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptHistoryEntry {
    pub id: i64,
    pub session_id: String,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionApplyOutcome {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TranscriptMutation {
    Upsert(TranscriptItem),
    Remove { stable_id: String },
}

/// Changes derived from one relay event. `None` leaves a scalar untouched;
/// the nested option on `session_title` permits explicitly clearing it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MaterializedSessionMutation {
    /// Relay receipt time for this event. Persistence and the actor cache both
    /// take a monotonic maximum so removing detail rows cannot move activity
    /// backwards.
    pub last_activity_at_ms: Option<i64>,
    pub execution: Option<MaterializedExecutionState>,
    pub session_title: Option<Option<String>>,
    pub configuration: Option<BTreeMap<String, serde_json::Value>>,
    pub transcript: Vec<TranscriptMutation>,
    pub queued_prompts: Option<Vec<MaterializedQueuedPrompt>>,
    pub pending_elicitations: Option<Vec<crate::hel_elicitation::ElicitationRequest>>,
}

pub fn database_path() -> PathBuf {
    data_dir().join("hel.sqlite3")
}

fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create Hel data directory {}", parent.display()))?;
    }
    let connection =
        Connection::open(path).with_context(|| format!("open Hel database {}", path.display()))?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = FULL;",
    )?;
    verify_schema_once(path, &connection)?;
    Ok(connection)
}

/// Databases this process has already migrated. A controller owns its store
/// exclusively (`ControllerStoreGuard`), so a schema verified once stays
/// verified and later connections skip the migration probes entirely.
fn verified_schemas() -> &'static Mutex<HashSet<PathBuf>> {
    static VERIFIED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    VERIFIED.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Stable cache identity for a database. The file itself may not exist yet, so
/// the canonicalized parent directory carries the identity.
fn schema_cache_key(path: &Path) -> PathBuf {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return path.to_owned();
    };
    match (fs::canonicalize(parent), path.file_name()) {
        (Ok(canonical), Some(name)) => canonical.join(name),
        _ => path.to_owned(),
    }
}

/// Run the migration ladder the first time this process opens a database.
/// Later opens confirm only the recorded schema version, which keeps relay
/// catch-up from paying for the full probe sequence on every connection. A
/// database whose version no longer matches is migrated again, so a recreated
/// file under a reused path still converges.
fn verify_schema_once(path: &Path, connection: &Connection) -> Result<()> {
    let key = schema_cache_key(path);
    let mut verified = verified_schemas()
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if verified.contains(&key) {
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version == SCHEMA_VERSION {
            return Ok(());
        }
    }
    // Holding the lock across the ladder keeps two first opens of the same
    // database from running the additive migration steps against each other.
    migrate_schema(connection)?;
    verified.insert(key);
    Ok(())
}

/// Forget that this process verified a database's schema. Only tests need it:
/// they simulate a store written by an older build by editing the schema of a
/// database this process has already opened, which no controller can do.
#[cfg(test)]
fn forget_verified_schema(path: &Path) {
    verified_schemas()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&schema_cache_key(path));
}

fn migrate_schema(connection: &Connection) -> Result<()> {
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        bail!("Hel database schema {version} is newer than supported schema {SCHEMA_VERSION}");
    }
    if version == 0 {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE schema_migrations (
                 version INTEGER PRIMARY KEY CHECK(version > 0),
                 applied_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE session_contexts (
                 session_id TEXT PRIMARY KEY,
                 bundle_id TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE sessions (
                 session_id TEXT PRIMARY KEY REFERENCES session_contexts(session_id),
                 title TEXT NOT NULL CHECK(length(trim(title)) > 0),
                 harness_kind TEXT NOT NULL CHECK(harness_kind IN ('codex','claude','kimi')),
                 last_profile TEXT NOT NULL,
                 target_template_id TEXT NOT NULL,
                 state TEXT NOT NULL CHECK(state IN (
                     'provisioning','running','disconnected','checkpointing','closing','destroying',
                     'archived','lost','error','destroyed-with-data-loss'
                 )),
                 native_session_id TEXT,
                 acp_session_title TEXT CHECK(acp_session_title IS NULL OR length(trim(acp_session_title)) > 0),
                 session_title_override TEXT CHECK(session_title_override IS NULL OR length(trim(session_title_override)) > 0),
                 updated_at TEXT NOT NULL,
                 last_viewed_event_sequence INTEGER NOT NULL DEFAULT 0 CHECK(last_viewed_event_sequence >= 0),
                 last_error TEXT
             ) STRICT;
             CREATE TABLE session_targets (
                 session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                 kind TEXT NOT NULL CHECK(kind IN ('local-bare','local-podman','apple-container','aws-ec2','ssh-bare','ssh-podman')),
                 host TEXT,
                 resource_id TEXT,
                 address TEXT,
                 workspace BLOB,
                 worker_id TEXT,
                 CHECK(
                     (kind = 'local-bare' AND workspace IS NOT NULL
                      AND host IS NULL AND resource_id IS NULL AND address IS NULL AND worker_id IS NULL)
                  OR (kind IN ('local-podman','apple-container') AND resource_id IS NOT NULL
                      AND host IS NULL AND address IS NULL AND workspace IS NULL AND worker_id IS NULL)
                  OR (kind = 'aws-ec2' AND resource_id IS NOT NULL
                      AND host IS NULL AND workspace IS NULL AND worker_id IS NULL)
                  OR (kind = 'ssh-bare' AND host IS NOT NULL AND workspace IS NOT NULL
                      AND resource_id IS NULL AND address IS NULL)
                  OR (kind = 'ssh-podman' AND host IS NOT NULL AND resource_id IS NOT NULL
                      AND address IS NULL AND workspace IS NULL AND worker_id IS NULL)
                 )
             ) STRICT;
             CREATE TABLE session_mounts (
                 session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                 ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                 source BLOB NOT NULL,
                 destination BLOB NOT NULL,
                 PRIMARY KEY(session_id, ordinal),
                 UNIQUE(session_id, destination)
             ) STRICT;
             CREATE TABLE session_checkpoints (
                 session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                 archive_path BLOB NOT NULL,
                 sha256 TEXT NOT NULL CHECK(length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
                 created_at TEXT NOT NULL,
                 event_sequence INTEGER NOT NULL CHECK(event_sequence >= 0)
             ) STRICT;
             CREATE TABLE mount_history (
                 host TEXT NOT NULL CHECK(length(trim(host)) > 0),
                 source BLOB NOT NULL,
                 ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                 PRIMARY KEY(host, ordinal),
                 UNIQUE(host, source)
             ) STRICT;
             CREATE TABLE prompt_history (
                 history_id INTEGER PRIMARY KEY,
                 session_id TEXT NOT NULL REFERENCES session_contexts(session_id),
                 event_sequence INTEGER NOT NULL CHECK(event_sequence >= 0),
                 submitted_at TEXT NOT NULL,
                 text TEXT NOT NULL CHECK(length(trim(text)) > 0),
                 UNIQUE(session_id, event_sequence)
             ) STRICT;
             CREATE INDEX prompt_history_session_recent
                 ON prompt_history(session_id, history_id DESC);
             CREATE INDEX session_contexts_bundle
                 ON session_contexts(bundle_id, session_id);
             CREATE INDEX prompt_history_recent
                 ON prompt_history(history_id DESC);
             INSERT INTO schema_migrations(version, applied_at)
                 VALUES (1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
             PRAGMA user_version = 1;
             COMMIT;",
        )?;
    }
    if version < 2 {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE sessions ADD COLUMN resource_allocation TEXT;
             INSERT INTO schema_migrations(version, applied_at)
                 VALUES (2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
             PRAGMA user_version = 2;
             COMMIT;",
        )?;
    }
    if version < 3 {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE sessions ADD COLUMN last_checkpoint_error TEXT;
             INSERT INTO schema_migrations(version, applied_at)
                 VALUES (3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
             PRAGMA user_version = 3;
             COMMIT;",
        )?;
    }
    if version < 4 {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE sessions ADD COLUMN project_directory BLOB;
             INSERT INTO schema_migrations(version, applied_at)
                 VALUES (4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
             PRAGMA user_version = 4;
             COMMIT;",
        )?;
    }
    if version < 5 {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE session_targets RENAME TO session_targets_v4;
             CREATE TABLE session_targets (
                 session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                 kind TEXT NOT NULL CHECK(kind IN ('local-bare','local-podman','apple-container','aws-ec2','ssh-bare','ssh-podman')),
                 host TEXT,
                 resource_id TEXT,
                 address TEXT,
                 workspace BLOB,
                 worker_id TEXT,
                 CHECK(
                     (kind = 'local-bare' AND workspace IS NOT NULL
                      AND host IS NULL AND resource_id IS NULL AND address IS NULL AND worker_id IS NULL)
                  OR (kind IN ('local-podman','apple-container') AND resource_id IS NOT NULL
                      AND host IS NULL AND address IS NULL AND workspace IS NULL AND worker_id IS NULL)
                  OR (kind = 'aws-ec2' AND resource_id IS NOT NULL
                      AND host IS NULL AND workspace IS NULL AND worker_id IS NULL)
                  OR (kind = 'ssh-bare' AND host IS NOT NULL AND workspace IS NOT NULL
                      AND resource_id IS NULL AND address IS NULL)
                  OR (kind = 'ssh-podman' AND host IS NOT NULL AND resource_id IS NOT NULL
                      AND address IS NULL AND workspace IS NULL AND worker_id IS NULL)
                 )
             ) STRICT;
             INSERT INTO session_targets
                 SELECT * FROM session_targets_v4;
             DROP TABLE session_targets_v4;
             INSERT INTO schema_migrations(version, applied_at)
                 VALUES (5, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
             PRAGMA user_version = 5;
             COMMIT;",
        )?;
    }
    if version < 6 {
        connection.execute_batch(&format!(
            "BEGIN IMMEDIATE;
             ALTER TABLE session_checkpoints
                 RENAME COLUMN event_sequence TO event_frontier;
             ALTER TABLE prompt_history
                 RENAME COLUMN event_sequence TO event_ordinal;
             ALTER TABLE sessions ADD COLUMN detached_after_event_ordinal INTEGER NOT NULL
                 DEFAULT 0 CHECK(detached_after_event_ordinal >= 0);
             ALTER TABLE sessions ADD COLUMN managed_worktree TEXT;
             CREATE TABLE materialized_sessions (
                 session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                 applied_event_ordinal INTEGER NOT NULL DEFAULT 0 CHECK(applied_event_ordinal >= 0),
                 applied_event_digest TEXT NOT NULL
                     DEFAULT '{RELAY_EVENT_GENESIS_DIGEST}'
                     CHECK(length(applied_event_digest) = 64
                           AND applied_event_digest NOT GLOB '*[^0-9a-f]*'),
                 last_activity_at_ms INTEGER,
                 execution_state TEXT NOT NULL DEFAULT 'idle'
                     CHECK(execution_state IN ('idle','running','closing','closed')),
                 running_started_at_ms INTEGER,
                 session_title TEXT CHECK(session_title IS NULL OR length(trim(session_title)) > 0),
                 configuration_json TEXT NOT NULL DEFAULT '{{}}',
                 CHECK(
                     (execution_state = 'running' AND running_started_at_ms IS NOT NULL)
                     OR (execution_state != 'running' AND running_started_at_ms IS NULL)
                 )
             ) STRICT;
             CREATE TABLE materialized_transcript_items (
                 session_id TEXT NOT NULL REFERENCES materialized_sessions(session_id) ON DELETE CASCADE,
                 stable_id TEXT NOT NULL CHECK(length(trim(stable_id)) > 0),
                 position INTEGER NOT NULL CHECK(position > 0),
                 latest_content_event_ordinal INTEGER
                     CHECK(latest_content_event_ordinal IS NULL
                           OR latest_content_event_ordinal >= position),
                 created_at_ms INTEGER NOT NULL,
                 last_changed_at_ms INTEGER NOT NULL CHECK(last_changed_at_ms >= created_at_ms),
                 body_json TEXT NOT NULL,
                 PRIMARY KEY(session_id, stable_id)
             ) STRICT;
             CREATE INDEX materialized_transcript_position
                 ON materialized_transcript_items(session_id, position, stable_id);
             CREATE TABLE materialized_queued_prompts (
                 session_id TEXT NOT NULL REFERENCES materialized_sessions(session_id) ON DELETE CASCADE,
                 ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                 command_id TEXT NOT NULL CHECK(length(trim(command_id)) > 0),
                 content_json TEXT NOT NULL,
                 queued_at_ms INTEGER NOT NULL,
                 PRIMARY KEY(session_id, ordinal),
                 UNIQUE(session_id, command_id)
             ) STRICT;
             INSERT INTO materialized_sessions(session_id)
                 SELECT session_id FROM sessions;
             INSERT INTO schema_migrations(version, applied_at)
                 VALUES (6, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
             PRAGMA user_version = 6;
             COMMIT;",
        ))?;
    }
    // Both development lines used schema version 6: durable relay projection
    // on this branch and managed raw-session worktrees on master. Structural
    // guards make either already-written v6 database converge before the v7
    // sessions-table rebuild, without inventing a second version-6 ledger row.
    ensure_managed_worktree_column(connection)?;
    if version < 7 {
        ensure_relay_projection_schema(connection)?;
        migrate_destroying_session_state(connection)?;
    }
    ensure_projection_digest_column(connection)?;
    ensure_session_draft_input_column(connection)?;
    if version < 8 {
        // Queue entries gained a kind so a configuration change can wait in the
        // same queue as prompts. Rows written before that are prompts.
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE materialized_queued_prompts
                 ADD COLUMN kind_json TEXT NOT NULL DEFAULT '\"prompt\"';
             INSERT INTO schema_migrations(version, applied_at)
                 VALUES (8, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
             PRAGMA user_version = 8;
             COMMIT;",
        )?;
    }
    // Runs last: it rebuilds `sessions`, so every column the steps above add
    // must already exist to be copied forward.
    if version < 9 {
        migrate_grok_harness_kind(connection)?;
    }
    // Added after the v9 rebuild so the rebuild never has to copy them.
    ensure_session_container_override_columns(connection)?;
    ensure_session_mount_read_only_column(connection)?;
    ensure_materialized_elicitation_column(connection)?;
    if version < 10 {
        migrate_stopped_session_state(connection)?;
    }
    if version < 11 {
        migrate_deepseek_harness_kind(connection)?;
    }
    if version < 12 {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE sessions
                 RENAME COLUMN detached_after_event_ordinal TO viewed_through_event_ordinal;
             UPDATE sessions
                 SET target_template_id = 'localhost'
                 WHERE target_template_id = 'raw-localhost';
             INSERT INTO schema_migrations(version, applied_at)
                 VALUES (12, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
             PRAGMA user_version = 12;
             COMMIT;",
        )?;
    }
    let recorded: Option<i64> =
        connection.query_row("SELECT max(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })?;
    if recorded != Some(SCHEMA_VERSION) {
        bail!(
            "Hel database migration ledger {:?} does not match schema {}",
            recorded,
            SCHEMA_VERSION
        );
    }
    Ok(())
}

fn table_has_column(connection: &Connection, table: &str, column: &str) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info(?1)
                 WHERE name = ?2
             )",
            params![table, column],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

/// Per-session container size overrides. They are additive columns, so
/// databases written before the dashboard could edit them open unchanged.
fn ensure_session_container_override_columns(connection: &Connection) -> Result<()> {
    for column in ["container_cpus", "container_memory"] {
        if !table_has_column(connection, "sessions", column)? {
            connection.execute_batch(&format!(
                "BEGIN IMMEDIATE;
                 ALTER TABLE sessions ADD COLUMN {column} TEXT;
                 COMMIT;"
            ))?;
        }
    }
    Ok(())
}

/// Per-mount read-only flag. It is an additive column, so a database written
/// before the mount editors offered the option opens unchanged and its mounts
/// keep the copy-on-write overlay they were provisioned with.
fn ensure_session_mount_read_only_column(connection: &Connection) -> Result<()> {
    if !table_has_column(connection, "session_mounts", "read_only")? {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE session_mounts ADD COLUMN read_only INTEGER NOT NULL DEFAULT 0;
             COMMIT;",
        )?;
    }
    Ok(())
}

fn ensure_materialized_elicitation_column(connection: &Connection) -> Result<()> {
    if !table_has_column(
        connection,
        "materialized_sessions",
        "pending_elicitations_json",
    )? {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE materialized_sessions
                 ADD COLUMN pending_elicitations_json TEXT NOT NULL DEFAULT '[]';
             COMMIT;",
        )?;
    }
    Ok(())
}

fn ensure_managed_worktree_column(connection: &Connection) -> Result<()> {
    if !table_has_column(connection, "sessions", "managed_worktree")? {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE sessions ADD COLUMN managed_worktree TEXT;
             COMMIT;",
        )?;
    }
    Ok(())
}

/// Complete the relay half of the colliding v6 migration for databases first
/// opened by master, whose v6 contained only `managed_worktree`.
fn ensure_relay_projection_schema(connection: &Connection) -> Result<()> {
    if table_has_column(connection, "sessions", "detached_after_event_ordinal")? {
        return Ok(());
    }
    connection.execute_batch(&format!(
        "BEGIN IMMEDIATE;
         ALTER TABLE session_checkpoints
             RENAME COLUMN event_sequence TO event_frontier;
         ALTER TABLE prompt_history
             RENAME COLUMN event_sequence TO event_ordinal;
         ALTER TABLE sessions ADD COLUMN detached_after_event_ordinal INTEGER NOT NULL
             DEFAULT 0 CHECK(detached_after_event_ordinal >= 0);
         CREATE TABLE materialized_sessions (
             session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
             applied_event_ordinal INTEGER NOT NULL DEFAULT 0 CHECK(applied_event_ordinal >= 0),
             applied_event_digest TEXT NOT NULL
                 DEFAULT '{RELAY_EVENT_GENESIS_DIGEST}'
                 CHECK(length(applied_event_digest) = 64
                       AND applied_event_digest NOT GLOB '*[^0-9a-f]*'),
             last_activity_at_ms INTEGER,
             execution_state TEXT NOT NULL DEFAULT 'idle'
                 CHECK(execution_state IN ('idle','running','closing','closed')),
             running_started_at_ms INTEGER,
             session_title TEXT CHECK(session_title IS NULL OR length(trim(session_title)) > 0),
             configuration_json TEXT NOT NULL DEFAULT '{{}}',
             CHECK(
                 (execution_state = 'running' AND running_started_at_ms IS NOT NULL)
                 OR (execution_state != 'running' AND running_started_at_ms IS NULL)
             )
         ) STRICT;
         CREATE TABLE materialized_transcript_items (
             session_id TEXT NOT NULL REFERENCES materialized_sessions(session_id) ON DELETE CASCADE,
             stable_id TEXT NOT NULL CHECK(length(trim(stable_id)) > 0),
             position INTEGER NOT NULL CHECK(position > 0),
             latest_content_event_ordinal INTEGER
                 CHECK(latest_content_event_ordinal IS NULL
                       OR latest_content_event_ordinal >= position),
             created_at_ms INTEGER NOT NULL,
             last_changed_at_ms INTEGER NOT NULL CHECK(last_changed_at_ms >= created_at_ms),
             body_json TEXT NOT NULL,
             PRIMARY KEY(session_id, stable_id)
         ) STRICT;
         CREATE INDEX materialized_transcript_position
             ON materialized_transcript_items(session_id, position, stable_id);
         CREATE TABLE materialized_queued_prompts (
             session_id TEXT NOT NULL REFERENCES materialized_sessions(session_id) ON DELETE CASCADE,
             ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
             command_id TEXT NOT NULL CHECK(length(trim(command_id)) > 0),
             content_json TEXT NOT NULL,
             queued_at_ms INTEGER NOT NULL,
             PRIMARY KEY(session_id, ordinal),
             UNIQUE(session_id, command_id)
         ) STRICT;
         INSERT INTO materialized_sessions(session_id)
             SELECT session_id FROM sessions;
         COMMIT;",
    ))?;
    Ok(())
}

fn migrate_destroying_session_state(connection: &Connection) -> Result<()> {
    // SQLite cannot widen a CHECK constraint in place. Foreign keys are
    // disabled only around the standard table-rebuild transaction; every
    // child continues to reference the replacement table by the same name.
    connection.execute_batch("PRAGMA foreign_keys = OFF;")?;
    let migration = connection.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE sessions_v7 (
             session_id TEXT PRIMARY KEY REFERENCES session_contexts(session_id),
             title TEXT NOT NULL CHECK(length(trim(title)) > 0),
             harness_kind TEXT NOT NULL CHECK(harness_kind IN ('codex','claude','kimi')),
             last_profile TEXT NOT NULL,
             target_template_id TEXT NOT NULL,
             state TEXT NOT NULL CHECK(state IN (
                 'provisioning','running','disconnected','checkpointing','closing','destroying',
                 'archived','lost','error','destroyed-with-data-loss'
             )),
             native_session_id TEXT,
             acp_session_title TEXT CHECK(acp_session_title IS NULL OR length(trim(acp_session_title)) > 0),
             session_title_override TEXT CHECK(session_title_override IS NULL OR length(trim(session_title_override)) > 0),
             updated_at TEXT NOT NULL,
             detached_after_event_ordinal INTEGER NOT NULL DEFAULT 0
                 CHECK(detached_after_event_ordinal >= 0),
             last_error TEXT,
             resource_allocation TEXT,
             last_checkpoint_error TEXT,
             project_directory BLOB,
             managed_worktree TEXT
         ) STRICT;
         INSERT INTO sessions_v7(
             session_id, title, harness_kind, last_profile, target_template_id, state,
             native_session_id, acp_session_title, session_title_override, updated_at,
             detached_after_event_ordinal, last_error, resource_allocation,
             last_checkpoint_error, project_directory, managed_worktree
         )
         SELECT
             session_id, title, harness_kind, last_profile, target_template_id, state,
             native_session_id, acp_session_title, session_title_override, updated_at,
             detached_after_event_ordinal, last_error, resource_allocation,
             last_checkpoint_error, project_directory, managed_worktree
         FROM sessions;
         DROP TABLE sessions;
         ALTER TABLE sessions_v7 RENAME TO sessions;
         INSERT INTO schema_migrations(version, applied_at)
             VALUES (7, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
         PRAGMA user_version = 7;
         COMMIT;",
    );
    if migration.is_err() {
        let _ = connection.execute_batch("ROLLBACK;");
    }
    let foreign_keys = connection.execute_batch("PRAGMA foreign_keys = ON;");
    migration.context("migrate durable destroying session state")?;
    foreign_keys.context("restore foreign key enforcement after schema migration")?;
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    if statement.exists([])? {
        bail!("foreign key violation after migrating durable destroying session state");
    }
    Ok(())
}

/// Admit the Grok Build harness. SQLite cannot widen a CHECK constraint in
/// place, so this repeats the v7 table rebuild with the wider harness list.
/// Foreign keys are disabled only around the rebuild transaction; every child
/// continues to reference the replacement table by the same name.
fn migrate_grok_harness_kind(connection: &Connection) -> Result<()> {
    connection.execute_batch("PRAGMA foreign_keys = OFF;")?;
    let migration = connection.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE sessions_v9 (
             session_id TEXT PRIMARY KEY REFERENCES session_contexts(session_id),
             title TEXT NOT NULL CHECK(length(trim(title)) > 0),
             harness_kind TEXT NOT NULL CHECK(harness_kind IN ('codex','claude','kimi','grok')),
             last_profile TEXT NOT NULL,
             target_template_id TEXT NOT NULL,
             state TEXT NOT NULL CHECK(state IN (
                 'provisioning','running','disconnected','checkpointing','closing','destroying',
                 'archived','lost','error','destroyed-with-data-loss'
             )),
             native_session_id TEXT,
             acp_session_title TEXT CHECK(acp_session_title IS NULL OR length(trim(acp_session_title)) > 0),
             session_title_override TEXT CHECK(session_title_override IS NULL OR length(trim(session_title_override)) > 0),
             updated_at TEXT NOT NULL,
             detached_after_event_ordinal INTEGER NOT NULL DEFAULT 0
                 CHECK(detached_after_event_ordinal >= 0),
             last_error TEXT,
             resource_allocation TEXT,
             last_checkpoint_error TEXT,
             project_directory BLOB,
             managed_worktree TEXT,
             draft_input TEXT NOT NULL DEFAULT ''
         ) STRICT;
         INSERT INTO sessions_v9(
             session_id, title, harness_kind, last_profile, target_template_id, state,
             native_session_id, acp_session_title, session_title_override, updated_at,
             detached_after_event_ordinal, last_error, resource_allocation,
             last_checkpoint_error, project_directory, managed_worktree, draft_input
         )
         SELECT
             session_id, title, harness_kind, last_profile, target_template_id, state,
             native_session_id, acp_session_title, session_title_override, updated_at,
             detached_after_event_ordinal, last_error, resource_allocation,
             last_checkpoint_error, project_directory, managed_worktree, draft_input
         FROM sessions;
         DROP TABLE sessions;
         ALTER TABLE sessions_v9 RENAME TO sessions;
         INSERT INTO schema_migrations(version, applied_at)
             VALUES (9, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
         PRAGMA user_version = 9;
         COMMIT;",
    );
    if migration.is_err() {
        let _ = connection.execute_batch("ROLLBACK;");
    }
    let foreign_keys = connection.execute_batch("PRAGMA foreign_keys = ON;");
    migration.context("migrate sessions table for the Grok Build harness")?;
    foreign_keys.context("restore foreign key enforcement after schema migration")?;
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    if statement.exists([])? {
        bail!("foreign key violation after migrating the sessions harness list");
    }
    Ok(())
}

/// Rename the `archived` lifecycle state to `stopped` and give sessions their
/// own display-only `archived` flag, which now means "hidden from the resume
/// dialog". SQLite cannot narrow or widen a CHECK constraint in place, so this
/// repeats the v9 table rebuild with the new state list and the new column.
/// It also adds the hidden set for native sessions Hel only reads.
fn migrate_stopped_session_state(connection: &Connection) -> Result<()> {
    connection.execute_batch("PRAGMA foreign_keys = OFF;")?;
    let migration = connection.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE sessions_v10 (
             session_id TEXT PRIMARY KEY REFERENCES session_contexts(session_id),
             title TEXT NOT NULL CHECK(length(trim(title)) > 0),
             harness_kind TEXT NOT NULL CHECK(harness_kind IN ('codex','claude','kimi','grok')),
             last_profile TEXT NOT NULL,
             target_template_id TEXT NOT NULL,
             state TEXT NOT NULL CHECK(state IN (
                 'provisioning','running','disconnected','checkpointing','closing','destroying',
                 'stopped','lost','error','destroyed-with-data-loss'
             )),
             native_session_id TEXT,
             acp_session_title TEXT CHECK(acp_session_title IS NULL OR length(trim(acp_session_title)) > 0),
             session_title_override TEXT CHECK(session_title_override IS NULL OR length(trim(session_title_override)) > 0),
             updated_at TEXT NOT NULL,
             detached_after_event_ordinal INTEGER NOT NULL DEFAULT 0
                 CHECK(detached_after_event_ordinal >= 0),
             last_error TEXT,
             resource_allocation TEXT,
             last_checkpoint_error TEXT,
             project_directory BLOB,
             managed_worktree TEXT,
             draft_input TEXT NOT NULL DEFAULT '',
             container_cpus TEXT,
             container_memory TEXT,
             archived INTEGER NOT NULL DEFAULT 0 CHECK(archived IN (0, 1))
         ) STRICT;
         INSERT INTO sessions_v10(
             session_id, title, harness_kind, last_profile, target_template_id, state,
             native_session_id, acp_session_title, session_title_override, updated_at,
             detached_after_event_ordinal, last_error, resource_allocation,
             last_checkpoint_error, project_directory, managed_worktree, draft_input,
             container_cpus, container_memory
         )
         SELECT
             session_id, title, harness_kind, last_profile, target_template_id,
             CASE state WHEN 'archived' THEN 'stopped' ELSE state END,
             native_session_id, acp_session_title, session_title_override, updated_at,
             detached_after_event_ordinal, last_error, resource_allocation,
             last_checkpoint_error, project_directory, managed_worktree, draft_input,
             container_cpus, container_memory
         FROM sessions;
         DROP TABLE sessions;
         ALTER TABLE sessions_v10 RENAME TO sessions;
         CREATE TABLE hidden_native_sessions (
             harness_kind TEXT NOT NULL CHECK(harness_kind IN ('codex','claude','kimi','grok')),
             native_session_id TEXT NOT NULL CHECK(length(trim(native_session_id)) > 0),
             hidden_at TEXT NOT NULL,
             PRIMARY KEY(harness_kind, native_session_id)
         ) STRICT;
         INSERT INTO schema_migrations(version, applied_at)
             VALUES (10, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
         PRAGMA user_version = 10;
         COMMIT;",
    );
    if migration.is_err() {
        let _ = connection.execute_batch("ROLLBACK;");
    }
    let foreign_keys = connection.execute_batch("PRAGMA foreign_keys = ON;");
    migration.context("migrate sessions table for the stopped session state")?;
    foreign_keys.context("restore foreign key enforcement after schema migration")?;
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    if statement.exists([])? {
        bail!("foreign key violation after migrating the stopped session state");
    }
    Ok(())
}

/// Admit DeepSeek Harness in both stored sessions and Hel's native-session
/// hidden set. SQLite requires rebuilding tables to widen CHECK constraints.
fn migrate_deepseek_harness_kind(connection: &Connection) -> Result<()> {
    connection.execute_batch("PRAGMA foreign_keys = OFF;")?;
    let migration = connection.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE sessions_v11 (
             session_id TEXT PRIMARY KEY REFERENCES session_contexts(session_id),
             title TEXT NOT NULL CHECK(length(trim(title)) > 0),
             harness_kind TEXT NOT NULL CHECK(harness_kind IN ('codex','claude','kimi','grok','deepseek')),
             last_profile TEXT NOT NULL,
             target_template_id TEXT NOT NULL,
             state TEXT NOT NULL CHECK(state IN (
                 'provisioning','running','disconnected','checkpointing','closing','destroying',
                 'stopped','lost','error','destroyed-with-data-loss'
             )),
             native_session_id TEXT,
             acp_session_title TEXT CHECK(acp_session_title IS NULL OR length(trim(acp_session_title)) > 0),
             session_title_override TEXT CHECK(session_title_override IS NULL OR length(trim(session_title_override)) > 0),
             updated_at TEXT NOT NULL,
             detached_after_event_ordinal INTEGER NOT NULL DEFAULT 0
                 CHECK(detached_after_event_ordinal >= 0),
             last_error TEXT,
             resource_allocation TEXT,
             last_checkpoint_error TEXT,
             project_directory BLOB,
             managed_worktree TEXT,
             draft_input TEXT NOT NULL DEFAULT '',
             container_cpus TEXT,
             container_memory TEXT,
             archived INTEGER NOT NULL DEFAULT 0 CHECK(archived IN (0, 1))
         ) STRICT;
         INSERT INTO sessions_v11 SELECT * FROM sessions;
         DROP TABLE sessions;
         ALTER TABLE sessions_v11 RENAME TO sessions;
         ALTER TABLE hidden_native_sessions RENAME TO hidden_native_sessions_v10;
         CREATE TABLE hidden_native_sessions (
             harness_kind TEXT NOT NULL CHECK(harness_kind IN ('codex','claude','kimi','grok','deepseek')),
             native_session_id TEXT NOT NULL CHECK(length(trim(native_session_id)) > 0),
             hidden_at TEXT NOT NULL,
             PRIMARY KEY(harness_kind, native_session_id)
         ) STRICT;
         INSERT INTO hidden_native_sessions SELECT * FROM hidden_native_sessions_v10;
         DROP TABLE hidden_native_sessions_v10;
         INSERT INTO schema_migrations(version, applied_at)
             VALUES (11, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
         PRAGMA user_version = 11;
         COMMIT;",
    );
    if migration.is_err() {
        let _ = connection.execute_batch("ROLLBACK;");
    }
    let foreign_keys = connection.execute_batch("PRAGMA foreign_keys = ON;");
    migration.context("migrate sessions table for DeepSeek Harness")?;
    foreign_keys.context("restore foreign key enforcement after schema migration")?;
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    if statement.exists([])? {
        bail!("foreign key violation after migrating the DeepSeek Harness list");
    }
    Ok(())
}

/// Carry unsent chat input across a detach. Added as a structural guard rather
/// than a new schema version so databases written by either development line
/// converge, matching `ensure_managed_worktree_column`.
fn ensure_session_draft_input_column(connection: &Connection) -> Result<()> {
    if !table_has_column(connection, "sessions", "draft_input")? {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE sessions ADD COLUMN draft_input TEXT NOT NULL DEFAULT '';
             COMMIT;",
        )?;
    }
    Ok(())
}

fn ensure_projection_digest_column(connection: &Connection) -> Result<()> {
    let present = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM pragma_table_info('materialized_sessions')
             WHERE name = 'applied_event_digest'
         )",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !present {
        connection.execute_batch(&format!(
            "BEGIN IMMEDIATE;
             ALTER TABLE materialized_sessions ADD COLUMN applied_event_digest TEXT NOT NULL
                 DEFAULT '{RELAY_EVENT_GENESIS_DIGEST}'
                 CHECK(length(applied_event_digest) = 64
                       AND applied_event_digest NOT GLOB '*[^0-9a-f]*');
             COMMIT;",
        ))?;
    }
    Ok(())
}

pub fn load_state() -> Result<HelState> {
    load_state_from(&database_path())
}

pub fn load_state_from(path: &Path) -> Result<HelState> {
    let connection = open(path)?;
    let mut state = HelState::default();
    let mut statement = connection.prepare(
        "SELECT s.session_id, s.title, s.harness_kind, s.last_profile, c.bundle_id,
                s.target_template_id, s.state, s.native_session_id, s.acp_session_title,
                s.session_title_override, c.created_at, s.updated_at,
                s.viewed_through_event_ordinal, s.last_error, s.resource_allocation,
                s.last_checkpoint_error, s.project_directory, s.managed_worktree,
                s.draft_input, s.container_cpus, s.container_memory, s.archived
         FROM sessions s JOIN session_contexts c USING(session_id)
         ORDER BY s.session_id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(SessionRecord {
            archived: row.get(21)?,
            container_cpus: row.get(19)?,
            container_memory: row.get(20)?,
            id: row.get(0)?,
            title: row.get(1)?,
            harness_kind: row.get::<_, String>(2)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::<dyn std::error::Error + Send + Sync>::from(format!("{error:#}")),
                )
            })?,
            last_profile: row.get(3)?,
            bundle_id: row.get(4)?,
            project_directory: row.get_ref(16)?.blob_or_null()?.map(blob_to_path),
            managed_worktree: row
                .get::<_, Option<String>>(17)?
                .map(|json| serde_json::from_str::<ManagedWorktree>(&json))
                .transpose()
                .map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        17,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
            target_template_id: row.get(5)?,
            resource_allocation: row
                .get::<_, Option<String>>(14)?
                .map(|json| serde_json::from_str::<SessionResourceAllocation>(&json))
                .transpose()
                .map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        14,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
            additional_mounts: Vec::new(),
            state: parse_session_state(&row.get::<_, String>(6)?),
            target: None,
            native_session_id: row.get(7)?,
            acp_session_title: row
                .get::<_, Option<String>>(8)?
                .as_deref()
                .and_then(crate::hel_state::normalize_session_title),
            session_title_override: row.get(9)?,
            created_at: row.get(10)?,
            updated_at: row.get(11)?,
            viewed_through_event_ordinal: row.get::<_, u64>(12)?,
            draft_input: row.get(18)?,
            last_error: row.get(13)?,
            last_checkpoint_error: row.get(15)?,
            checkpoint: None,
        })
    })?;
    for row in rows {
        let session = row?;
        state.sessions.insert(session.id.clone(), session);
    }
    load_targets(&connection, &mut state)?;
    load_mounts(&connection, &mut state)?;
    load_checkpoints(&connection, &mut state)?;
    let mut statement =
        connection.prepare("SELECT host, source FROM mount_history ORDER BY host, ordinal")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            blob_to_path(row.get_ref(1)?.as_blob()?),
        ))
    })?;
    for row in rows {
        let (host, source) = row?;
        state.mount_history.entry(host).or_default().push(source);
    }
    state.validate()?;
    Ok(state)
}

pub fn save_state(state: &HelState) -> Result<()> {
    save_state_to(&database_path(), state)
}

/// Persist one operational session without rewriting unrelated controller
/// state. Dashboard lifecycle jobs use this path so independent jobs can
/// commit concurrently without restoring stale copies of other sessions.
pub fn save_session(session: &SessionRecord) -> Result<()> {
    save_session_to(&database_path(), session)
}

/// Update only the fields a lifecycle transition owns on a session that
/// already exists. Everything else — display titles, checkpoints, container
/// settings, and attached directories — stays with its own writer.
pub fn save_lifecycle_session(session: &SessionRecord) -> Result<()> {
    save_lifecycle_session_to(&database_path(), session)
}

/// Install a lifecycle transition together with the checkpoint it just
/// verified and the harness session id that produced it.
pub fn save_checkpointed_session(session: &SessionRecord) -> Result<()> {
    save_checkpointed_session_to(&database_path(), session)
}

/// Recover lifecycle rows stranded by a process exit during checkpoint
/// creation. This must be called once by the top-level controller process
/// while it owns the controller-store guard, not by per-operation reloads.
pub fn recover_interrupted_checkpointing_sessions(updated_at: &str) -> Result<usize> {
    recover_interrupted_checkpointing_sessions_to(&database_path(), updated_at)
}

/// Change only the user-owned display name. This avoids writing a stale
/// SessionRecord over independently committed checkpoint or relay metadata.
pub fn set_session_title_override(session_id: &str, title: &str, updated_at: &str) -> Result<()> {
    set_session_title_override_to(&database_path(), session_id, title, updated_at)
}

/// Change only whether the resume dialog hides this session. Archiving is a
/// display choice, so it has its own writer and never rewrites lifecycle,
/// checkpoint, or title columns another task owns.
pub fn set_session_archived(session_id: &str, archived: bool) -> Result<()> {
    set_session_archived_to(&database_path(), session_id, archived)
}

/// Record that the managed target of an otherwise live session is definitively
/// gone. The state predicate keeps a late poll result from overwriting a
/// concurrent checkpoint or teardown transition.
pub fn mark_session_target_lost(session_id: &str, detail: &str, updated_at: &str) -> Result<bool> {
    mark_session_target_lost_to(&database_path(), session_id, detail, updated_at)
}

fn mark_session_target_lost_to(
    path: &Path,
    session_id: &str,
    detail: &str,
    updated_at: &str,
) -> Result<bool> {
    let connection = open(path)?;
    let changed = connection.execute(
        "UPDATE sessions
         SET state = 'lost', last_error = ?2, updated_at = ?3
         WHERE session_id = ?1
           AND state IN ('provisioning', 'running', 'disconnected', 'error')",
        params![session_id, detail, updated_at],
    )?;
    ensure!(changed <= 1, "updated {changed} sessions for {session_id}");
    Ok(changed == 1)
}

fn set_session_archived_to(path: &Path, session_id: &str, archived: bool) -> Result<()> {
    let connection = open(path)?;
    let changed = connection.execute(
        "UPDATE sessions SET archived = ?2 WHERE session_id = ?1",
        params![session_id, archived],
    )?;
    if changed != 1 {
        bail!("unknown session {session_id}");
    }
    Ok(())
}

/// Native sessions the resume dialog hides. Hel never writes into a harness
/// home, so the hidden set lives here instead of in the harness's own store.
pub fn hidden_native_sessions() -> Result<BTreeSet<(crate::hel_config::HarnessKind, String)>> {
    hidden_native_sessions_from(&database_path())
}

fn hidden_native_sessions_from(
    path: &Path,
) -> Result<BTreeSet<(crate::hel_config::HarnessKind, String)>> {
    let connection = open(path)?;
    let mut statement =
        connection.prepare("SELECT harness_kind, native_session_id FROM hidden_native_sessions")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut hidden = BTreeSet::new();
    for row in rows {
        let (harness, native_session_id) = row?;
        let harness = harness
            .parse::<crate::hel_config::HarnessKind>()
            .with_context(|| format!("unknown harness {harness:?} in the hidden session set"))?;
        hidden.insert((harness, native_session_id));
    }
    Ok(hidden)
}

/// Hide or reveal one native session in the resume dialog.
pub fn set_native_session_hidden(
    harness: crate::hel_config::HarnessKind,
    native_session_id: &str,
    hidden: bool,
) -> Result<()> {
    set_native_session_hidden_to(&database_path(), harness, native_session_id, hidden)
}

fn set_native_session_hidden_to(
    path: &Path,
    harness: crate::hel_config::HarnessKind,
    native_session_id: &str,
    hidden: bool,
) -> Result<()> {
    if native_session_id.trim().is_empty() {
        bail!("native session id is empty");
    }
    let connection = open(path)?;
    if hidden {
        connection.execute(
            "INSERT INTO hidden_native_sessions(harness_kind, native_session_id, hidden_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(harness_kind, native_session_id) DO NOTHING",
            params![harness.id(), native_session_id, Utc::now().to_rfc3339()],
        )?;
    } else {
        connection.execute(
            "DELETE FROM hidden_native_sessions
             WHERE harness_kind = ?1 AND native_session_id = ?2",
            params![harness.id(), native_session_id],
        )?;
    }
    Ok(())
}

/// Change only the per-session container provisioning inputs: the size
/// overrides and the attached directories. Everything else the session row
/// owns is left to its own writer.
pub fn set_session_container_settings(
    session_id: &str,
    cpus: Option<&str>,
    memory: Option<&str>,
    mounts: &[AdditionalMount],
    updated_at: &str,
) -> Result<()> {
    set_session_container_settings_to(
        &database_path(),
        session_id,
        cpus,
        memory,
        mounts,
        updated_at,
    )
}

fn set_session_container_settings_to(
    path: &Path,
    session_id: &str,
    cpus: Option<&str>,
    memory: Option<&str>,
    mounts: &[AdditionalMount],
    updated_at: &str,
) -> Result<()> {
    if updated_at.trim().is_empty() {
        bail!("session update timestamp is empty");
    }
    crate::hel_targets::validate_additional_mounts(mounts)?;
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let changed = tx.execute(
        "UPDATE sessions
         SET container_cpus = ?2, container_memory = ?3, updated_at = ?4
         WHERE session_id = ?1",
        params![session_id, cpus, memory, updated_at],
    )?;
    if changed != 1 {
        bail!("unknown session {session_id}");
    }
    tx.execute(
        "DELETE FROM session_mounts WHERE session_id = ?1",
        [session_id],
    )?;
    for (ordinal, mount) in mounts.iter().enumerate() {
        tx.execute(
            "INSERT INTO session_mounts(session_id, ordinal, source, destination, read_only)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session_id,
                ordinal as i64,
                path_to_blob(&mount.source),
                path_to_blob(&mount.destination),
                mount.read_only
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

fn set_session_title_override_to(
    path: &Path,
    session_id: &str,
    title: &str,
    updated_at: &str,
) -> Result<()> {
    if title.trim().is_empty() {
        bail!("session title is empty");
    }
    if updated_at.trim().is_empty() {
        bail!("session update timestamp is empty");
    }
    let connection = open(path)?;
    let changed = connection.execute(
        "UPDATE sessions
         SET session_title_override = ?2, updated_at = ?3
         WHERE session_id = ?1",
        params![session_id, title, updated_at],
    )?;
    if changed != 1 {
        bail!("unknown session {session_id}");
    }
    Ok(())
}

/// Persist the latest ACP-provided title without replacing unrelated session
/// fields that may have changed in another supervised controller task.
pub fn set_session_acp_title(session_id: &str, title: Option<&str>) -> Result<()> {
    set_session_acp_title_to(&database_path(), session_id, title)
}

fn set_session_acp_title_to(path: &Path, session_id: &str, title: Option<&str>) -> Result<()> {
    if title.is_some_and(|title| title.trim().is_empty()) {
        bail!("ACP session title is empty");
    }
    let title = title.and_then(crate::hel_state::normalize_session_title);
    let connection = open(path)?;
    let changed = connection.execute(
        "UPDATE sessions SET acp_session_title = ?2 WHERE session_id = ?1",
        params![session_id, title],
    )?;
    if changed != 1 {
        bail!("unknown session {session_id}");
    }
    Ok(())
}

/// Commit the successful handshake for a newly provisioned worker without
/// replacing checkpoint or display metadata owned by other controller tasks.
pub fn mark_session_worker_connected(
    session_id: &str,
    native_session_id: Option<&str>,
    updated_at: &str,
) -> Result<()> {
    if updated_at.trim().is_empty() {
        bail!("worker connection timestamp is empty");
    }
    let connection = open(&database_path())?;
    let changed = connection.execute(
        "UPDATE sessions
         SET state = 'running',
             native_session_id = coalesce(?2, native_session_id),
             updated_at = ?3,
             last_error = NULL
         WHERE session_id = ?1",
        params![session_id, native_session_id, updated_at],
    )?;
    if changed != 1 {
        bail!("unknown session {session_id}");
    }
    Ok(())
}

fn recover_interrupted_checkpointing_sessions_to(path: &Path, updated_at: &str) -> Result<usize> {
    if updated_at.trim().is_empty() {
        bail!("checkpoint recovery timestamp is empty");
    }
    let connection = open(path)?;
    connection
        .execute(
            "UPDATE sessions
             SET state = 'running', updated_at = ?1, last_checkpoint_error = ?2
             WHERE state = 'checkpointing'",
            params![
                updated_at,
                "checkpointing was interrupted by a controller restart; the target was left running"
            ],
        )
        .context("recover interrupted checkpointing sessions")
}

fn save_session_to(path: &Path, session: &SessionRecord) -> Result<()> {
    validate_session_record(session)?;

    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if let Some(existing_bundle) = tx
        .query_row(
            "SELECT bundle_id FROM session_contexts WHERE session_id = ?1",
            [session.id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        && existing_bundle != session.bundle_id
    {
        bail!(
            "session {} was already associated with bundle {}, not {}",
            session.id,
            existing_bundle,
            session.bundle_id
        );
    }
    insert_session(&tx, session)?;
    tx.commit()?;
    Ok(())
}

fn save_lifecycle_session_to(path: &Path, session: &SessionRecord) -> Result<()> {
    validate_session_record(session)?;

    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    update_lifecycle_fields(&tx, session)?;
    tx.commit()?;
    Ok(())
}

fn save_checkpointed_session_to(path: &Path, session: &SessionRecord) -> Result<()> {
    validate_session_record(session)?;

    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    update_lifecycle_fields(&tx, session)?;
    tx.execute(
        "UPDATE sessions SET native_session_id = ?2 WHERE session_id = ?1",
        params![session.id, session.native_session_id],
    )?;
    replace_checkpoint(&tx, session)?;
    tx.commit()?;
    Ok(())
}

fn validate_session_record(session: &SessionRecord) -> Result<()> {
    let mut validation = HelState::default();
    validation
        .sessions
        .insert(session.id.clone(), session.clone());
    validation.validate()
}

/// Remove one operational session while retaining its relational history
/// context and prompt history.
pub fn delete_session(session_id: &str) -> Result<()> {
    let connection = open(&database_path())?;
    connection.execute("DELETE FROM sessions WHERE session_id = ?1", [session_id])?;
    Ok(())
}

pub fn load_materialized_session(session_id: &str) -> Result<Option<MaterializedSession>> {
    load_materialized_session_from(&database_path(), session_id)
}

/// Load only the projection fields needed by dashboard session summaries.
/// Transcript bodies for tools, plans, thoughts, and old messages stay in
/// SQLite, which keeps dashboard startup independent of transcript size.
pub fn load_materialized_session_summary(
    session_id: &str,
) -> Result<Option<MaterializedSessionSummary>> {
    load_materialized_session_summary_from(&database_path(), session_id)
}

fn load_materialized_session_summary_from(
    path: &Path,
    session_id: &str,
) -> Result<Option<MaterializedSessionSummary>> {
    let connection = open(path)?;
    let row = connection
        .query_row(
            "SELECT applied_event_ordinal, last_activity_at_ms, execution_state,
                    running_started_at_ms, session_title
             FROM materialized_sessions WHERE session_id = ?1",
            [session_id],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((
        applied_event_ordinal,
        last_activity_at_ms,
        execution,
        running_started_at_ms,
        session_title,
    )) = row
    else {
        return Ok(None);
    };

    let last_user_message = last_materialized_user_message(&connection, session_id)?;
    let last_agent_message = last_materialized_agent_message(&connection, session_id)?;
    let last_agent_message_follows_last_user =
        last_agent_message
            .as_ref()
            .is_some_and(|(agent_position, _)| {
                last_user_message
                    .as_ref()
                    .is_none_or(|(user_position, _)| agent_position > user_position)
            });
    let mut ordinal_statement = connection.prepare(
        "SELECT latest_content_event_ordinal
         FROM materialized_transcript_items
         WHERE session_id = ?1
           AND latest_content_event_ordinal IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM json_each(
                   CASE
                       WHEN latest_content_event_ordinal IS NOT NULL
                           AND json_valid(body_json)
                       THEN body_json
                       ELSE '{}'
                   END,
                   '$.chunks'
               ) AS chunk
               WHERE json_extract(chunk.value, '$.content.type') IS NOT NULL
                 AND (
                     json_extract(chunk.value, '$.content.type') <> 'text'
                     OR trim(coalesce(json_extract(chunk.value, '$.content.text'), '')) <> ''
                 )
           )
         ORDER BY position, stable_id",
    )?;
    let agent_message_latest_content_ordinals = ordinal_statement
        .query_map([session_id], |row| row.get::<_, u64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(Some(MaterializedSessionSummary {
        session_id: session_id.to_owned(),
        applied_event_ordinal,
        last_activity_at_ms,
        execution: parse_materialized_execution(&execution, running_started_at_ms)?,
        session_title,
        last_agent_message: last_agent_message.map(|(_, message)| message),
        last_user_message: last_user_message.map(|(_, message)| message),
        last_agent_message_follows_last_user,
        agent_message_latest_content_ordinals,
    }))
}

fn last_materialized_user_message(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<(u64, String)>> {
    let mut statement = connection.prepare(
        "SELECT position, body_json
         FROM materialized_transcript_items
         WHERE session_id = ?1
           AND json_extract(
               CASE
                   WHEN stable_id GLOB 'user:*' OR stable_id GLOB 'user-*'
                   THEN body_json
                   ELSE '{}'
               END,
               '$.kind'
           ) = 'user'
         ORDER BY position DESC, stable_id DESC",
    )?;
    let rows = statement.query_map([session_id], |row| {
        Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (position, body_json) = row?;
        let body: TranscriptBody = serde_json::from_str(&body_json)
            .with_context(|| format!("parse materialized user message for session {session_id}"))?;
        let TranscriptBody::User { content } = body else {
            continue;
        };
        let text = crate::hel_chat::materialized_content_text(&content);
        if !text.trim().is_empty() {
            return Ok(Some((position, text)));
        }
    }
    Ok(None)
}

fn last_materialized_agent_message(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<(u64, String)>> {
    let row = connection
        .query_row(
            "SELECT position, body_json
             FROM materialized_transcript_items
             WHERE session_id = ?1
               AND latest_content_event_ordinal IS NOT NULL
               AND EXISTS (
                   SELECT 1 FROM json_each(
                       CASE
                           WHEN latest_content_event_ordinal IS NOT NULL
                               AND json_valid(body_json)
                           THEN body_json
                           ELSE '{}'
                       END,
                       '$.chunks'
                   ) AS chunk
                   WHERE json_extract(chunk.value, '$.content.type') IS NOT NULL
                     AND (
                         json_extract(chunk.value, '$.content.type') <> 'text'
                         OR trim(coalesce(json_extract(chunk.value, '$.content.text'), '')) <> ''
                     )
               )
             ORDER BY position DESC, stable_id DESC
             LIMIT 1",
            [session_id],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((position, body_json)) = row else {
        return Ok(None);
    };
    let body: TranscriptBody = serde_json::from_str(&body_json)
        .with_context(|| format!("parse materialized agent message for session {session_id}"))?;
    let TranscriptBody::Agent { chunks, .. } = body else {
        return Ok(None);
    };
    let text = crate::hel_chat::materialized_chunks_text(&chunks);
    Ok((!text.trim().is_empty()).then_some((position, text)))
}

/// Read only the projection's event frontier. Deciding whether a stored
/// projection already matches an archive costs one row this way, instead of
/// deserializing every transcript item to compare two integers.
pub fn materialized_event_frontier(session_id: &str) -> Result<Option<(u64, String)>> {
    materialized_event_frontier_from(&database_path(), session_id)
}

fn materialized_event_frontier_from(
    path: &Path,
    session_id: &str,
) -> Result<Option<(u64, String)>> {
    Ok(open(path)?
        .query_row(
            "SELECT applied_event_ordinal, applied_event_digest
             FROM materialized_sessions WHERE session_id = ?1",
            [session_id],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?)
}

/// Replace a session's durable prompt queue without touching its transcript or
/// event frontier. Resume uses this when it keeps the stored projection but
/// still has to drop the queue the archive carried.
pub fn replace_materialized_queued_prompts(
    session_id: &str,
    queued_prompts: &[MaterializedQueuedPrompt],
) -> Result<()> {
    replace_materialized_queued_prompts_in(&database_path(), session_id, queued_prompts)
}

fn replace_materialized_queued_prompts_in(
    path: &Path,
    session_id: &str,
    queued_prompts: &[MaterializedQueuedPrompt],
) -> Result<()> {
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if !session_exists(&tx, session_id)? {
        bail!("unknown session {session_id}");
    }
    replace_materialized_queue(&tx, session_id, queued_prompts)?;
    tx.commit()?;
    Ok(())
}

/// Load only the durable prompt queues without deserializing transcript rows.
/// Dashboard startup uses this path so work is proportional to queued prompts,
/// not to the complete retained conversation history.
pub fn load_materialized_queued_prompts() -> Result<BTreeMap<String, Vec<MaterializedQueuedPrompt>>>
{
    load_materialized_queued_prompts_from(&database_path())
}

fn load_materialized_queued_prompts_from(
    path: &Path,
) -> Result<BTreeMap<String, Vec<MaterializedQueuedPrompt>>> {
    let connection = open(path)?;
    let mut statement = connection.prepare(
        "SELECT session_id, command_id, kind_json, content_json, queued_at_ms
         FROM materialized_queued_prompts
         ORDER BY session_id, ordinal",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
        ))
    })?;
    let mut queues = BTreeMap::<String, Vec<MaterializedQueuedPrompt>>::new();
    for row in rows {
        let (session_id, command_id, kind_json, content_json, queued_at_ms) = row?;
        let content = serde_json::from_str(&content_json).with_context(|| {
            format!("parse materialized queued prompt for session {session_id}")
        })?;
        let kind = serde_json::from_str(&kind_json).with_context(|| {
            format!("parse materialized queue entry kind for session {session_id}")
        })?;
        queues
            .entry(session_id)
            .or_default()
            .push(MaterializedQueuedPrompt {
                command_id,
                kind,
                content,
                queued_at_ms,
            });
    }
    Ok(queues)
}

fn load_materialized_session_from(
    path: &Path,
    session_id: &str,
) -> Result<Option<MaterializedSession>> {
    let connection = open(path)?;
    load_materialized_session_with(&connection, session_id)
}

fn load_materialized_session_with(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<MaterializedSession>> {
    let row = connection
        .query_row(
            "SELECT applied_event_ordinal, applied_event_digest, last_activity_at_ms,
                    execution_state, running_started_at_ms, session_title, configuration_json,
                    pending_elicitations_json
             FROM materialized_sessions WHERE session_id = ?1",
            [session_id],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .optional()?;
    let Some((
        applied_event_ordinal,
        applied_event_digest,
        last_activity_at_ms,
        execution,
        running_started_at_ms,
        session_title,
        configuration_json,
        pending_elicitations_json,
    )) = row
    else {
        return Ok(None);
    };

    let configuration = serde_json::from_str(&configuration_json)
        .with_context(|| format!("parse materialized configuration for session {session_id}"))?;
    let pending_elicitations = serde_json::from_str(&pending_elicitations_json)
        .with_context(|| format!("parse pending elicitations for session {session_id}"))?;
    let mut transcript_statement = connection.prepare(
        "SELECT stable_id, position, latest_content_event_ordinal, created_at_ms,
                last_changed_at_ms, body_json
         FROM materialized_transcript_items
         WHERE session_id = ?1
         ORDER BY position, stable_id",
    )?;
    let transcript_rows = transcript_statement
        .query_map([session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u64>(1)?,
                row.get::<_, Option<u64>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let transcript = transcript_rows
        .into_iter()
        .map(
            |(
                stable_id,
                position,
                latest_content_event_ordinal,
                created_at_ms,
                last_changed_at_ms,
                body_json,
            )| {
                Ok(Arc::new(TranscriptItem {
                    stable_id,
                    position,
                    latest_content_event_ordinal,
                    created_at_ms,
                    last_changed_at_ms,
                    body: serde_json::from_str(&body_json).with_context(|| {
                        format!("parse materialized transcript body for session {session_id}")
                    })?,
                }))
            },
        )
        .collect::<Result<Vec<_>>>()?;

    let mut queue_statement = connection.prepare(
        "SELECT command_id, kind_json, content_json, queued_at_ms
         FROM materialized_queued_prompts
         WHERE session_id = ?1
         ORDER BY ordinal",
    )?;
    let queue_rows = queue_statement
        .query_map([session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let queued_prompts = queue_rows
        .into_iter()
        .map(|(command_id, kind_json, content_json, queued_at_ms)| {
            Ok(MaterializedQueuedPrompt {
                command_id,
                kind: serde_json::from_str(&kind_json).with_context(|| {
                    format!("parse materialized queue entry kind for session {session_id}")
                })?,
                content: serde_json::from_str(&content_json).with_context(|| {
                    format!("parse materialized queued prompt for session {session_id}")
                })?,
                queued_at_ms,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let materialized = MaterializedSession {
        session_id: session_id.to_owned(),
        applied_event_ordinal,
        applied_event_digest,
        last_activity_at_ms,
        execution: parse_materialized_execution(&execution, running_started_at_ms)?,
        session_title,
        configuration,
        transcript,
        queued_prompts,
        pending_elicitations,
    };
    materialized.validate()?;
    Ok(Some(materialized))
}

/// Replace a complete projection, primarily when seeding a restored
/// checkpoint. Operational `SessionRecord` metadata and read receipts are not
/// modified.
pub fn save_materialized_session(materialized: &MaterializedSession) -> Result<()> {
    save_materialized_session_to(&database_path(), materialized)
}

fn save_materialized_session_to(path: &Path, materialized: &MaterializedSession) -> Result<()> {
    materialized.validate()?;
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if !session_exists(&tx, &materialized.session_id)? {
        bail!("unknown session {}", materialized.session_id);
    }
    write_materialized_session(&tx, materialized)?;
    tx.commit()?;
    Ok(())
}

/// One relay page being applied inside a single write transaction. The relay
/// retains everything past the last acknowledgement, so a page that fails
/// part-way rolls back to the previous durable frontier and is simply
/// redelivered. Only a committed page may be acknowledged.
pub struct ProjectionPage<'a> {
    session_id: &'a str,
    transaction: Transaction<'a>,
    applied_ordinal: u64,
    applied_digest: String,
    dirty: bool,
    pending: MaterializedSessionMutation,
    pending_transcript: BTreeMap<String, PendingTranscriptMutation>,
}

struct PendingTranscriptMutation {
    final_mutation: TranscriptMutation,
    remove_before_upsert: bool,
}

impl ProjectionPage<'_> {
    /// Apply the projection effects of the next relay event to the open page.
    /// The event must continue the chain the page has reached so far, which is
    /// the persisted frontier plus every event already applied to this page.
    pub fn apply(
        &mut self,
        event_ordinal: u64,
        previous_event_digest: &str,
        event_digest: &str,
        mutation: &MaterializedSessionMutation,
    ) -> Result<ProjectionApplyOutcome> {
        if event_ordinal == 0 {
            bail!("relay event ordinal must be positive");
        }
        validate_relay_event_digest(previous_event_digest, "previous relay event digest")?;
        validate_relay_event_frontier(event_ordinal, event_digest, "relay event frontier")?;
        let session_id = self.session_id;
        let applied = self.applied_ordinal;
        if event_ordinal < applied {
            return Ok(ProjectionApplyOutcome::AlreadyApplied);
        }
        if event_ordinal == applied {
            if event_digest != self.applied_digest {
                bail!(
                    "relay event digest mismatch for session {session_id} at ordinal {event_ordinal}: projection has {}, received {event_digest}",
                    self.applied_digest
                );
            }
            return Ok(ProjectionApplyOutcome::AlreadyApplied);
        }
        let expected = applied
            .checked_add(1)
            .context("materialized event ordinal overflow")?;
        if event_ordinal != expected {
            bail!(
                "relay event gap for session {session_id}: expected ordinal {expected}, received {event_ordinal}"
            );
        }
        if previous_event_digest != self.applied_digest {
            bail!(
                "relay event chain diverged for session {session_id} before ordinal {event_ordinal}: projection has {}, event follows {previous_event_digest}",
                self.applied_digest
            );
        }

        if let Some(activity_at_ms) = mutation.last_activity_at_ms {
            self.pending.last_activity_at_ms = Some(
                self.pending
                    .last_activity_at_ms
                    .map_or(activity_at_ms, |existing| existing.max(activity_at_ms)),
            );
        }
        if let Some(execution) = mutation.execution {
            self.pending.execution = Some(execution);
        }
        if let Some(title) = &mutation.session_title {
            if title.as_ref().is_some_and(|title| title.trim().is_empty()) {
                bail!("materialized session title cannot be empty");
            }
            self.pending.session_title = Some(title.clone());
        }
        if let Some(configuration) = &mutation.configuration {
            self.pending.configuration = Some(configuration.clone());
        }
        for item_mutation in &mutation.transcript {
            match item_mutation {
                TranscriptMutation::Upsert(item) => {
                    item.validate(event_ordinal)?;
                    let stable_id = item.stable_id.clone();
                    let entry = self.pending_transcript.entry(stable_id).or_insert_with(|| {
                        PendingTranscriptMutation {
                            final_mutation: TranscriptMutation::Upsert(item.clone()),
                            remove_before_upsert: false,
                        }
                    });
                    entry.remove_before_upsert |=
                        matches!(&entry.final_mutation, TranscriptMutation::Remove { .. });
                    entry.final_mutation = TranscriptMutation::Upsert(item.clone());
                }
                TranscriptMutation::Remove { stable_id } => {
                    if stable_id.trim().is_empty() {
                        bail!("cannot remove a transcript item with an empty stable id");
                    }
                    let removed = TranscriptMutation::Remove {
                        stable_id: stable_id.clone(),
                    };
                    self.pending_transcript
                        .entry(stable_id.clone())
                        .and_modify(|entry| entry.final_mutation = removed.clone())
                        .or_insert(PendingTranscriptMutation {
                            final_mutation: removed,
                            remove_before_upsert: false,
                        });
                }
            }
        }
        if let Some(queued_prompts) = &mutation.queued_prompts {
            self.pending.queued_prompts = Some(queued_prompts.clone());
        }
        if let Some(pending_elicitations) = &mutation.pending_elicitations {
            self.pending.pending_elicitations = Some(pending_elicitations.clone());
        }
        self.applied_ordinal = event_ordinal;
        event_digest.clone_into(&mut self.applied_digest);
        self.dirty = true;
        Ok(ProjectionApplyOutcome::Applied)
    }

    /// Persist the coalesced final state of this page. Intermediate event
    /// frontiers are useful only for chain validation: a page commits or rolls
    /// back as a unit, so writing them individually adds no recovery value.
    fn flush(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let tx = &self.transaction;
        let session_id = self.session_id;
        if let Some(execution) = self.pending.execution {
            let (state, started_at_ms) = materialized_execution_columns(execution);
            tx.execute(
                "UPDATE materialized_sessions
                 SET execution_state = ?2, running_started_at_ms = ?3
                 WHERE session_id = ?1",
                params![session_id, state, started_at_ms],
            )?;
        }
        if let Some(title) = &self.pending.session_title {
            tx.execute(
                "UPDATE materialized_sessions SET session_title = ?2 WHERE session_id = ?1",
                params![session_id, title],
            )?;
        }
        if let Some(configuration) = &self.pending.configuration {
            tx.execute(
                "UPDATE materialized_sessions SET configuration_json = ?2 WHERE session_id = ?1",
                params![session_id, serde_json::to_string(configuration)?],
            )?;
        }
        for pending in self.pending_transcript.values() {
            match &pending.final_mutation {
                TranscriptMutation::Upsert(item) => {
                    // A remove followed by an upsert deliberately starts a new
                    // item identity. Preserve that boundary even though other
                    // repeated updates are coalesced to one write.
                    if pending.remove_before_upsert {
                        tx.execute(
                            "DELETE FROM materialized_transcript_items
                             WHERE session_id = ?1 AND stable_id = ?2",
                            params![session_id, item.stable_id],
                        )?;
                    }
                    upsert_transcript_item(tx, session_id, item)?;
                }
                TranscriptMutation::Remove { stable_id } => {
                    tx.execute(
                        "DELETE FROM materialized_transcript_items
                         WHERE session_id = ?1 AND stable_id = ?2",
                        params![session_id, stable_id],
                    )?;
                }
            }
        }
        if let Some(queued_prompts) = &self.pending.queued_prompts {
            replace_materialized_queue(tx, session_id, queued_prompts)?;
        }
        if let Some(pending_elicitations) = &self.pending.pending_elicitations {
            tx.execute(
                "UPDATE materialized_sessions
                 SET pending_elicitations_json = ?2 WHERE session_id = ?1",
                params![session_id, serde_json::to_string(pending_elicitations)?],
            )?;
        }
        tx.execute(
            "UPDATE materialized_sessions
             SET last_activity_at_ms = CASE
                     WHEN ?2 IS NULL THEN last_activity_at_ms
                     WHEN last_activity_at_ms IS NULL OR last_activity_at_ms < ?2 THEN ?2
                     ELSE last_activity_at_ms
                 END,
                 applied_event_ordinal = ?3,
                 applied_event_digest = ?4
             WHERE session_id = ?1",
            params![
                session_id,
                self.pending.last_activity_at_ms,
                self.applied_ordinal,
                self.applied_digest,
            ],
        )?;
        Ok(())
    }
}

/// Apply one relay page in a single transaction. `fill` feeds the page's
/// events through [`ProjectionPage::apply`]; the projection changes and the
/// event frontier commit together only when `fill` succeeds, so callers may
/// acknowledge the page's last ordinal to the relay after this returns.
pub fn apply_projection_page<T>(
    session_id: &str,
    fill: impl FnOnce(&mut ProjectionPage<'_>) -> Result<T>,
) -> Result<T> {
    apply_projection_page_to(&database_path(), session_id, fill)
}

fn apply_projection_page_to<T>(
    path: &Path,
    session_id: &str,
    fill: impl FnOnce(&mut ProjectionPage<'_>) -> Result<T>,
) -> Result<T> {
    let mut connection = open(path)?;
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let (applied_ordinal, applied_digest) = transaction
        .query_row(
            "SELECT applied_event_ordinal, applied_event_digest
             FROM materialized_sessions WHERE session_id = ?1",
            [session_id],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
        .with_context(|| format!("unknown session {session_id}"))?;
    validate_relay_event_frontier(
        applied_ordinal,
        &applied_digest,
        "persisted relay event frontier",
    )?;
    let mut page = ProjectionPage {
        session_id,
        transaction,
        applied_ordinal,
        applied_digest,
        dirty: false,
        pending: MaterializedSessionMutation::default(),
        pending_transcript: BTreeMap::new(),
    };
    // Dropping the page on failure rolls the whole transaction back, leaving
    // the projection at the frontier the relay last saw acknowledged.
    let filled = fill(&mut page)?;
    page.flush()?;
    page.transaction.commit()?;
    Ok(filled)
}

/// Apply exactly one relay event, as a page of one.
pub fn apply_projection_event(
    session_id: &str,
    event_ordinal: u64,
    previous_event_digest: &str,
    event_digest: &str,
    mutation: &MaterializedSessionMutation,
) -> Result<ProjectionApplyOutcome> {
    apply_projection_event_to(
        &database_path(),
        session_id,
        event_ordinal,
        previous_event_digest,
        event_digest,
        mutation,
    )
}

fn apply_projection_event_to(
    path: &Path,
    session_id: &str,
    event_ordinal: u64,
    previous_event_digest: &str,
    event_digest: &str,
    mutation: &MaterializedSessionMutation,
) -> Result<ProjectionApplyOutcome> {
    apply_projection_page_to(path, session_id, |page| {
        page.apply(event_ordinal, previous_event_digest, event_digest, mutation)
    })
}

/// Advance the persisted detach/read receipt monotonically. A receipt cannot
/// acknowledge an event the controller projection has not durably applied.
pub fn advance_viewed_through_event_ordinal(session_id: &str, through: u64) -> Result<u64> {
    advance_viewed_through_event_ordinal_to(&database_path(), session_id, through)
}

fn advance_viewed_through_event_ordinal_to(
    path: &Path,
    session_id: &str,
    through: u64,
) -> Result<u64> {
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let applied = tx
        .query_row(
            "SELECT applied_event_ordinal FROM materialized_sessions WHERE session_id = ?1",
            [session_id],
            |row| row.get::<_, u64>(0),
        )
        .optional()?
        .with_context(|| format!("unknown session {session_id}"))?;
    if through > applied {
        bail!(
            "cannot acknowledge event ordinal {through} for session {session_id}; projection is at {applied}"
        );
    }
    tx.execute(
        "UPDATE sessions
         SET viewed_through_event_ordinal = max(viewed_through_event_ordinal, ?2)
         WHERE session_id = ?1",
        params![session_id, through],
    )?;
    let receipt = tx.query_row(
        "SELECT viewed_through_event_ordinal FROM sessions WHERE session_id = ?1",
        [session_id],
        |row| row.get::<_, u64>(0),
    )?;
    tx.commit()?;
    Ok(receipt)
}

/// Overwrite the unsent chat input carried across a detach. Unlike the read
/// receipt this is not monotonic: a draft can shrink, and an empty string
/// clears it.
pub fn set_session_draft_input(session_id: &str, draft: &str) -> Result<()> {
    set_session_draft_input_at(&database_path(), session_id, draft)
}

fn set_session_draft_input_at(path: &Path, session_id: &str, draft: &str) -> Result<()> {
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let updated = tx.execute(
        "UPDATE sessions SET draft_input = ?2 WHERE session_id = ?1",
        params![session_id, draft],
    )?;
    ensure!(updated == 1, "unknown session {session_id}");
    tx.commit()?;
    Ok(())
}

/// Atomically apply the controller's MRU policy for newly used mount sources.
pub fn remember_mount_sources(host: &str, mounts: &[AdditionalMount]) -> Result<()> {
    if mounts.is_empty() {
        return Ok(());
    }
    remember_sources(
        &database_path(),
        host,
        mounts.iter().map(|mount| mount.source.clone()),
    )
}

/// Replace one host's remembered mount sources with exactly this list, so the
/// dashboard can forget a directory the user no longer wants suggested.
pub fn replace_mount_history(host: &str, sources: &[PathBuf]) -> Result<()> {
    replace_mount_history_in(&database_path(), host, sources)
}

fn replace_mount_history_in(path: &Path, host: &str, sources: &[PathBuf]) -> Result<()> {
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    write_mount_history(&tx, host, sources)?;
    tx.commit()?;
    Ok(())
}

fn write_mount_history(tx: &Transaction<'_>, host: &str, sources: &[PathBuf]) -> Result<()> {
    tx.execute("DELETE FROM mount_history WHERE host = ?1", [host])?;
    let mut written = Vec::new();
    for source in sources.iter().take(20) {
        if written.contains(source) {
            continue;
        }
        tx.execute(
            "INSERT INTO mount_history(host, source, ordinal) VALUES (?1, ?2, ?3)",
            params![host, path_to_blob(source), written.len() as i64],
        )?;
        written.push(source.clone());
    }
    Ok(())
}

pub fn remember_project_directory(host: &str, directory: &Path) -> Result<()> {
    remember_sources(
        &database_path(),
        &format!("project:{host}"),
        std::iter::once(directory.to_path_buf()),
    )
}

fn remember_sources(
    path: &Path,
    host: &str,
    new_sources: impl IntoIterator<Item = PathBuf>,
) -> Result<()> {
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let mut sources = {
        let mut statement =
            tx.prepare("SELECT source FROM mount_history WHERE host = ?1 ORDER BY ordinal")?;
        statement
            .query_map([host], |row| Ok(blob_to_path(row.get_ref(0)?.as_blob()?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let additions = new_sources.into_iter().collect::<Vec<_>>();
    for source in additions.iter().rev() {
        sources.retain(|existing| existing != source);
        sources.insert(0, source.clone());
    }
    sources.truncate(20);
    write_mount_history(&tx, host, &sources)?;
    tx.commit()?;
    Ok(())
}

pub fn record_recovery_success(
    session_id: &str,
    native_session_id: &str,
    checkpoint: &CheckpointMetadata,
) -> Result<()> {
    record_recovery_success_to(&database_path(), session_id, native_session_id, checkpoint)
}

fn record_recovery_success_to(
    path: &Path,
    session_id: &str,
    native_session_id: &str,
    checkpoint: &CheckpointMetadata,
) -> Result<()> {
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let changed = tx.execute(
        "UPDATE sessions
         SET native_session_id = ?2, last_checkpoint_error = NULL
         WHERE session_id = ?1",
        params![session_id, native_session_id],
    )?;
    if changed != 1 {
        bail!("unknown session {session_id}");
    }
    tx.execute(
        "INSERT INTO session_checkpoints(
             session_id, archive_path, sha256, created_at, event_frontier
         ) VALUES (?1,?2,?3,?4,?5)
         ON CONFLICT(session_id) DO UPDATE SET
             archive_path = excluded.archive_path,
             sha256 = excluded.sha256,
             created_at = excluded.created_at,
             event_frontier = excluded.event_frontier",
        params![
            session_id,
            path_to_blob(&checkpoint.archive_path),
            checkpoint.sha256,
            checkpoint.created_at,
            checkpoint.event_frontier,
        ],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn record_recovery_failure(session_id: &str, detail: &str) -> Result<()> {
    let connection = open(&database_path())?;
    let changed = connection.execute(
        "UPDATE sessions SET last_checkpoint_error = ?2 WHERE session_id = ?1",
        params![session_id, detail],
    )?;
    if changed != 1 {
        bail!("unknown session {session_id}");
    }
    Ok(())
}

pub fn save_state_to(path: &Path, state: &HelState) -> Result<()> {
    state.validate()?;
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let existing_contexts = existing_contexts(&tx)?;
    let existing_sessions = {
        let mut statement = tx.prepare("SELECT session_id FROM sessions")?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    for session_id in existing_sessions {
        if !state.sessions.contains_key(&session_id) {
            tx.execute("DELETE FROM sessions WHERE session_id = ?1", [session_id])?;
        }
    }
    tx.execute("DELETE FROM mount_history", [])?;
    for session in state.sessions.values() {
        if let Some(existing_bundle) = existing_contexts.get(&session.id)
            && existing_bundle != &session.bundle_id
        {
            bail!(
                "session {} was already associated with bundle {}, not {}",
                session.id,
                existing_bundle,
                session.bundle_id
            );
        }
        insert_session(&tx, session)?;
    }
    for (host, sources) in &state.mount_history {
        for (ordinal, source) in sources.iter().enumerate() {
            tx.execute(
                "INSERT INTO mount_history(host, source, ordinal) VALUES (?1, ?2, ?3)",
                params![host, path_to_blob(source), ordinal as i64],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn existing_contexts(tx: &Transaction<'_>) -> Result<BTreeMap<String, String>> {
    let mut statement = tx.prepare("SELECT session_id, bundle_id FROM session_contexts")?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
}

fn session_exists(tx: &Transaction<'_>, session_id: &str) -> Result<bool> {
    Ok(tx
        .query_row(
            "SELECT 1 FROM sessions WHERE session_id = ?1",
            [session_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn write_materialized_session(
    tx: &Transaction<'_>,
    materialized: &MaterializedSession,
) -> Result<()> {
    let (execution, running_started_at_ms) = materialized_execution_columns(materialized.execution);
    tx.execute(
        "INSERT INTO materialized_sessions(
             session_id, applied_event_ordinal, applied_event_digest, execution_state,
             running_started_at_ms, session_title, configuration_json, last_activity_at_ms,
             pending_elicitations_json
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
         ON CONFLICT(session_id) DO UPDATE SET
             applied_event_ordinal = excluded.applied_event_ordinal,
             applied_event_digest = excluded.applied_event_digest,
             execution_state = excluded.execution_state,
             running_started_at_ms = excluded.running_started_at_ms,
             session_title = excluded.session_title,
             configuration_json = excluded.configuration_json,
             last_activity_at_ms = excluded.last_activity_at_ms,
             pending_elicitations_json = excluded.pending_elicitations_json",
        params![
            materialized.session_id,
            materialized.applied_event_ordinal,
            materialized.applied_event_digest,
            execution,
            running_started_at_ms,
            materialized.session_title,
            serde_json::to_string(&materialized.configuration)?,
            materialized.last_activity_at_ms,
            serde_json::to_string(&materialized.pending_elicitations)?,
        ],
    )?;
    tx.execute(
        "DELETE FROM materialized_transcript_items WHERE session_id = ?1",
        [materialized.session_id.as_str()],
    )?;
    for item in &materialized.transcript {
        upsert_transcript_item(tx, &materialized.session_id, item)?;
    }
    replace_materialized_queue(tx, &materialized.session_id, &materialized.queued_prompts)?;
    Ok(())
}

fn upsert_transcript_item(
    tx: &Transaction<'_>,
    session_id: &str,
    item: &TranscriptItem,
) -> Result<()> {
    let existing = tx
        .query_row(
            "SELECT position, latest_content_event_ordinal, created_at_ms, last_changed_at_ms
             FROM materialized_transcript_items
             WHERE session_id = ?1 AND stable_id = ?2",
            params![session_id, item.stable_id],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, Option<u64>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;
    if let Some((position, latest_content_event_ordinal, created_at_ms, last_changed_at_ms)) =
        existing
    {
        if position != item.position || created_at_ms != item.created_at_ms {
            return Err(ProjectionIntegrityError(format!(
                "transcript item {:?} changed immutable identity fields",
                item.stable_id
            ))
            .into());
        }
        if item.last_changed_at_ms < last_changed_at_ms {
            return Err(ProjectionIntegrityError(format!(
                "transcript item {:?} moved its changed timestamp backwards",
                item.stable_id
            ))
            .into());
        }
        if latest_content_event_ordinal.is_some_and(|existing| {
            item.latest_content_event_ordinal
                .is_none_or(|next| next < existing)
        }) {
            return Err(ProjectionIntegrityError(format!(
                "transcript item {:?} moved its latest content ordinal backwards",
                item.stable_id
            ))
            .into());
        }
        tx.execute(
            "UPDATE materialized_transcript_items
             SET latest_content_event_ordinal = ?3, last_changed_at_ms = ?4, body_json = ?5
             WHERE session_id = ?1 AND stable_id = ?2",
            params![
                session_id,
                item.stable_id,
                item.latest_content_event_ordinal,
                item.last_changed_at_ms,
                serde_json::to_string(&item.body)?,
            ],
        )?;
    } else {
        tx.execute(
            "INSERT INTO materialized_transcript_items(
                 session_id, stable_id, position, latest_content_event_ordinal,
                 created_at_ms, last_changed_at_ms, body_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                session_id,
                item.stable_id,
                item.position,
                item.latest_content_event_ordinal,
                item.created_at_ms,
                item.last_changed_at_ms,
                serde_json::to_string(&item.body)?,
            ],
        )?;
    }
    Ok(())
}

fn replace_materialized_queue(
    tx: &Transaction<'_>,
    session_id: &str,
    queued_prompts: &[MaterializedQueuedPrompt],
) -> Result<()> {
    let mut command_ids = BTreeSet::new();
    for prompt in queued_prompts {
        if prompt.command_id.trim().is_empty() {
            bail!("materialized prompt queue has an empty command id");
        }
        if !command_ids.insert(prompt.command_id.as_str()) {
            bail!(
                "materialized prompt queue contains duplicate command {:?}",
                prompt.command_id
            );
        }
    }
    tx.execute(
        "DELETE FROM materialized_queued_prompts WHERE session_id = ?1",
        [session_id],
    )?;
    for (ordinal, prompt) in queued_prompts.iter().enumerate() {
        tx.execute(
            "INSERT INTO materialized_queued_prompts(
                 session_id, ordinal, command_id, kind_json, content_json, queued_at_ms
             ) VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                session_id,
                ordinal as i64,
                prompt.command_id,
                serde_json::to_string(&prompt.kind)?,
                serde_json::to_string(&prompt.content)?,
                prompt.queued_at_ms,
            ],
        )?;
    }
    Ok(())
}

fn materialized_execution_columns(
    execution: MaterializedExecutionState,
) -> (&'static str, Option<i64>) {
    match execution {
        MaterializedExecutionState::Idle => ("idle", None),
        MaterializedExecutionState::Running { started_at_ms } => ("running", Some(started_at_ms)),
        MaterializedExecutionState::Closing => ("closing", None),
        MaterializedExecutionState::Closed => ("closed", None),
    }
}

fn parse_materialized_execution(
    execution: &str,
    running_started_at_ms: Option<i64>,
) -> Result<MaterializedExecutionState> {
    match (execution, running_started_at_ms) {
        ("idle", None) => Ok(MaterializedExecutionState::Idle),
        ("running", Some(started_at_ms)) => {
            Ok(MaterializedExecutionState::Running { started_at_ms })
        }
        ("closing", None) => Ok(MaterializedExecutionState::Closing),
        ("closed", None) => Ok(MaterializedExecutionState::Closed),
        _ => bail!("invalid materialized execution state {execution:?}"),
    }
}

/// Write every field of a session, including the ones other writers own.
/// Only a flow that authors the whole record — creation, import, resume, or
/// orphan adoption — may use this.
fn insert_session(tx: &Transaction<'_>, session: &SessionRecord) -> Result<()> {
    tx.execute(
        "INSERT INTO session_contexts(session_id, bundle_id, created_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(session_id) DO NOTHING",
        params![session.id, session.bundle_id, session.created_at],
    )?;
    tx.execute(
        "INSERT INTO sessions(
             session_id, title, harness_kind, last_profile, target_template_id, state,
             native_session_id, acp_session_title, session_title_override, updated_at,
             viewed_through_event_ordinal, last_error, resource_allocation,
             last_checkpoint_error, project_directory, managed_worktree,
             container_cpus, container_memory, archived
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)
         ON CONFLICT(session_id) DO UPDATE SET
             title = excluded.title,
             harness_kind = excluded.harness_kind,
             last_profile = excluded.last_profile,
             target_template_id = excluded.target_template_id,
             state = excluded.state,
             native_session_id = excluded.native_session_id,
             acp_session_title = excluded.acp_session_title,
             session_title_override = excluded.session_title_override,
             updated_at = excluded.updated_at,
             viewed_through_event_ordinal = max(
                 sessions.viewed_through_event_ordinal,
                 excluded.viewed_through_event_ordinal
             ),
             last_error = excluded.last_error,
             resource_allocation = excluded.resource_allocation,
             last_checkpoint_error = excluded.last_checkpoint_error,
             project_directory = excluded.project_directory,
             managed_worktree = excluded.managed_worktree,
             container_cpus = excluded.container_cpus,
             container_memory = excluded.container_memory,
             archived = excluded.archived",
        params![
            session.id,
            session.title,
            session.harness_kind.id(),
            session.last_profile,
            session.target_template_id,
            session_state_name(session.state),
            session.native_session_id,
            session.acp_session_title,
            session.session_title_override,
            session.updated_at,
            session.viewed_through_event_ordinal,
            session.last_error,
            session
                .resource_allocation
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
            session.last_checkpoint_error,
            session
                .project_directory
                .as_ref()
                .map(|path| path_to_blob(path)),
            session
                .managed_worktree
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
            session.container_cpus,
            session.container_memory,
            session.archived,
        ],
    )?;
    tx.execute(
        "INSERT INTO materialized_sessions(session_id) VALUES (?1)
         ON CONFLICT(session_id) DO NOTHING",
        [session.id.as_str()],
    )?;
    replace_targets(tx, session)?;
    tx.execute(
        "DELETE FROM session_mounts WHERE session_id = ?1",
        [session.id.as_str()],
    )?;
    for (ordinal, mount) in session.additional_mounts.iter().enumerate() {
        tx.execute(
            "INSERT INTO session_mounts(session_id, ordinal, source, destination, read_only)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session.id,
                ordinal as i64,
                path_to_blob(&mount.source),
                path_to_blob(&mount.destination),
                mount.read_only
            ],
        )?;
    }
    replace_checkpoint(tx, session)?;
    Ok(())
}

/// Update the columns a lifecycle transition owns, plus the target locator
/// that provisioning and teardown maintain with them. The row must exist:
/// a transition never resurrects a session another writer deleted.
fn update_lifecycle_fields(tx: &Transaction<'_>, session: &SessionRecord) -> Result<()> {
    let changed = tx.execute(
        // The detach ordinal only ever moves forward, so a transition that
        // started before a detach receipt cannot rewind it.
        "UPDATE sessions
         SET title = ?2,
             harness_kind = ?3,
             last_profile = ?4,
             target_template_id = ?5,
             state = ?6,
             updated_at = ?7,
             viewed_through_event_ordinal = max(viewed_through_event_ordinal, ?8),
             last_error = ?9,
             resource_allocation = ?10,
             last_checkpoint_error = ?11,
             project_directory = ?12,
             managed_worktree = ?13
         WHERE session_id = ?1",
        params![
            session.id,
            session.title,
            session.harness_kind.id(),
            session.last_profile,
            session.target_template_id,
            session_state_name(session.state),
            session.updated_at,
            session.viewed_through_event_ordinal,
            session.last_error,
            session
                .resource_allocation
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
            session.last_checkpoint_error,
            session
                .project_directory
                .as_ref()
                .map(|path| path_to_blob(path)),
            session
                .managed_worktree
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
        ],
    )?;
    if changed != 1 {
        bail!("unknown session {}", session.id);
    }
    replace_targets(tx, session)
}

fn replace_targets(tx: &Transaction<'_>, session: &SessionRecord) -> Result<()> {
    tx.execute(
        "DELETE FROM session_targets WHERE session_id = ?1",
        [session.id.as_str()],
    )?;
    if let Some(target) = &session.target {
        insert_target(tx, &session.id, target)?;
    }
    Ok(())
}

fn replace_checkpoint(tx: &Transaction<'_>, session: &SessionRecord) -> Result<()> {
    tx.execute(
        "DELETE FROM session_checkpoints WHERE session_id = ?1",
        [session.id.as_str()],
    )?;
    if let Some(checkpoint) = &session.checkpoint {
        tx.execute(
            "INSERT INTO session_checkpoints(session_id, archive_path, sha256, created_at, event_frontier)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session.id,
                path_to_blob(&checkpoint.archive_path),
                checkpoint.sha256,
                checkpoint.created_at,
                checkpoint.event_frontier,
            ],
        )?;
    }
    Ok(())
}

fn insert_target(tx: &Transaction<'_>, session_id: &str, target: &TargetLocator) -> Result<()> {
    let (kind, host, resource, address, workspace, worker_id) = match target {
        TargetLocator::LocalBare { worker_root } => (
            "local-bare",
            None,
            None,
            None,
            Some(path_to_blob(worker_root)),
            None,
        ),
        TargetLocator::LocalPodman { container_id } => (
            "local-podman",
            None,
            Some(container_id.as_str()),
            None,
            None,
            None,
        ),
        TargetLocator::AppleContainer { container_id } => (
            "apple-container",
            None,
            Some(container_id.as_str()),
            None,
            None,
            None,
        ),
        TargetLocator::AwsEc2 {
            instance_id,
            address,
        } => (
            "aws-ec2",
            None,
            Some(instance_id.as_str()),
            address.as_deref(),
            None,
            None,
        ),
        TargetLocator::SshBare {
            host,
            workspace,
            worker_id,
        } => (
            "ssh-bare",
            Some(host.as_str()),
            None,
            None,
            Some(path_to_blob(workspace)),
            worker_id.as_deref(),
        ),
        TargetLocator::SshPodman { host, container_id } => (
            "ssh-podman",
            Some(host.as_str()),
            Some(container_id.as_str()),
            None,
            None,
            None,
        ),
    };
    tx.execute(
        "INSERT INTO session_targets(session_id, kind, host, resource_id, address, workspace, worker_id)
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![session_id, kind, host, resource, address, workspace, worker_id],
    )?;
    Ok(())
}

fn load_targets(connection: &Connection, state: &mut HelState) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT session_id, kind, host, resource_id, address, workspace, worker_id
         FROM session_targets",
    )?;
    let rows = statement.query_map([], |row| {
        let session_id: String = row.get(0)?;
        let kind: String = row.get(1)?;
        let host: Option<String> = row.get(2)?;
        let resource: Option<String> = row.get(3)?;
        let address: Option<String> = row.get(4)?;
        let workspace = row.get_ref(5)?.blob_or_null()?.map(blob_to_path);
        let worker_id: Option<String> = row.get(6)?;
        let target = match kind.as_str() {
            "local-bare" => TargetLocator::LocalBare {
                worker_root: workspace.unwrap(),
            },
            "local-podman" => TargetLocator::LocalPodman {
                container_id: resource.unwrap(),
            },
            "apple-container" => TargetLocator::AppleContainer {
                container_id: resource.unwrap(),
            },
            "aws-ec2" => TargetLocator::AwsEc2 {
                instance_id: resource.unwrap(),
                address,
            },
            "ssh-bare" => TargetLocator::SshBare {
                host: host.unwrap(),
                workspace: workspace.unwrap(),
                worker_id,
            },
            "ssh-podman" => TargetLocator::SshPodman {
                host: host.unwrap(),
                container_id: resource.unwrap(),
            },
            _ => unreachable!("target kind constrained by schema"),
        };
        Ok((session_id, target))
    })?;
    for row in rows {
        let (session_id, target) = row?;
        state.sessions.get_mut(&session_id).unwrap().target = Some(target);
    }
    Ok(())
}

fn load_mounts(connection: &Connection, state: &mut HelState) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT session_id, source, destination, read_only
         FROM session_mounts ORDER BY session_id, ordinal",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            AdditionalMount {
                source: blob_to_path(row.get_ref(1)?.as_blob()?),
                destination: blob_to_path(row.get_ref(2)?.as_blob()?),
                read_only: row.get(3)?,
            },
        ))
    })?;
    for row in rows {
        let (session_id, mount) = row?;
        state
            .sessions
            .get_mut(&session_id)
            .unwrap()
            .additional_mounts
            .push(mount);
    }
    Ok(())
}

fn load_checkpoints(connection: &Connection, state: &mut HelState) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT session_id, archive_path, sha256, created_at, event_frontier FROM session_checkpoints",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            CheckpointMetadata {
                archive_path: blob_to_path(row.get_ref(1)?.as_blob()?),
                sha256: row.get(2)?,
                created_at: row.get(3)?,
                event_frontier: row.get(4)?,
            },
        ))
    })?;
    for row in rows {
        let (session_id, checkpoint) = row?;
        state.sessions.get_mut(&session_id).unwrap().checkpoint = Some(checkpoint);
    }
    Ok(())
}

/// Re-associate a session with another project bundle.
///
/// A session's bundle is otherwise fixed, because prompt history is grouped by
/// it. Resume calls this when it converts a session between its raw and bundle
/// representations: the project is the same, so its history follows it, and
/// only the name Hel files it under changes.
pub fn rebind_session_bundle(session_id: &str, bundle_id: &str) -> Result<()> {
    rebind_session_bundle_to(&database_path(), session_id, bundle_id)
}

fn rebind_session_bundle_to(path: &Path, session_id: &str, bundle_id: &str) -> Result<()> {
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let changed = tx.execute(
        "UPDATE session_contexts SET bundle_id = ?2 WHERE session_id = ?1",
        params![session_id, bundle_id],
    )?;
    if changed == 0 {
        tx.execute(
            "INSERT INTO session_contexts(session_id, bundle_id, created_at) VALUES (?1, ?2, ?3)",
            params![session_id, bundle_id, Utc::now().to_rfc3339()],
        )?;
    }
    tx.commit()?;
    Ok(())
}

pub fn record_prompt(
    session_id: &str,
    bundle_id: &str,
    event_ordinal: u64,
    submitted_at: Option<&str>,
    text: &str,
) -> Result<()> {
    record_prompt_to(
        &database_path(),
        session_id,
        bundle_id,
        event_ordinal,
        submitted_at,
        text,
    )
}

fn record_prompt_to(
    path: &Path,
    session_id: &str,
    bundle_id: &str,
    event_ordinal: u64,
    submitted_at: Option<&str>,
    text: &str,
) -> Result<()> {
    if text.trim().is_empty() {
        return Ok(());
    }
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    tx.execute(
        "INSERT INTO session_contexts(session_id, bundle_id, created_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(session_id) DO NOTHING",
        params![session_id, bundle_id, submitted_at.unwrap_or("unknown")],
    )?;
    let actual_bundle: String = tx.query_row(
        "SELECT bundle_id FROM session_contexts WHERE session_id = ?1",
        [session_id],
        |row| row.get(0),
    )?;
    if actual_bundle != bundle_id {
        bail!("session {session_id} belongs to bundle {actual_bundle}, not {bundle_id}");
    }
    tx.execute(
        "INSERT INTO prompt_history(session_id, event_ordinal, submitted_at, text)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(session_id, event_ordinal) DO NOTHING",
        params![
            session_id,
            event_ordinal,
            submitted_at
                .map(str::to_owned)
                .unwrap_or_else(|| Utc::now().to_rfc3339()),
            text,
        ],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn search_prompts(
    session_id: &str,
    bundle_id: &str,
    scope: HistoryScope,
    query: &str,
) -> Result<Vec<PromptHistoryEntry>> {
    search_prompts_from(&database_path(), session_id, bundle_id, scope, query)
}

fn search_prompts_from(
    path: &Path,
    session_id: &str,
    bundle_id: &str,
    scope: HistoryScope,
    query: &str,
) -> Result<Vec<PromptHistoryEntry>> {
    const PAGE_SIZE: usize = 256;
    let connection = open(path)?;
    let query = query.to_lowercase();
    let mut seen = std::collections::HashSet::new();
    let mut matches = Vec::new();
    let mut before = i64::MAX;
    loop {
        let page = match scope {
            HistoryScope::Project => query_history_page(
                &connection,
                "SELECT h.history_id, h.session_id, h.text
                 FROM prompt_history h JOIN session_contexts c USING(session_id)
                 WHERE c.bundle_id = ?1 AND h.history_id < ?2
                 ORDER BY h.history_id DESC LIMIT ?3",
                params![bundle_id, before, PAGE_SIZE as i64],
            )?,
            HistoryScope::Session => query_history_page(
                &connection,
                "SELECT history_id, session_id, text FROM prompt_history
                 WHERE session_id = ?1 AND history_id < ?2
                 ORDER BY history_id DESC LIMIT ?3",
                params![session_id, before, PAGE_SIZE as i64],
            )?,
            HistoryScope::All => query_history_page(
                &connection,
                "SELECT history_id, session_id, text FROM prompt_history
                 WHERE history_id < ?1 ORDER BY history_id DESC LIMIT ?2",
                params![before, PAGE_SIZE as i64],
            )?,
        };
        let page_len = page.len();
        for entry in page {
            before = entry.id;
            if entry.text.to_lowercase().contains(&query) && seen.insert(entry.text.clone()) {
                matches.push(entry);
            }
        }
        if page_len < PAGE_SIZE {
            break;
        }
    }
    Ok(matches)
}

fn query_history_page(
    connection: &Connection,
    sql: &str,
    parameters: impl rusqlite::Params,
) -> Result<Vec<PromptHistoryEntry>> {
    let mut statement = connection.prepare_cached(sql)?;
    let rows = statement.query_map(parameters, |row| {
        Ok(PromptHistoryEntry {
            id: row.get(0)?,
            session_id: row.get(1)?,
            text: row.get(2)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

pub fn migrate_legacy_state() -> Result<()> {
    let legacy = crate::hel_state::state_path();
    let database = database_path();
    migrate_legacy_state_from(&legacy, &database)
}

fn migrate_legacy_state_from(legacy: &Path, database: &Path) -> Result<()> {
    if !legacy.exists() {
        return Ok(());
    }
    // The database may exist after an interrupted migration. The legacy file
    // remains the authority until the import commits and this file is renamed.
    let mut state = HelState::load_json_from(legacy)?;
    // Legacy worker sequence numbers are not relay event ordinals. Carrying
    // them across the new compatibility floor could mark unseen relay events
    // as read.
    for session in state.sessions.values_mut() {
        session.viewed_through_event_ordinal = 0;
    }
    save_state_to(database, &state)?;
    let migrated = legacy.with_file_name("state.json.migrated-v1");
    fs::rename(legacy, &migrated)
        .with_context(|| format!("retain migrated Hel state as {}", migrated.display()))?;
    Ok(())
}

fn session_state_name(value: SessionState) -> &'static str {
    match value {
        SessionState::Provisioning => "provisioning",
        SessionState::Running => "running",
        SessionState::Disconnected => "disconnected",
        SessionState::Checkpointing => "checkpointing",
        SessionState::Closing => "closing",
        SessionState::Destroying => "destroying",
        SessionState::Stopped => "stopped",
        SessionState::Lost => "lost",
        SessionState::Error => "error",
        SessionState::DestroyedWithDataLoss => "destroyed-with-data-loss",
    }
}
fn parse_session_state(value: &str) -> SessionState {
    match value {
        "provisioning" => SessionState::Provisioning,
        "running" => SessionState::Running,
        "disconnected" => SessionState::Disconnected,
        "checkpointing" => SessionState::Checkpointing,
        "closing" => SessionState::Closing,
        "destroying" => SessionState::Destroying,
        // Rows written before the verb was renamed still say "archived".
        "stopped" | "archived" => SessionState::Stopped,
        "lost" => SessionState::Lost,
        "error" => SessionState::Error,
        "destroyed-with-data-loss" => SessionState::DestroyedWithDataLoss,
        _ => unreachable!(),
    }
}

#[cfg(unix)]
fn path_to_blob(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}
#[cfg(unix)]
fn blob_to_path(bytes: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStrExt;
    PathBuf::from(std::ffi::OsStr::from_bytes(bytes))
}
#[cfg(windows)]
fn path_to_blob(path: &Path) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;
    path.as_os_str()
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect()
}
#[cfg(windows)]
fn blob_to_path(bytes: &[u8]) -> PathBuf {
    use std::os::windows::ffi::OsStringExt;
    let wide = bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u16::from_le_bytes([b[0], b[1]]))
        .collect::<Vec<_>>();
    PathBuf::from(std::ffi::OsString::from_wide(&wide))
}

trait ValueRefExt<'a> {
    fn blob_or_null(self) -> rusqlite::Result<Option<&'a [u8]>>;
}
impl<'a> ValueRefExt<'a> for rusqlite::types::ValueRef<'a> {
    fn blob_or_null(self) -> rusqlite::Result<Option<&'a [u8]>> {
        match self {
            rusqlite::types::ValueRef::Null => Ok(None),
            value => Ok(Some(value.as_blob()?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_config::HarnessKind;
    use crate::hel_state::{ManagedWorktreeTarget, QueuedCommandKind, TranscriptBody};
    use crate::hel_worker::RELAY_EVENT_GENESIS_DIGEST;
    use rusqlite::OptionalExtension;

    fn event_digest(value: u64) -> String {
        format!("{value:064x}")
    }

    fn session(id: &str, bundle: &str) -> SessionRecord {
        SessionRecord {
            archived: false,
            container_cpus: None,
            container_memory: None,
            id: id.into(),
            title: "test session".into(),
            harness_kind: HarnessKind::Codex,
            last_profile: "codex".into(),
            bundle_id: bundle.into(),
            project_directory: None,
            managed_worktree: None,
            target_template_id: "local".into(),
            resource_allocation: Some(SessionResourceAllocation::Container {
                cpus: 8,
                memory_bytes: 32 * 1024 * 1024 * 1024,
            }),
            additional_mounts: vec![AdditionalMount {
                source: PathBuf::from("/host/cache"),
                destination: PathBuf::from("/mnt/cache"),
                read_only: false,
            }],
            state: SessionState::Stopped,
            target: Some(TargetLocator::LocalPodman {
                container_id: "container-1".into(),
            }),
            native_session_id: Some("native-1".into()),
            acp_session_title: Some("Agent title".into()),
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T01:00:00Z".into(),
            viewed_through_event_ordinal: 7,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: Some("temporary recovery failure".into()),
            checkpoint: Some(CheckpointMetadata {
                archive_path: PathBuf::from("sessions/test.hel.zip"),
                sha256: "a".repeat(64),
                created_at: "2026-08-12T01:00:00Z".into(),
                event_frontier: 6,
            }),
        }
    }

    fn materialized_session(session_id: &str) -> MaterializedSession {
        MaterializedSession {
            session_id: session_id.into(),
            applied_event_ordinal: 7,
            applied_event_digest: event_digest(7),
            last_activity_at_ms: Some(1_500),
            execution: MaterializedExecutionState::Running {
                started_at_ms: 1_000,
            },
            session_title: Some("Relay refactor".into()),
            configuration: BTreeMap::from([
                ("model".into(), serde_json::json!("gpt-5.6-sol")),
                ("effort".into(), serde_json::json!("high")),
            ]),
            transcript: vec![
                Arc::new(TranscriptItem {
                    stable_id: "user:1".into(),
                    position: 1,
                    latest_content_event_ordinal: None,
                    created_at_ms: 1_000,
                    last_changed_at_ms: 1_000,
                    body: TranscriptBody::User {
                        content: vec![serde_json::json!({
                            "type": "text",
                            "text": "build it"
                        })],
                    },
                }),
                Arc::new(TranscriptItem {
                    stable_id: "agent:2".into(),
                    position: 2,
                    latest_content_event_ordinal: Some(2),
                    created_at_ms: 1_100,
                    last_changed_at_ms: 1_300,
                    body: TranscriptBody::Agent {
                        chunks: vec![serde_json::json!({
                            "content": {"type": "text", "text": "Working on it"},
                            "messageId": "answer-1",
                            "_meta": {"provider": "test"}
                        })],
                        streaming: false,
                    },
                }),
                Arc::new(TranscriptItem {
                    stable_id: "tool:call-1".into(),
                    position: 3,
                    latest_content_event_ordinal: None,
                    created_at_ms: 1_200,
                    last_changed_at_ms: 1_400,
                    body: TranscriptBody::Tool {
                        call: serde_json::json!({
                            "toolCallId": "call-1",
                            "title": "Edit files",
                            "kind": "edit",
                            "status": "completed",
                            "content": [{
                                "type": "content",
                                "content": {"type": "text", "text": "done"}
                            }],
                            "locations": [{"path": "src/main.rs", "line": 4}],
                            "rawInput": {"path": "src/main.rs"},
                            "rawOutput": {"changed": true},
                            "_meta": {"provider": "test"}
                        }),
                        terminal_outputs: Vec::new(),
                        terminal_refs: Vec::new(),
                    },
                }),
                Arc::new(TranscriptItem {
                    stable_id: "plan:1".into(),
                    position: 4,
                    latest_content_event_ordinal: None,
                    created_at_ms: 1_250,
                    last_changed_at_ms: 1_350,
                    body: TranscriptBody::Plan {
                        plan: serde_json::json!({
                            "entries": [{
                                "content": "Implement relay",
                                "priority": "high",
                                "status": "in_progress",
                                "_meta": {"provider": "test"}
                            }],
                            "_meta": {"planProvider": "test"}
                        }),
                    },
                }),
            ],
            queued_prompts: vec![MaterializedQueuedPrompt {
                command_id: "prompt-2".into(),
                kind: QueuedCommandKind::Prompt,
                content: vec![serde_json::json!({"type": "text", "text": "then test"})],
                queued_at_ms: 1_500,
            }],
            pending_elicitations: vec![crate::hel_elicitation::ElicitationRequest {
                id: "elicitation-1".into(),
                message: "Choose one".into(),
                title: Some("Question".into()),
                description: None,
                fields: Vec::new(),
            }],
        }
    }

    #[test]
    fn normalized_state_round_trip_preserves_children_and_order() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut state = HelState::default();
        let mut record = session("session-1", "project-1");
        record.project_directory = Some(PathBuf::from("/srv/project-1/.hel/worktrees/session-1"));
        record.managed_worktree = Some(ManagedWorktree {
            source_project_directory: PathBuf::from("/srv/project-1"),
            source_repository: PathBuf::from("/srv/project-1"),
            worktree_root: PathBuf::from("/srv/project-1/.hel/worktrees/session-1"),
            branch: "hel/session-1".into(),
            target: ManagedWorktreeTarget::Ssh {
                destination: "builder".into(),
                ssh_args: vec!["-o".into(), "BatchMode=yes".into()],
            },
        });
        record.resource_allocation = None;
        record.target = Some(TargetLocator::LocalBare {
            worker_root: PathBuf::from("/var/lib/hel/workers/session-1"),
        });
        state.sessions.insert(record.id.clone(), record);
        state.mount_history.insert(
            "local".into(),
            vec![PathBuf::from("/recent"), PathBuf::from("/older")],
        );

        save_state_to(&database, &state).unwrap();

        assert_eq!(load_state_from(&database).unwrap(), state);
        let connection = open(&database).unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert_eq!(
            connection
                .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
                .optional()
                .unwrap(),
            None
        );
    }

    #[test]
    fn loading_state_does_not_restore_a_hidden_context_session_name() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut state = HelState::default();
        let mut record = session("session-1", "project-1");
        record.acp_session_title = Some("<hel-project-memory>private and truncated".into());
        state.sessions.insert(record.id.clone(), record);
        save_state_to(&database, &state).unwrap();

        assert_eq!(
            load_state_from(&database).unwrap().sessions["session-1"].acp_session_title,
            None
        );
    }

    #[test]
    fn container_settings_write_overrides_mounts_and_remembered_sources() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let record = session("session-1", "project-1");
        save_session_to(&database, &record).unwrap();

        set_session_container_settings_to(
            &database,
            "session-1",
            Some("6"),
            Some("12g"),
            &[AdditionalMount {
                source: PathBuf::from("/host/models"),
                destination: PathBuf::from("/mnt/models"),
                read_only: true,
            }],
            "2026-08-13T00:00:00Z",
        )
        .unwrap();
        replace_mount_history_in(&database, "local", &[PathBuf::from("/host/models")]).unwrap();

        let loaded = load_state_from(&database).unwrap();
        let session = &loaded.sessions["session-1"];
        assert_eq!(session.container_cpus.as_deref(), Some("6"));
        assert_eq!(session.container_memory.as_deref(), Some("12g"));
        assert_eq!(
            session.additional_mounts,
            vec![AdditionalMount {
                source: PathBuf::from("/host/models"),
                destination: PathBuf::from("/mnt/models"),
                read_only: true,
            }]
        );
        assert_eq!(session.updated_at, "2026-08-13T00:00:00Z");
        assert_eq!(
            loaded.mount_history["local"],
            vec![PathBuf::from("/host/models")]
        );
    }

    #[test]
    fn mount_read_only_round_trips_through_both_writers_and_defaults_before_the_column() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut record = session("session-1", "project-1");
        record.additional_mounts = vec![
            AdditionalMount {
                source: PathBuf::from("/host/cache"),
                destination: PathBuf::from("/mnt/cache"),
                read_only: false,
            },
            AdditionalMount {
                source: PathBuf::from("/net/share"),
                destination: PathBuf::from("/mnt/share"),
                read_only: true,
            },
        ];

        save_session_to(&database, &record).unwrap();
        assert_eq!(
            load_state_from(&database).unwrap().sessions["session-1"].additional_mounts,
            record.additional_mounts
        );

        // A database written before the column existed keeps its rows, and they
        // load as overlay mounts.
        let connection = open(&database).unwrap();
        connection
            .execute_batch(
                "ALTER TABLE session_mounts DROP COLUMN read_only;
                 DELETE FROM session_mounts;
                 INSERT INTO session_mounts(session_id, ordinal, source, destination)
                     VALUES ('session-1', 0, CAST('/net/share' AS BLOB), CAST('/mnt/share' AS BLOB));",
            )
            .unwrap();
        drop(connection);
        // Editing the schema of an already-open store is something only this
        // test does, so it has to retract the process's verification too.
        forget_verified_schema(&database);

        assert_eq!(
            load_state_from(&database).unwrap().sessions["session-1"].additional_mounts,
            vec![AdditionalMount {
                source: PathBuf::from("/net/share"),
                destination: PathBuf::from("/mnt/share"),
                read_only: false,
            }]
        );
    }

    #[test]
    fn lifecycle_save_preserves_container_settings_and_mounts() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut stale = session("session-1", "project-1");
        stale.additional_mounts.clear();
        save_session_to(&database, &stale).unwrap();

        // A read-only mount proves the flag survives a stale lifecycle save too.
        let attached = AdditionalMount {
            source: PathBuf::from("/host/models"),
            destination: PathBuf::from("/mnt/models"),
            read_only: true,
        };
        set_session_container_settings_to(
            &database,
            "session-1",
            Some("6"),
            Some("12g"),
            std::slice::from_ref(&attached),
            "2026-08-15T00:00:00Z",
        )
        .unwrap();

        // The lifecycle writer still holds the record as it was before the
        // container settings were edited.
        stale.state = SessionState::Destroying;
        stale.updated_at = "2026-08-15T00:01:00Z".into();
        save_lifecycle_session_to(&database, &stale).unwrap();

        let loaded = load_state_from(&database).unwrap();
        let session = &loaded.sessions["session-1"];
        assert_eq!(session.state, SessionState::Destroying);
        assert_eq!(session.additional_mounts, vec![attached]);
        assert_eq!(session.container_cpus.as_deref(), Some("6"));
        assert_eq!(session.container_memory.as_deref(), Some("12g"));
    }

    #[test]
    fn missing_target_marks_only_a_live_session_lost() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut live = session("session-1", "project-1");
        live.state = SessionState::Running;
        save_session_to(&database, &live).unwrap();

        assert!(
            mark_session_target_lost_to(
                &database,
                "session-1",
                "managed container is missing",
                "2026-08-25T16:00:00Z",
            )
            .unwrap()
        );
        let loaded = load_state_from(&database).unwrap();
        let lost = &loaded.sessions["session-1"];
        assert_eq!(lost.state, SessionState::Lost);
        assert_eq!(
            lost.last_error.as_deref(),
            Some("managed container is missing")
        );

        assert!(
            !mark_session_target_lost_to(
                &database,
                "session-1",
                "late duplicate",
                "2026-08-25T16:01:00Z",
            )
            .unwrap()
        );
        let loaded = load_state_from(&database).unwrap();
        assert_eq!(
            loaded.sessions["session-1"].last_error.as_deref(),
            Some("managed container is missing")
        );
    }

    #[test]
    fn checkpointed_save_preserves_container_settings_and_mounts() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut stale = session("session-1", "project-1");
        stale.additional_mounts.clear();
        save_session_to(&database, &stale).unwrap();

        let attached = AdditionalMount {
            source: PathBuf::from("/host/models"),
            destination: PathBuf::from("/mnt/models"),
            read_only: true,
        };
        set_session_container_settings_to(
            &database,
            "session-1",
            Some("6"),
            Some("12g"),
            std::slice::from_ref(&attached),
            "2026-08-15T00:00:00Z",
        )
        .unwrap();

        let verified = CheckpointMetadata {
            archive_path: PathBuf::from("sessions/verified.hel.zip"),
            sha256: "c".repeat(64),
            created_at: "2026-08-15T00:02:00Z".into(),
            event_frontier: 21,
        };
        stale.state = SessionState::Running;
        stale.updated_at = "2026-08-15T00:02:00Z".into();
        stale.native_session_id = Some("native-checkpointed".into());
        stale.checkpoint = Some(verified.clone());
        save_checkpointed_session_to(&database, &stale).unwrap();

        let loaded = load_state_from(&database).unwrap();
        let session = &loaded.sessions["session-1"];
        assert_eq!(session.checkpoint.as_ref(), Some(&verified));
        assert_eq!(
            session.native_session_id.as_deref(),
            Some("native-checkpointed")
        );
        assert_eq!(session.additional_mounts, vec![attached]);
        assert_eq!(session.container_cpus.as_deref(), Some("6"));
        assert_eq!(session.container_memory.as_deref(), Some("12g"));
    }

    #[test]
    fn lifecycle_save_fails_for_unknown_session() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let missing = session("session-1", "project-1");

        for error in [
            save_lifecycle_session_to(&database, &missing).unwrap_err(),
            save_checkpointed_session_to(&database, &missing).unwrap_err(),
        ] {
            assert!(
                format!("{error:#}").contains("unknown session session-1"),
                "{error:#}"
            );
        }

        assert!(load_state_from(&database).unwrap().sessions.is_empty());
    }

    #[test]
    fn destroying_session_round_trip_is_durable() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut record = session("session-1", "project-1");
        record.state = SessionState::Destroying;

        save_session_to(&database, &record).unwrap();

        assert_eq!(
            load_state_from(&database).unwrap().sessions["session-1"],
            record
        );
    }

    #[test]
    fn interrupted_checkpoint_recovery_is_field_scoped_and_one_shot() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut checkpointing = session("session-1", "project-1");
        checkpointing.state = SessionState::Checkpointing;
        checkpointing.last_error = Some("preserve this diagnostic".into());
        let mut closing = session("session-2", "project-2");
        closing.state = SessionState::Closing;
        save_session_to(&database, &checkpointing).unwrap();
        save_session_to(&database, &closing).unwrap();

        assert_eq!(
            recover_interrupted_checkpointing_sessions_to(&database, "2026-08-14T12:00:00Z")
                .unwrap(),
            1
        );

        let recovered = load_state_from(&database).unwrap();
        let session = &recovered.sessions["session-1"];
        assert_eq!(session.state, SessionState::Running);
        assert_eq!(session.updated_at, "2026-08-14T12:00:00Z");
        assert_eq!(
            session.last_error.as_deref(),
            Some("preserve this diagnostic")
        );
        assert_eq!(session.target, checkpointing.target);
        assert_eq!(session.checkpoint, checkpointing.checkpoint);
        assert!(
            session
                .last_checkpoint_error
                .as_deref()
                .is_some_and(|error| error.contains("controller restart"))
        );
        assert_eq!(recovered.sessions["session-2"], closing);
        assert_eq!(
            recover_interrupted_checkpointing_sessions_to(&database, "2026-08-14T12:01:00Z")
                .unwrap(),
            0
        );
    }

    #[test]
    fn display_updates_cannot_restore_a_stale_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut stale = session("session-1", "project-1");
        stale.state = SessionState::Error;
        stale.last_error = Some("worker bootstrap failed: upload failed".into());
        save_session_to(&database, &stale).unwrap();
        let recovered = CheckpointMetadata {
            archive_path: PathBuf::from("sessions/recovered.hel.zip"),
            sha256: "b".repeat(64),
            created_at: "2026-08-14T12:00:00Z".into(),
            event_frontier: 42,
        };
        record_recovery_success_to(&database, "session-1", "native-recovered", &recovered).unwrap();

        set_session_title_override_to(
            &database,
            "session-1",
            "Renamed safely",
            "2026-08-14T12:01:00Z",
        )
        .unwrap();
        set_session_acp_title_to(&database, "session-1", Some("Harness title")).unwrap();

        let loaded = load_state_from(&database).unwrap();
        let session = &loaded.sessions["session-1"];
        assert_eq!(session.checkpoint.as_ref(), Some(&recovered));
        assert_eq!(
            session.native_session_id.as_deref(),
            Some("native-recovered")
        );
        assert_eq!(
            session.session_title_override.as_deref(),
            Some("Renamed safely")
        );
        assert_eq!(session.acp_session_title.as_deref(), Some("Harness title"));
        assert_eq!(session.state, SessionState::Error);
        assert_eq!(
            session.last_error.as_deref(),
            Some("worker bootstrap failed: upload failed")
        );

        set_session_acp_title_to(&database, "session-1", None).unwrap();
        assert!(
            load_state_from(&database).unwrap().sessions["session-1"]
                .acp_session_title
                .is_none()
        );
    }

    #[test]
    fn lifecycle_write_preserves_independently_owned_session_fields() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut stale = session("session-1", "project-1");
        stale.native_session_id = Some("native-old".into());
        stale.acp_session_title = Some("Old harness title".into());
        stale.session_title_override = Some("Old user title".into());
        stale.checkpoint = Some(CheckpointMetadata {
            archive_path: PathBuf::from("sessions/old.hel.zip"),
            sha256: "a".repeat(64),
            created_at: "2026-08-14T11:00:00Z".into(),
            event_frontier: 10,
        });
        save_session_to(&database, &stale).unwrap();
        let recovered = CheckpointMetadata {
            archive_path: PathBuf::from("sessions/recovered.hel.zip"),
            sha256: "b".repeat(64),
            created_at: "2026-08-14T12:00:00Z".into(),
            event_frontier: 42,
        };
        record_recovery_success_to(&database, "session-1", "native-recovered", &recovered).unwrap();
        set_session_title_override_to(
            &database,
            "session-1",
            "Current user title",
            "2026-08-14T12:01:00Z",
        )
        .unwrap();
        set_session_acp_title_to(&database, "session-1", Some("Current harness title")).unwrap();

        stale.state = SessionState::Destroying;
        save_lifecycle_session_to(&database, &stale).unwrap();

        let loaded = load_state_from(&database).unwrap();
        let session = &loaded.sessions["session-1"];
        assert_eq!(session.state, SessionState::Destroying);
        assert_eq!(
            session.native_session_id.as_deref(),
            Some("native-recovered")
        );
        assert_eq!(session.checkpoint.as_ref(), Some(&recovered));
        assert_eq!(
            session.session_title_override.as_deref(),
            Some("Current user title")
        );
        assert_eq!(
            session.acp_session_title.as_deref(),
            Some("Current harness title")
        );
    }

    #[test]
    fn version_four_database_migrates_existing_targets_and_accepts_local_bare() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY CHECK(version > 0),
                     applied_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE session_contexts (
                     session_id TEXT PRIMARY KEY,
                     bundle_id TEXT NOT NULL,
                     created_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE sessions (
                     session_id TEXT PRIMARY KEY REFERENCES session_contexts(session_id),
                     title TEXT NOT NULL CHECK(length(trim(title)) > 0),
                     harness_kind TEXT NOT NULL CHECK(harness_kind IN ('codex','claude','kimi')),
                     last_profile TEXT NOT NULL,
                     target_template_id TEXT NOT NULL,
                     state TEXT NOT NULL CHECK(state IN (
                         'provisioning','running','disconnected','checkpointing','closing',
                         'archived','lost','error','destroyed-with-data-loss'
                     )),
                     native_session_id TEXT,
                     acp_session_title TEXT,
                     session_title_override TEXT,
                     updated_at TEXT NOT NULL,
                     last_viewed_event_sequence INTEGER NOT NULL DEFAULT 0,
                     last_error TEXT,
                     resource_allocation TEXT,
                     last_checkpoint_error TEXT,
                     project_directory BLOB
                 ) STRICT;
                 CREATE TABLE session_mounts (
                     session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                     ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                     source BLOB NOT NULL,
                     destination BLOB NOT NULL,
                     PRIMARY KEY(session_id, ordinal),
                     UNIQUE(session_id, destination)
                 ) STRICT;
                 CREATE TABLE session_checkpoints (
                     session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                     archive_path BLOB NOT NULL,
                     sha256 TEXT NOT NULL,
                     created_at TEXT NOT NULL,
                     event_sequence INTEGER NOT NULL CHECK(event_sequence >= 0)
                 ) STRICT;
                 CREATE TABLE prompt_history (
                     history_id INTEGER PRIMARY KEY,
                     session_id TEXT NOT NULL REFERENCES session_contexts(session_id),
                     event_sequence INTEGER NOT NULL CHECK(event_sequence >= 0),
                     submitted_at TEXT NOT NULL,
                     text TEXT NOT NULL CHECK(length(trim(text)) > 0),
                     UNIQUE(session_id, event_sequence)
                 ) STRICT;
                 CREATE TABLE session_targets (
                     session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                     kind TEXT NOT NULL CHECK(kind IN ('local-podman','apple-container','aws-ec2','ssh-bare','ssh-podman')),
                     host TEXT,
                     resource_id TEXT,
                     address TEXT,
                     workspace BLOB,
                     worker_id TEXT
                 ) STRICT;
                 INSERT INTO schema_migrations(version, applied_at) VALUES (1, 'now'), (2, 'now'), (3, 'now'), (4, 'now');
                 INSERT INTO session_contexts VALUES ('old-session', 'project-1', 'now');
                 INSERT INTO sessions(
                     session_id, title, harness_kind, last_profile, target_template_id,
                     state, updated_at
                 ) VALUES (
                     'old-session', 'old session', 'codex', 'codex', 'local',
                     'running', 'now'
                 );
                 INSERT INTO session_targets(session_id, kind, resource_id)
                     VALUES ('old-session', 'local-podman', 'container-1');
                 PRAGMA user_version = 4;",
            )
            .unwrap();
        drop(connection);

        let connection = open(&database).unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert!(
            connection
                .query_row(
                    "SELECT managed_worktree IS NULL FROM sessions WHERE session_id = 'old-session'",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT resource_id FROM session_targets WHERE session_id = 'old-session'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "container-1"
        );
        connection
            .execute(
                "DELETE FROM session_targets WHERE session_id = 'old-session'",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO session_targets(session_id, kind, workspace)
                 VALUES ('old-session', 'local-bare', ?1)",
                [path_to_blob(Path::new("/var/lib/hel/workers/old-session"))],
            )
            .unwrap();
    }

    #[test]
    fn version_five_database_establishes_new_receipt_and_seeds_projection() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY CHECK(version > 0),
                     applied_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE session_contexts (
                     session_id TEXT PRIMARY KEY,
                     bundle_id TEXT NOT NULL,
                     created_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE sessions (
                     session_id TEXT PRIMARY KEY REFERENCES session_contexts(session_id),
                     title TEXT NOT NULL CHECK(length(trim(title)) > 0),
                     harness_kind TEXT NOT NULL CHECK(harness_kind IN ('codex','claude','kimi')),
                     last_profile TEXT NOT NULL,
                     target_template_id TEXT NOT NULL,
                     state TEXT NOT NULL CHECK(state IN (
                         'provisioning','running','disconnected','checkpointing','closing',
                         'archived','lost','error','destroyed-with-data-loss'
                     )),
                     native_session_id TEXT,
                     acp_session_title TEXT,
                     session_title_override TEXT,
                     updated_at TEXT NOT NULL,
                     last_viewed_event_sequence INTEGER NOT NULL DEFAULT 0,
                     last_error TEXT,
                     resource_allocation TEXT,
                     last_checkpoint_error TEXT,
                     project_directory BLOB
                 ) STRICT;
                 CREATE TABLE session_mounts (
                     session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                     ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                     source BLOB NOT NULL,
                     destination BLOB NOT NULL,
                     PRIMARY KEY(session_id, ordinal),
                     UNIQUE(session_id, destination)
                 ) STRICT;
                 CREATE TABLE session_checkpoints (
                     session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                     archive_path BLOB NOT NULL,
                     sha256 TEXT NOT NULL,
                     created_at TEXT NOT NULL,
                     event_sequence INTEGER NOT NULL CHECK(event_sequence >= 0)
                 ) STRICT;
                 CREATE TABLE prompt_history (
                     history_id INTEGER PRIMARY KEY,
                     session_id TEXT NOT NULL REFERENCES session_contexts(session_id),
                     event_sequence INTEGER NOT NULL CHECK(event_sequence >= 0),
                     submitted_at TEXT NOT NULL,
                     text TEXT NOT NULL CHECK(length(trim(text)) > 0),
                     UNIQUE(session_id, event_sequence)
                 ) STRICT;
                 INSERT INTO schema_migrations(version, applied_at)
                     VALUES (1, 'now'), (2, 'now'), (3, 'now'), (4, 'now'), (5, 'now');
                 INSERT INTO session_contexts VALUES ('session-1', 'project-1', 'now');
                 INSERT INTO sessions(
                     session_id, title, harness_kind, last_profile, target_template_id,
                     state, updated_at, last_viewed_event_sequence
                 ) VALUES (
                     'session-1', 'old session', 'codex', 'codex', 'local',
                     'running', 'now', 41
                 );
                 INSERT INTO prompt_history(session_id, event_sequence, submitted_at, text)
                     VALUES ('session-1', 9, 'now', 'remember the ordinal');
                 PRAGMA user_version = 5;",
            )
            .unwrap();
        drop(connection);

        let connection = open(&database).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT viewed_through_event_ordinal FROM sessions WHERE session_id = 'session-1'",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .unwrap(),
            0
        );
        assert!(
            connection
                .query_row(
                    "SELECT managed_worktree IS NULL FROM sessions WHERE session_id = 'session-1'",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT applied_event_ordinal FROM materialized_sessions WHERE session_id = 'session-1'",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT text FROM prompt_history
                     WHERE session_id = 'session-1' AND event_ordinal = 9",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "remember the ordinal"
        );
        connection
            .execute(
                "UPDATE sessions SET state = 'destroying' WHERE session_id = 'session-1'",
                [],
            )
            .unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT applied_event_digest FROM materialized_sessions
                     WHERE session_id = 'session-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            RELAY_EVENT_GENESIS_DIGEST
        );
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM pragma_table_info('materialized_sessions')
                     WHERE name = 'last_activity_at_ms'",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM pragma_table_info('materialized_transcript_items')
                     WHERE name = 'latest_content_event_ordinal'",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn version_seven_database_runs_the_queue_kind_and_grok_harness_migrations() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(&format!(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY CHECK(version > 0),
                     applied_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE session_contexts (
                     session_id TEXT PRIMARY KEY,
                     bundle_id TEXT NOT NULL,
                     created_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE sessions (
                     session_id TEXT PRIMARY KEY REFERENCES session_contexts(session_id),
                     title TEXT NOT NULL CHECK(length(trim(title)) > 0),
                     harness_kind TEXT NOT NULL CHECK(harness_kind IN ('codex','claude','kimi')),
                     last_profile TEXT NOT NULL,
                     target_template_id TEXT NOT NULL,
                     state TEXT NOT NULL CHECK(state IN (
                         'provisioning','running','disconnected','checkpointing','closing','destroying',
                         'archived','lost','error','destroyed-with-data-loss'
                     )),
                     native_session_id TEXT,
                     acp_session_title TEXT,
                     session_title_override TEXT,
                     updated_at TEXT NOT NULL,
                     detached_after_event_ordinal INTEGER NOT NULL DEFAULT 0
                         CHECK(detached_after_event_ordinal >= 0),
                     last_error TEXT,
                     resource_allocation TEXT,
                     last_checkpoint_error TEXT,
                     project_directory BLOB,
                     managed_worktree TEXT
                 ) STRICT;
                 CREATE TABLE session_targets (
                     session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                     kind TEXT NOT NULL CHECK(kind IN ('local-bare','local-podman','apple-container','aws-ec2','ssh-bare','ssh-podman')),
                     host TEXT,
                     resource_id TEXT,
                     address TEXT,
                     workspace BLOB,
                     worker_id TEXT
                 ) STRICT;
                 CREATE TABLE session_mounts (
                     session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                     ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                     source BLOB NOT NULL,
                     destination BLOB NOT NULL,
                     PRIMARY KEY(session_id, ordinal),
                     UNIQUE(session_id, destination)
                 ) STRICT;
                 CREATE TABLE session_checkpoints (
                     session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                     archive_path BLOB NOT NULL,
                     sha256 TEXT NOT NULL,
                     created_at TEXT NOT NULL,
                     event_frontier INTEGER NOT NULL CHECK(event_frontier >= 0)
                 ) STRICT;
                 CREATE TABLE prompt_history (
                     history_id INTEGER PRIMARY KEY,
                     session_id TEXT NOT NULL REFERENCES session_contexts(session_id),
                     event_ordinal INTEGER NOT NULL CHECK(event_ordinal >= 0),
                     submitted_at TEXT NOT NULL,
                     text TEXT NOT NULL CHECK(length(trim(text)) > 0),
                     UNIQUE(session_id, event_ordinal)
                 ) STRICT;
                 CREATE TABLE materialized_sessions (
                     session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                     applied_event_ordinal INTEGER NOT NULL DEFAULT 0
                         CHECK(applied_event_ordinal >= 0),
                     applied_event_digest TEXT NOT NULL
                         DEFAULT '{RELAY_EVENT_GENESIS_DIGEST}'
                         CHECK(length(applied_event_digest) = 64
                               AND applied_event_digest NOT GLOB '*[^0-9a-f]*'),
                     last_activity_at_ms INTEGER,
                     execution_state TEXT NOT NULL DEFAULT 'idle'
                         CHECK(execution_state IN ('idle','running','closing','closed')),
                     running_started_at_ms INTEGER,
                     session_title TEXT,
                     configuration_json TEXT NOT NULL DEFAULT '{{}}'
                 ) STRICT;
                 CREATE TABLE materialized_transcript_items (
                     session_id TEXT NOT NULL REFERENCES materialized_sessions(session_id) ON DELETE CASCADE,
                     stable_id TEXT NOT NULL,
                     position INTEGER NOT NULL CHECK(position > 0),
                     latest_content_event_ordinal INTEGER,
                     created_at_ms INTEGER NOT NULL,
                     last_changed_at_ms INTEGER NOT NULL,
                     body_json TEXT NOT NULL,
                     PRIMARY KEY(session_id, stable_id)
                 ) STRICT;
                 CREATE TABLE materialized_queued_prompts (
                     session_id TEXT NOT NULL REFERENCES materialized_sessions(session_id) ON DELETE CASCADE,
                     ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                     command_id TEXT NOT NULL,
                     content_json TEXT NOT NULL,
                     queued_at_ms INTEGER NOT NULL,
                     PRIMARY KEY(session_id, ordinal),
                     UNIQUE(session_id, command_id)
                 ) STRICT;
                 INSERT INTO schema_migrations(version, applied_at)
                     VALUES (1, 'now'), (2, 'now'), (3, 'now'), (4, 'now'), (5, 'now'),
                            (6, 'now'), (7, 'now');
                 INSERT INTO session_contexts VALUES ('session-1', 'project-1', 'now');
                 INSERT INTO sessions(
                     session_id, title, harness_kind, last_profile, target_template_id,
                     state, updated_at, detached_after_event_ordinal, last_error
                 ) VALUES (
                     'session-1', 'old session', 'kimi', 'kimi-1', 'raw-localhost',
                     'running', 'now', 12, 'nothing yet'
                 );
                 INSERT INTO session_contexts VALUES ('session-2', 'project-1', 'now');
                 INSERT INTO sessions(
                     session_id, title, harness_kind, last_profile, target_template_id,
                     state, updated_at, detached_after_event_ordinal
                 ) VALUES (
                     'session-2', 'stopped session', 'kimi', 'kimi-1', 'podman',
                     'archived', 'now', 0
                 );
                 INSERT INTO materialized_sessions(session_id) VALUES ('session-2');
                 INSERT INTO session_targets(session_id, kind, resource_id)
                     VALUES ('session-1', 'local-podman', 'container-1');
                 INSERT INTO materialized_sessions(session_id) VALUES ('session-1');
                 INSERT INTO materialized_queued_prompts(
                     session_id, ordinal, command_id, content_json, queued_at_ms
                 ) VALUES ('session-1', 0, 'queued-1', '[]', 1600);
                 PRAGMA user_version = 7;",
            ))
            .unwrap();
        drop(connection);

        let connection = open(&database).unwrap();

        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT target_template_id FROM sessions WHERE session_id = 'session-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "localhost"
        );
        // Version 8 gave queue entries a kind. A row written before it is a
        // prompt.
        assert!(table_has_column(&connection, "materialized_queued_prompts", "kind_json").unwrap());
        assert_eq!(
            connection
                .query_row(
                    "SELECT kind_json FROM materialized_queued_prompts
                     WHERE command_id = 'queued-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "\"prompt\""
        );
        // Version 9 rebuilt `sessions`; the existing row survives with every
        // column intact.
        let (title, harness, ordinal, error, draft): (String, String, u64, String, String) =
            connection
                .query_row(
                    "SELECT title, harness_kind, viewed_through_event_ordinal, last_error,
                            draft_input
                     FROM sessions WHERE session_id = 'session-1'",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .unwrap();
        assert_eq!(
            (
                title.as_str(),
                harness.as_str(),
                ordinal,
                error.as_str(),
                draft.as_str()
            ),
            ("old session", "kimi", 12, "nothing yet", "")
        );
        // Children still resolve through the replacement table.
        assert_eq!(
            connection
                .query_row(
                    "SELECT resource_id FROM session_targets WHERE session_id = 'session-1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "container-1"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM materialized_sessions WHERE session_id = 'session-1'",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .unwrap(),
            1
        );

        connection
            .execute_batch(
                "INSERT INTO session_contexts VALUES ('session-3', 'project-1', 'now');
                 INSERT INTO sessions(
                     session_id, title, harness_kind, last_profile, target_template_id,
                     state, updated_at
                 ) VALUES (
                     'session-3', 'grok session', 'grok', 'grok-1', 'podman',
                     'running', 'now'
                 );",
            )
            .expect("a migrated database must accept a Grok Build session");

        connection
            .execute_batch(
                "INSERT INTO session_contexts VALUES ('session-4', 'project-1', 'now');
                 INSERT INTO sessions(
                     session_id, title, harness_kind, last_profile, target_template_id,
                     state, updated_at
                 ) VALUES (
                     'session-4', 'deepseek session', 'deepseek', 'deepseek-1', 'podman',
                     'running', 'now'
                 );",
            )
            .expect("a migrated database must accept a DeepSeek Harness session");

        // Version 10 renamed the `archived` lifecycle state to `stopped` and
        // gave sessions a display-only archived flag, defaulted off.
        let (state, archived): (String, bool) = connection
            .query_row(
                "SELECT state, archived FROM sessions WHERE session_id = 'session-2'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "stopped");
        assert!(!archived);
        connection
            .execute(
                "UPDATE sessions SET state = 'archived' WHERE session_id = 'session-2'",
                [],
            )
            .expect_err("the retired state name is no longer accepted");

        // The hidden set for native sessions lives in Hel's own database.
        connection
            .execute_batch(
                "INSERT INTO hidden_native_sessions(harness_kind, native_session_id, hidden_at)
                     VALUES ('codex', 'native-1', 'now');",
            )
            .expect("a migrated database holds the native hidden set");
    }

    /// Archiving is a display choice with its own writer: it must not disturb
    /// the lifecycle state, checkpoint, or titles other writers own.
    #[test]
    fn the_archived_flag_round_trips_without_touching_other_session_fields() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut session = session("session-1", "project-1");
        save_session_to(&database, &session).unwrap();
        assert!(!load_state_from(&database).unwrap().sessions["session-1"].archived);

        set_session_archived_to(&database, "session-1", true).unwrap();
        let reloaded = load_state_from(&database).unwrap().sessions["session-1"].clone();
        assert!(reloaded.archived);
        session.archived = true;
        assert_eq!(reloaded.state, session.state);
        assert_eq!(reloaded.checkpoint, session.checkpoint);
        assert_eq!(reloaded.acp_session_title, session.acp_session_title);

        set_session_archived_to(&database, "session-1", false).unwrap();
        assert!(!load_state_from(&database).unwrap().sessions["session-1"].archived);
        assert!(set_session_archived_to(&database, "missing", true).is_err());
    }

    /// Hel never writes a harness home, so the hidden set for native sessions
    /// is Hel's own state and is keyed per harness.
    #[test]
    fn the_native_hidden_set_is_keyed_by_harness_and_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        assert!(hidden_native_sessions_from(&database).unwrap().is_empty());

        set_native_session_hidden_to(&database, HarnessKind::Codex, "native-1", true).unwrap();
        set_native_session_hidden_to(&database, HarnessKind::Codex, "native-1", true).unwrap();
        set_native_session_hidden_to(&database, HarnessKind::Claude, "native-1", true).unwrap();
        assert_eq!(
            hidden_native_sessions_from(&database).unwrap(),
            BTreeSet::from([
                (HarnessKind::Claude, "native-1".to_owned()),
                (HarnessKind::Codex, "native-1".to_owned()),
            ])
        );

        set_native_session_hidden_to(&database, HarnessKind::Codex, "native-1", false).unwrap();
        assert_eq!(
            hidden_native_sessions_from(&database).unwrap(),
            BTreeSet::from([(HarnessKind::Claude, "native-1".to_owned())])
        );
        // Revealing something that was never hidden is not an error.
        set_native_session_hidden_to(&database, HarnessKind::Grok, "native-9", false).unwrap();
        assert!(set_native_session_hidden_to(&database, HarnessKind::Grok, "  ", true).is_err());
    }

    #[test]
    fn a_fresh_database_accepts_a_session_for_every_harness_kind() {
        let directory = tempfile::tempdir().unwrap();
        let connection = open(&directory.path().join("hel.sqlite3")).unwrap();

        for (index, kind) in HarnessKind::ALL.into_iter().enumerate() {
            let session_id = format!("session-{index}");
            connection
                .execute(
                    "INSERT INTO session_contexts VALUES (?1, 'project-1', 'now')",
                    params![session_id],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO sessions(
                         session_id, title, harness_kind, last_profile, target_template_id,
                         state, updated_at
                     ) VALUES (?1, ?2, ?3, 'profile-1', 'podman', 'running', 'now')",
                    params![session_id, format!("{kind:?} session"), kind.id()],
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "the sessions harness_kind CHECK must admit {:?} ({:?}): {error}",
                        kind,
                        kind.id()
                    )
                });
        }

        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM sessions", [], |row| row
                    .get::<_, usize>(0))
                .unwrap(),
            HarnessKind::ALL.len()
        );
    }

    #[test]
    fn master_version_six_database_converges_to_the_relay_schema() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY CHECK(version > 0),
                     applied_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE session_contexts (
                     session_id TEXT PRIMARY KEY,
                     bundle_id TEXT NOT NULL,
                     created_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE sessions (
                     session_id TEXT PRIMARY KEY REFERENCES session_contexts(session_id),
                     title TEXT,
                     harness_kind TEXT,
                     last_profile TEXT,
                     target_template_id TEXT,
                     state TEXT,
                     native_session_id TEXT,
                     acp_session_title TEXT,
                     session_title_override TEXT,
                     updated_at TEXT,
                     last_viewed_event_sequence INTEGER NOT NULL DEFAULT 0,
                     last_error TEXT,
                     resource_allocation TEXT,
                     last_checkpoint_error TEXT,
                     project_directory BLOB,
                     managed_worktree TEXT
                 ) STRICT;
                 CREATE TABLE session_mounts (
                     session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                     ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                     source BLOB NOT NULL,
                     destination BLOB NOT NULL,
                     PRIMARY KEY(session_id, ordinal),
                     UNIQUE(session_id, destination)
                 ) STRICT;
                 CREATE TABLE session_checkpoints (
                     session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                     archive_path BLOB,
                     sha256 TEXT,
                     created_at TEXT,
                     event_sequence INTEGER NOT NULL DEFAULT 0
                 ) STRICT;
                 CREATE TABLE prompt_history (
                     history_id INTEGER PRIMARY KEY,
                     session_id TEXT REFERENCES session_contexts(session_id),
                     event_sequence INTEGER NOT NULL DEFAULT 0,
                     submitted_at TEXT,
                     text TEXT
                 ) STRICT;
                 INSERT INTO schema_migrations(version, applied_at)
                     VALUES (1, 'now'), (2, 'now'), (3, 'now'), (4, 'now'), (5, 'now'), (6, 'now');
                 PRAGMA user_version = 6;",
            )
            .unwrap();
        drop(connection);

        let connection = open(&database).unwrap();

        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        for (table, column) in [
            ("sessions", "viewed_through_event_ordinal"),
            ("sessions", "managed_worktree"),
            ("session_checkpoints", "event_frontier"),
            ("prompt_history", "event_ordinal"),
            ("materialized_sessions", "applied_event_digest"),
            ("materialized_sessions", "pending_elicitations_json"),
            ("materialized_queued_prompts", "kind_json"),
        ] {
            assert!(table_has_column(&connection, table, column).unwrap());
        }
    }

    #[test]
    fn queue_entry_kinds_round_trip_and_default_to_prompt() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        let mut materialized = materialized_session("session-1");
        materialized.queued_prompts.push(MaterializedQueuedPrompt {
            command_id: "config-1".into(),
            kind: QueuedCommandKind::SetConfig {
                key: "model".into(),
                value: "sonnet".into(),
            },
            content: vec![serde_json::json!({"type": "text", "text": "/model sonnet"})],
            queued_at_ms: 1_600,
        });
        save_materialized_session_to(&database, &materialized).unwrap();

        let loaded = load_materialized_session_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.queued_prompts, materialized.queued_prompts);
        assert_eq!(
            load_materialized_queued_prompts_from(&database).unwrap()["session-1"],
            materialized.queued_prompts
        );

        // Rows written before queue entries carried a kind load as prompts.
        let connection = open(&database).unwrap();
        connection
            .execute(
                "INSERT INTO materialized_queued_prompts(
                     session_id, ordinal, command_id, content_json, queued_at_ms
                 ) VALUES ('session-1', 9, 'legacy-1', ?1, 1700)",
                params![serde_json::json!([{"type": "text", "text": "older"}]).to_string()],
            )
            .unwrap();
        drop(connection);

        let loaded = load_materialized_session_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.queued_prompts.last().unwrap().command_id, "legacy-1");
        assert_eq!(
            loaded.queued_prompts.last().unwrap().kind,
            QueuedCommandKind::Prompt
        );
    }

    #[test]
    fn materialized_session_round_trip_preserves_typed_projection() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        let materialized = materialized_session("session-1");

        save_materialized_session_to(&database, &materialized).unwrap();

        let loaded = load_materialized_session_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert_eq!(loaded, materialized);
        assert_eq!(loaded.last_activity_at_ms(), Some(1_500));
    }

    #[test]
    fn materialized_summary_loads_messages_without_deserializing_full_history() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        let mut materialized = materialized_session("session-1");
        materialized.transcript.extend([
            Arc::new(TranscriptItem {
                stable_id: "user:5".into(),
                position: 5,
                latest_content_event_ordinal: None,
                created_at_ms: 1_600,
                last_changed_at_ms: 1_600,
                body: TranscriptBody::User {
                    content: vec![serde_json::json!({"type": "text", "text": "ship it"})],
                },
            }),
            Arc::new(TranscriptItem {
                stable_id: "agent:6".into(),
                position: 6,
                latest_content_event_ordinal: Some(7),
                created_at_ms: 1_700,
                last_changed_at_ms: 1_700,
                body: TranscriptBody::Agent {
                    chunks: vec![serde_json::json!({
                        "content": {"type": "text", "text": "Finished"}
                    })],
                    streaming: false,
                },
            }),
            Arc::new(TranscriptItem {
                stable_id: "user:7".into(),
                position: 7,
                latest_content_event_ordinal: None,
                created_at_ms: 1_800,
                last_changed_at_ms: 1_800,
                body: TranscriptBody::User {
                    content: vec![serde_json::json!({
                        "type": "text",
                        "text": "one more thing"
                    })],
                },
            }),
        ]);
        save_materialized_session_to(&database, &materialized).unwrap();

        // A large or damaged tool result must not be read just to build the
        // dashboard's two message snippets.
        open(&database)
            .unwrap()
            .execute(
                "UPDATE materialized_transcript_items
                 SET body_json = 'not-json' WHERE stable_id = 'tool:call-1'",
                [],
            )
            .unwrap();

        let summary = load_materialized_session_summary_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_user_message.as_deref(), Some("one more thing"));
        assert_eq!(summary.last_agent_message.as_deref(), Some("Finished"));
        assert!(!summary.last_agent_message_follows_last_user);
        assert_eq!(summary.agent_message_latest_content_ordinals, vec![2, 7]);
        assert_eq!(summary.execution, materialized.execution);
        assert!(load_materialized_session_from(&database, "session-1").is_err());
    }

    #[test]
    fn queued_prompt_loader_does_not_deserialize_transcript_history() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        let materialized = materialized_session("session-1");
        let expected = materialized.queued_prompts.clone();
        save_materialized_session_to(&database, &materialized).unwrap();
        let connection = open(&database).unwrap();
        connection
            .execute(
                "UPDATE materialized_transcript_items SET body_json = 'not-json'",
                [],
            )
            .unwrap();
        drop(connection);

        let queues = load_materialized_queued_prompts_from(&database).unwrap();

        assert_eq!(queues.get("session-1"), Some(&expected));
        assert!(load_materialized_session_from(&database, "session-1").is_err());
    }

    /// Resume compares frontiers to decide whether to rebuild a projection,
    /// and clears the queue without touching the transcript when it does not.
    #[test]
    fn a_queue_replacement_keeps_the_projection_frontier_and_transcript() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        let materialized = materialized_session("session-1");
        assert!(!materialized.queued_prompts.is_empty());
        save_materialized_session_to(&database, &materialized).unwrap();

        assert_eq!(
            materialized_event_frontier_from(&database, "session-1").unwrap(),
            Some((materialized.applied_event_ordinal, event_digest(7)))
        );
        assert_eq!(
            materialized_event_frontier_from(&database, "unknown").unwrap(),
            None
        );

        replace_materialized_queued_prompts_in(&database, "session-1", &[]).unwrap();

        let cleared = load_materialized_session_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert!(cleared.queued_prompts.is_empty());
        assert_eq!(cleared.transcript, materialized.transcript);
        assert_eq!(
            cleared.applied_event_ordinal,
            materialized.applied_event_ordinal
        );
        assert_eq!(
            cleared.applied_event_digest,
            materialized.applied_event_digest
        );

        replace_materialized_queued_prompts_in(
            &database,
            "session-1",
            &materialized.queued_prompts,
        )
        .unwrap();
        assert_eq!(
            load_materialized_session_from(&database, "session-1").unwrap(),
            Some(materialized)
        );
    }

    #[test]
    fn operational_session_updates_do_not_delete_its_projection() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut operational = session("session-1", "project-1");
        save_session_to(&database, &operational).unwrap();
        let materialized = materialized_session("session-1");
        save_materialized_session_to(&database, &materialized).unwrap();

        operational.session_title_override = Some("renamed".into());
        save_session_to(&database, &operational).unwrap();

        assert_eq!(
            load_materialized_session_from(&database, "session-1").unwrap(),
            Some(materialized)
        );
    }

    #[test]
    fn projection_event_application_is_atomic_ordered_and_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        let first_item = TranscriptItem {
            stable_id: "agent:1".into(),
            position: 1,
            latest_content_event_ordinal: Some(1),
            created_at_ms: 100,
            last_changed_at_ms: 100,
            body: TranscriptBody::Agent {
                chunks: vec![serde_json::json!({
                    "content": {"type": "text", "text": "hel"}
                })],
                streaming: true,
            },
        };
        let first = MaterializedSessionMutation {
            last_activity_at_ms: Some(105),
            execution: Some(MaterializedExecutionState::Running { started_at_ms: 90 }),
            session_title: Some(Some("Testing".into())),
            configuration: Some(BTreeMap::from([("model".into(), serde_json::json!("sol"))])),
            transcript: vec![TranscriptMutation::Upsert(first_item.clone())],
            queued_prompts: Some(vec![MaterializedQueuedPrompt {
                command_id: "prompt-2".into(),
                kind: QueuedCommandKind::Prompt,
                content: vec![serde_json::json!({"type": "text", "text": "next"})],
                queued_at_ms: 105,
            }]),
            pending_elicitations: None,
        };
        let first_digest = event_digest(1);
        let second_digest = event_digest(2);
        let third_digest = event_digest(3);
        assert_eq!(
            apply_projection_event_to(
                &database,
                "session-1",
                1,
                RELAY_EVENT_GENESIS_DIGEST,
                &first_digest,
                &first,
            )
            .unwrap(),
            ProjectionApplyOutcome::Applied
        );

        let destructive_duplicate = MaterializedSessionMutation {
            transcript: vec![TranscriptMutation::Remove {
                stable_id: first_item.stable_id.clone(),
            }],
            ..MaterializedSessionMutation::default()
        };
        assert_eq!(
            apply_projection_event_to(
                &database,
                "session-1",
                1,
                RELAY_EVENT_GENESIS_DIGEST,
                &first_digest,
                &destructive_duplicate,
            )
            .unwrap(),
            ProjectionApplyOutcome::AlreadyApplied
        );
        assert!(
            apply_projection_event_to(
                &database,
                "session-1",
                1,
                RELAY_EVENT_GENESIS_DIGEST,
                &event_digest(99),
                &MaterializedSessionMutation::default(),
            )
            .unwrap_err()
            .to_string()
            .contains("digest mismatch")
        );
        assert!(
            apply_projection_event_to(
                &database,
                "session-1",
                3,
                &first_digest,
                &third_digest,
                &MaterializedSessionMutation::default()
            )
            .unwrap_err()
            .to_string()
            .contains("expected ordinal 2")
        );
        assert!(
            apply_projection_event_to(
                &database,
                "session-1",
                2,
                &event_digest(99),
                &second_digest,
                &MaterializedSessionMutation::default(),
            )
            .unwrap_err()
            .to_string()
            .contains("chain diverged")
        );

        let updated_item = TranscriptItem {
            latest_content_event_ordinal: Some(2),
            last_changed_at_ms: 120,
            body: TranscriptBody::Agent {
                chunks: vec![serde_json::json!({
                    "content": {"type": "text", "text": "hello"}
                })],
                streaming: false,
            },
            ..first_item.clone()
        };
        apply_projection_event_to(
            &database,
            "session-1",
            2,
            &first_digest,
            &second_digest,
            &MaterializedSessionMutation {
                last_activity_at_ms: Some(120),
                transcript: vec![TranscriptMutation::Upsert(updated_item.clone())],
                ..MaterializedSessionMutation::default()
            },
        )
        .unwrap();

        let regressed_content_ordinal = TranscriptItem {
            latest_content_event_ordinal: Some(1),
            last_changed_at_ms: 130,
            ..updated_item.clone()
        };
        assert!(
            apply_projection_event_to(
                &database,
                "session-1",
                3,
                &second_digest,
                &third_digest,
                &MaterializedSessionMutation {
                    last_activity_at_ms: Some(130),
                    transcript: vec![TranscriptMutation::Upsert(regressed_content_ordinal)],
                    ..MaterializedSessionMutation::default()
                }
            )
            .unwrap_err()
            .to_string()
            .contains("latest content ordinal backwards")
        );

        let invalid_identity = TranscriptItem {
            position: 2,
            ..updated_item
        };
        assert!(
            apply_projection_event_to(
                &database,
                "session-1",
                3,
                &second_digest,
                &third_digest,
                &MaterializedSessionMutation {
                    last_activity_at_ms: Some(130),
                    transcript: vec![TranscriptMutation::Upsert(invalid_identity)],
                    ..MaterializedSessionMutation::default()
                }
            )
            .unwrap_err()
            .to_string()
            .contains("immutable identity")
        );
        let loaded = load_materialized_session_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.applied_event_ordinal, 2);
        assert_eq!(loaded.applied_event_digest, second_digest);
        assert_eq!(loaded.last_activity_at_ms(), Some(120));
        assert_eq!(loaded.transcript.len(), 1);
        assert_eq!(loaded.transcript[0].latest_content_event_ordinal, Some(2));
        assert_eq!(
            loaded.transcript[0].body,
            TranscriptBody::Agent {
                chunks: vec![serde_json::json!({
                    "content": {"type": "text", "text": "hello"}
                })],
                streaming: false,
            }
        );
    }

    #[test]
    fn detach_receipt_is_monotonic_and_cannot_pass_projection() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut operational = session("session-1", "project-1");
        operational.viewed_through_event_ordinal = 0;
        save_session_to(&database, &operational).unwrap();
        let mut previous_digest = RELAY_EVENT_GENESIS_DIGEST.to_owned();
        for ordinal in 1..=2 {
            let digest = event_digest(ordinal);
            apply_projection_event_to(
                &database,
                "session-1",
                ordinal,
                &previous_digest,
                &digest,
                &MaterializedSessionMutation::default(),
            )
            .unwrap();
            previous_digest = digest;
        }

        assert_eq!(
            advance_viewed_through_event_ordinal_to(&database, "session-1", 2).unwrap(),
            2
        );
        assert_eq!(
            advance_viewed_through_event_ordinal_to(&database, "session-1", 1).unwrap(),
            2
        );
        assert!(
            advance_viewed_through_event_ordinal_to(&database, "session-1", 3)
                .unwrap_err()
                .to_string()
                .contains("projection is at 2")
        );
        assert_eq!(
            load_state_from(&database).unwrap().sessions["session-1"].viewed_through_event_ordinal,
            2
        );
    }

    #[test]
    fn session_draft_input_round_trips_and_an_empty_draft_clears_it() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();

        assert_eq!(
            load_state_from(&database).unwrap().sessions["session-1"].draft_input,
            ""
        );

        set_session_draft_input_at(&database, "session-1", "half typed thought").unwrap();
        assert_eq!(
            load_state_from(&database).unwrap().sessions["session-1"].draft_input,
            "half typed thought"
        );

        // An ordinary session save must not roll the draft back.
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        assert_eq!(
            load_state_from(&database).unwrap().sessions["session-1"].draft_input,
            "half typed thought"
        );

        set_session_draft_input_at(&database, "session-1", "").unwrap();
        assert_eq!(
            load_state_from(&database).unwrap().sessions["session-1"].draft_input,
            ""
        );

        assert!(
            set_session_draft_input_at(&database, "missing", "text")
                .unwrap_err()
                .to_string()
                .contains("unknown session missing")
        );
    }

    #[test]
    fn projection_activity_watermark_is_atomic_and_monotonic() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        let first_digest = event_digest(1);
        apply_projection_event_to(
            &database,
            "session-1",
            1,
            RELAY_EVENT_GENESIS_DIGEST,
            &first_digest,
            &MaterializedSessionMutation {
                last_activity_at_ms: Some(500),
                queued_prompts: Some(vec![MaterializedQueuedPrompt {
                    command_id: "queued-1".into(),
                    kind: QueuedCommandKind::Prompt,
                    content: vec![serde_json::json!({"type": "text", "text": "later"})],
                    queued_at_ms: 500,
                }]),
                ..MaterializedSessionMutation::default()
            },
        )
        .unwrap();
        apply_projection_event_to(
            &database,
            "session-1",
            2,
            &first_digest,
            &event_digest(2),
            &MaterializedSessionMutation {
                last_activity_at_ms: Some(400),
                queued_prompts: Some(Vec::new()),
                ..MaterializedSessionMutation::default()
            },
        )
        .unwrap();

        let loaded = load_materialized_session_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert!(loaded.queued_prompts.is_empty());
        assert_eq!(loaded.last_activity_at_ms(), Some(500));
        assert_eq!(loaded.applied_event_ordinal, 2);
    }

    /// One agent message per relay event, shaped so the projection can store it.
    fn agent_message_mutation(ordinal: u64) -> MaterializedSessionMutation {
        MaterializedSessionMutation {
            last_activity_at_ms: Some(1_000 + ordinal as i64),
            transcript: vec![TranscriptMutation::Upsert(TranscriptItem {
                stable_id: format!("item-{ordinal}"),
                position: ordinal,
                latest_content_event_ordinal: Some(ordinal),
                created_at_ms: 1_000 + ordinal as i64,
                last_changed_at_ms: 1_000 + ordinal as i64,
                body: TranscriptBody::Agent {
                    chunks: vec![serde_json::json!({
                        "content": {"type": "text", "text": format!("event {ordinal}")}
                    })],
                    streaming: false,
                },
            })],
            ..MaterializedSessionMutation::default()
        }
    }

    #[test]
    fn projection_page_advances_the_frontier_only_when_the_whole_page_commits() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();

        // The second event breaks the chain only after the first has written
        // its rows, so the page has to unwind work it already did.
        let interrupted = apply_projection_page_to(&database, "session-1", |page| {
            page.apply(
                1,
                RELAY_EVENT_GENESIS_DIGEST,
                &event_digest(1),
                &agent_message_mutation(1),
            )?;
            page.apply(
                3,
                &event_digest(1),
                &event_digest(3),
                &agent_message_mutation(3),
            )
        })
        .unwrap_err();
        assert!(
            interrupted.to_string().contains("expected ordinal 2"),
            "unexpected page failure: {interrupted:#}"
        );

        // The relay retains everything past the last acknowledgement, so an
        // interrupted page must leave the durable frontier where it was.
        let rolled_back = load_materialized_session_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert_eq!(rolled_back.applied_event_ordinal, 0);
        assert_eq!(rolled_back.applied_event_digest, RELAY_EVENT_GENESIS_DIGEST);
        assert!(rolled_back.transcript.is_empty());
        assert_eq!(rolled_back.last_activity_at_ms(), None);

        apply_projection_page_to(&database, "session-1", |page| {
            page.apply(
                1,
                RELAY_EVENT_GENESIS_DIGEST,
                &event_digest(1),
                &agent_message_mutation(1),
            )?;
            page.apply(
                2,
                &event_digest(1),
                &event_digest(2),
                &agent_message_mutation(2),
            )
        })
        .unwrap();

        let committed = load_materialized_session_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert_eq!(committed.applied_event_ordinal, 2);
        assert_eq!(committed.applied_event_digest, event_digest(2));
        assert_eq!(
            committed
                .transcript
                .iter()
                .map(|item| item.stable_id.clone())
                .collect::<Vec<_>>(),
            vec!["item-1".to_owned(), "item-2".to_owned()]
        );
        assert_eq!(committed.last_activity_at_ms(), Some(1_002));
    }

    #[test]
    fn projection_page_coalesces_repeated_item_updates_to_the_final_value() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();

        let first = agent_message_mutation(1);
        let mut second = agent_message_mutation(2);
        let TranscriptMutation::Upsert(second_item) = &mut second.transcript[0] else {
            unreachable!();
        };
        second_item.stable_id = "item-1".into();
        second_item.position = 1;
        second_item.created_at_ms = 1_001;
        apply_projection_page_to(&database, "session-1", |page| {
            page.apply(1, RELAY_EVENT_GENESIS_DIGEST, &event_digest(1), &first)?;
            page.apply(2, &event_digest(1), &event_digest(2), &second)
        })
        .unwrap();

        let committed = load_materialized_session_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert_eq!(committed.applied_event_ordinal, 2);
        assert_eq!(committed.transcript.len(), 1);
        assert_eq!(committed.transcript[0].stable_id, "item-1");
        assert_eq!(
            committed.transcript[0].latest_content_event_ordinal,
            Some(2)
        );
        let TranscriptBody::Agent { chunks, .. } = &committed.transcript[0].body else {
            panic!("coalesced item stayed an agent message");
        };
        assert_eq!(chunks[0]["content"]["text"], "event 2");
        assert_eq!(committed.last_activity_at_ms(), Some(1_002));
    }

    #[test]
    fn projection_page_preserves_remove_then_reinsert_identity_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        apply_projection_event_to(
            &database,
            "session-1",
            1,
            RELAY_EVENT_GENESIS_DIGEST,
            &event_digest(1),
            &agent_message_mutation(1),
        )
        .unwrap();

        let removed = MaterializedSessionMutation {
            transcript: vec![TranscriptMutation::Remove {
                stable_id: "item-1".into(),
            }],
            ..MaterializedSessionMutation::default()
        };
        let mut reinserted = agent_message_mutation(3);
        let TranscriptMutation::Upsert(reinserted_item) = &mut reinserted.transcript[0] else {
            unreachable!();
        };
        reinserted_item.stable_id = "item-1".into();
        apply_projection_page_to(&database, "session-1", |page| {
            page.apply(2, &event_digest(1), &event_digest(2), &removed)?;
            page.apply(3, &event_digest(2), &event_digest(3), &reinserted)
        })
        .unwrap();

        let committed = load_materialized_session_from(&database, "session-1")
            .unwrap()
            .unwrap();
        assert_eq!(committed.applied_event_ordinal, 3);
        assert_eq!(committed.transcript.len(), 1);
        assert_eq!(committed.transcript[0].stable_id, "item-1");
        assert_eq!(committed.transcript[0].position, 3);
        assert_eq!(committed.transcript[0].created_at_ms, 1_003);
    }

    /// The process caches which databases it has migrated. A database that is
    /// gone and recreated under the same path must still be migrated.
    #[test]
    fn reopening_a_recreated_database_migrates_it_again() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        for suffix in ["", "-wal", "-shm"] {
            let sidecar = directory.path().join(format!("hel.sqlite3{suffix}"));
            if sidecar.exists() {
                fs::remove_file(&sidecar).unwrap();
            }
        }

        save_session_to(&database, &session("session-2", "project-1")).unwrap();
        let state = load_state_from(&database).unwrap();
        assert!(state.sessions.contains_key("session-2"));
        assert!(!state.sessions.contains_key("session-1"));
    }

    /// Catch-up throughput: one durable commit per page instead of one per
    /// event. Ignored by default because it measures wall-clock time.
    #[test]
    #[ignore = "timing benchmark; run with --ignored --nocapture"]
    fn projection_page_apply_outruns_per_event_apply() {
        const EVENTS: u64 = 2_000;
        let directory = tempfile::tempdir().unwrap();

        let per_event_database = directory.path().join("per-event/hel.sqlite3");
        save_session_to(&per_event_database, &session("session-1", "project-1")).unwrap();
        let started = std::time::Instant::now();
        for ordinal in 1..=EVENTS {
            apply_projection_event_to(
                &per_event_database,
                "session-1",
                ordinal,
                &event_digest(ordinal - 1),
                &event_digest(ordinal),
                &agent_message_mutation(ordinal),
            )
            .unwrap();
        }
        let per_event = started.elapsed();

        let per_page_database = directory.path().join("per-page/hel.sqlite3");
        save_session_to(&per_page_database, &session("session-1", "project-1")).unwrap();
        let started = std::time::Instant::now();
        apply_projection_page_to(&per_page_database, "session-1", |page| {
            for ordinal in 1..=EVENTS {
                page.apply(
                    ordinal,
                    &event_digest(ordinal - 1),
                    &event_digest(ordinal),
                    &agent_message_mutation(ordinal),
                )?;
            }
            Ok(())
        })
        .unwrap();
        let per_page = started.elapsed();

        println!("{EVENTS} events per-event: {per_event:?}, one page: {per_page:?}");
        assert_eq!(
            load_materialized_session_from(&per_page_database, "session-1")
                .unwrap()
                .unwrap()
                .applied_event_ordinal,
            EVENTS
        );
        assert!(
            per_page < per_event,
            "one page took {per_page:?} against {per_event:?} per event"
        );
    }

    #[test]
    fn deleting_operational_session_retains_relational_history_context() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut state = HelState::default();
        let record = session("session-1", "project-1");
        state.sessions.insert(record.id.clone(), record);
        save_state_to(&database, &state).unwrap();
        let connection = open(&database).unwrap();
        connection
            .execute(
                "INSERT INTO prompt_history(session_id, event_ordinal, submitted_at, text)
                 VALUES ('session-1', 8, '2026-08-12T02:00:00Z', 'remember this')",
                [],
            )
            .unwrap();

        state.sessions.clear();
        save_state_to(&database, &state).unwrap();

        let retained: String = connection
            .query_row(
                "SELECT text FROM prompt_history WHERE session_id = 'session-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained, "remember this");
    }

    #[test]
    fn context_rejects_reassigning_a_session_to_another_project() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut state = HelState::default();
        state
            .sessions
            .insert("session-1".into(), session("session-1", "project-1"));
        save_state_to(&database, &state).unwrap();
        state.sessions.get_mut("session-1").unwrap().bundle_id = "project-2".into();

        assert!(
            save_state_to(&database, &state)
                .unwrap_err()
                .to_string()
                .contains("already associated")
        );
    }

    #[test]
    fn history_search_scopes_by_project_session_and_all_projects() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        for (session, bundle, sequence, text) in [
            ("session-1", "project-1", 1, "fix parser"),
            ("session-2", "project-1", 1, "fix renderer"),
            ("session-3", "project-2", 1, "fix database"),
            ("session-1", "project-1", 2, "fix parser"),
        ] {
            record_prompt_to(
                &database,
                session,
                bundle,
                sequence,
                Some("2026-08-12T00:00:00Z"),
                text,
            )
            .unwrap();
        }

        let project = search_prompts_from(
            &database,
            "session-1",
            "project-1",
            HistoryScope::Project,
            "FIX",
        )
        .unwrap();
        assert_eq!(
            project
                .iter()
                .map(|entry| entry.text.as_str())
                .collect::<Vec<_>>(),
            ["fix parser", "fix renderer"]
        );
        let session = search_prompts_from(
            &database,
            "session-1",
            "project-1",
            HistoryScope::Session,
            "parser",
        )
        .unwrap();
        assert_eq!(session.len(), 1, "duplicate prompt text is suppressed");
        let all = search_prompts_from(
            &database,
            "session-1",
            "project-1",
            HistoryScope::All,
            "database",
        )
        .unwrap();
        assert_eq!(all[0].session_id, "session-3");
    }

    #[test]
    fn rebinding_a_session_moves_its_prompt_history_to_the_new_bundle() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        record_prompt_to(
            &database,
            "session-1",
            "project-1",
            1,
            Some("2026-08-12T00:00:00Z"),
            "fix parser",
        )
        .unwrap();

        rebind_session_bundle_to(&database, "session-1", "project-2").unwrap();

        assert!(
            search_prompts_from(
                &database,
                "session-1",
                "project-1",
                HistoryScope::Project,
                "fix"
            )
            .unwrap()
            .is_empty()
        );
        assert_eq!(
            search_prompts_from(
                &database,
                "session-1",
                "project-2",
                HistoryScope::Project,
                "fix"
            )
            .unwrap()
            .len(),
            1
        );
        // Recording under the new bundle now succeeds where it would have been
        // refused as a bundle mismatch.
        record_prompt_to(
            &database,
            "session-1",
            "project-2",
            2,
            Some("2026-08-12T00:01:00Z"),
            "fix renderer",
        )
        .unwrap();
    }

    #[test]
    fn prompt_recording_is_idempotent_by_session_event_ordinal() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        for _ in 0..2 {
            record_prompt_to(
                &database,
                "session-1",
                "project-1",
                7,
                Some("2026-08-12T00:00:00Z"),
                "ship it",
            )
            .unwrap();
        }
        let connection = open(&database).unwrap();
        let count: i64 = connection
            .query_row("SELECT count(*) FROM prompt_history", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn independent_session_writes_preserve_both_updates() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        save_session_to(&database, &session("session-1", "project-1")).unwrap();
        save_session_to(&database, &session("session-2", "project-2")).unwrap();

        let first_database = database.clone();
        let first = std::thread::spawn(move || {
            let mut record = session("session-1", "project-1");
            record.session_title_override = Some("first changed".into());
            save_session_to(&first_database, &record).unwrap();
        });
        let second_database = database.clone();
        let second = std::thread::spawn(move || {
            let mut record = session("session-2", "project-2");
            record.session_title_override = Some("second changed".into());
            save_session_to(&second_database, &record).unwrap();
        });
        first.join().unwrap();
        second.join().unwrap();

        let state = load_state_from(&database).unwrap();
        assert_eq!(
            state.sessions["session-1"]
                .session_title_override
                .as_deref(),
            Some("first changed")
        );
        assert_eq!(
            state.sessions["session-2"]
                .session_title_override
                .as_deref(),
            Some("second changed")
        );
    }

    #[test]
    fn legacy_json_migration_commits_before_retaining_source_backup() {
        let directory = tempfile::tempdir().unwrap();
        let legacy = directory.path().join("state.json");
        let database = directory.path().join("hel.sqlite3");
        let mut state = HelState::default();
        state
            .sessions
            .insert("session-1".into(), session("session-1", "project-1"));
        state.save_to(&legacy).unwrap();

        migrate_legacy_state_from(&legacy, &database).unwrap();

        state
            .sessions
            .get_mut("session-1")
            .unwrap()
            .viewed_through_event_ordinal = 0;
        assert_eq!(load_state_from(&database).unwrap(), state);
        assert!(!legacy.exists());
        assert!(directory.path().join("state.json.migrated-v1").exists());
    }
}
