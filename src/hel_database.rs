//! Normalized controller state and composer history stored in SQLite.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::hel_config::{HarnessKind, data_dir};
use crate::hel_state::{
    CheckpointMetadata, HelState, SessionRecord, SessionResourceAllocation, SessionState,
    TargetLocator,
};
use crate::hel_targets::AdditionalMount;

const SCHEMA_VERSION: i64 = 5;

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
    migrate_schema(&connection)?;
    Ok(connection)
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
                     'provisioning','running','disconnected','checkpointing','closing',
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
                s.last_viewed_event_sequence, s.last_error, s.resource_allocation,
                s.last_checkpoint_error, s.project_directory
         FROM sessions s JOIN session_contexts c USING(session_id)
         ORDER BY s.session_id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(SessionRecord {
            id: row.get(0)?,
            title: row.get(1)?,
            harness_kind: parse_harness(&row.get::<_, String>(2)?),
            last_profile: row.get(3)?,
            bundle_id: row.get(4)?,
            project_directory: row.get_ref(16)?.blob_or_null()?.map(blob_to_path),
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
            acp_session_title: row.get(8)?,
            session_title_override: row.get(9)?,
            created_at: row.get(10)?,
            updated_at: row.get(11)?,
            last_viewed_event_sequence: row.get::<_, u64>(12)?,
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

fn save_session_to(path: &Path, session: &SessionRecord) -> Result<()> {
    let mut validation = HelState::default();
    validation
        .sessions
        .insert(session.id.clone(), session.clone());
    validation.validate()?;

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
    tx.execute(
        "DELETE FROM sessions WHERE session_id = ?1",
        [session.id.as_str()],
    )?;
    insert_session(&tx, session)?;
    tx.commit()?;
    Ok(())
}

/// Remove one operational session while retaining its relational history
/// context and prompt history.
pub fn delete_session(session_id: &str) -> Result<()> {
    let connection = open(&database_path())?;
    connection.execute("DELETE FROM sessions WHERE session_id = ?1", [session_id])?;
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
    tx.execute("DELETE FROM mount_history WHERE host = ?1", [host])?;
    for (ordinal, source) in sources.iter().enumerate() {
        tx.execute(
            "INSERT INTO mount_history(host, source, ordinal) VALUES (?1, ?2, ?3)",
            params![host, path_to_blob(source), ordinal as i64],
        )?;
    }
    tx.commit()?;
    Ok(())
}

pub fn record_recovery_success(
    session_id: &str,
    native_session_id: &str,
    checkpoint: &CheckpointMetadata,
) -> Result<()> {
    let mut connection = open(&database_path())?;
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
             session_id, archive_path, sha256, created_at, event_sequence
         ) VALUES (?1,?2,?3,?4,?5)
         ON CONFLICT(session_id) DO UPDATE SET
             archive_path = excluded.archive_path,
             sha256 = excluded.sha256,
             created_at = excluded.created_at,
             event_sequence = excluded.event_sequence",
        params![
            session_id,
            path_to_blob(&checkpoint.archive_path),
            checkpoint.sha256,
            checkpoint.created_at,
            checkpoint.event_sequence,
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
    tx.execute("DELETE FROM sessions", [])?;
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
             last_viewed_event_sequence, last_error, resource_allocation,
             last_checkpoint_error, project_directory
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            session.id,
            session.title,
            harness_name(session.harness_kind),
            session.last_profile,
            session.target_template_id,
            session_state_name(session.state),
            session.native_session_id,
            session.acp_session_title,
            session.session_title_override,
            session.updated_at,
            session.last_viewed_event_sequence,
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
        ],
    )?;
    if let Some(target) = &session.target {
        insert_target(tx, &session.id, target)?;
    }
    for (ordinal, mount) in session.additional_mounts.iter().enumerate() {
        tx.execute(
            "INSERT INTO session_mounts(session_id, ordinal, source, destination)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                session.id,
                ordinal as i64,
                path_to_blob(&mount.source),
                path_to_blob(&mount.destination)
            ],
        )?;
    }
    if let Some(checkpoint) = &session.checkpoint {
        tx.execute(
            "INSERT INTO session_checkpoints(session_id, archive_path, sha256, created_at, event_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session.id,
                path_to_blob(&checkpoint.archive_path),
                checkpoint.sha256,
                checkpoint.created_at,
                checkpoint.event_sequence,
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
        "SELECT session_id, source, destination FROM session_mounts ORDER BY session_id, ordinal",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            AdditionalMount {
                source: blob_to_path(row.get_ref(1)?.as_blob()?),
                destination: blob_to_path(row.get_ref(2)?.as_blob()?),
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
        "SELECT session_id, archive_path, sha256, created_at, event_sequence FROM session_checkpoints",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            CheckpointMetadata {
                archive_path: blob_to_path(row.get_ref(1)?.as_blob()?),
                sha256: row.get(2)?,
                created_at: row.get(3)?,
                event_sequence: row.get(4)?,
            },
        ))
    })?;
    for row in rows {
        let (session_id, checkpoint) = row?;
        state.sessions.get_mut(&session_id).unwrap().checkpoint = Some(checkpoint);
    }
    Ok(())
}

pub fn record_prompt(
    session_id: &str,
    bundle_id: &str,
    event_sequence: u64,
    submitted_at: Option<&str>,
    text: &str,
) -> Result<()> {
    record_prompt_to(
        &database_path(),
        session_id,
        bundle_id,
        event_sequence,
        submitted_at,
        text,
    )
}

fn record_prompt_to(
    path: &Path,
    session_id: &str,
    bundle_id: &str,
    event_sequence: u64,
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
        "INSERT INTO prompt_history(session_id, event_sequence, submitted_at, text)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(session_id, event_sequence) DO NOTHING",
        params![
            session_id,
            event_sequence,
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
    let state = HelState::load_json_from(legacy)?;
    save_state_to(database, &state)?;
    let migrated = legacy.with_file_name("state.json.migrated-v1");
    fs::rename(legacy, &migrated)
        .with_context(|| format!("retain migrated Hel state as {}", migrated.display()))?;
    Ok(())
}

fn harness_name(value: HarnessKind) -> &'static str {
    match value {
        HarnessKind::Codex => "codex",
        HarnessKind::Claude => "claude",
        HarnessKind::Kimi => "kimi",
    }
}
fn parse_harness(value: &str) -> HarnessKind {
    match value {
        "codex" => HarnessKind::Codex,
        "claude" => HarnessKind::Claude,
        "kimi" => HarnessKind::Kimi,
        _ => unreachable!(),
    }
}
fn session_state_name(value: SessionState) -> &'static str {
    match value {
        SessionState::Provisioning => "provisioning",
        SessionState::Running => "running",
        SessionState::Disconnected => "disconnected",
        SessionState::Checkpointing => "checkpointing",
        SessionState::Closing => "closing",
        SessionState::Archived => "archived",
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
        "archived" => SessionState::Archived,
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
        .chunks_exact(2)
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
    use rusqlite::OptionalExtension;

    fn session(id: &str, bundle: &str) -> SessionRecord {
        SessionRecord {
            id: id.into(),
            title: "test session".into(),
            harness_kind: HarnessKind::Codex,
            last_profile: "codex".into(),
            bundle_id: bundle.into(),
            project_directory: None,
            target_template_id: "local".into(),
            resource_allocation: Some(SessionResourceAllocation::Container {
                cpus: 8,
                memory_bytes: 32 * 1024 * 1024 * 1024,
            }),
            additional_mounts: vec![AdditionalMount {
                source: PathBuf::from("/host/cache"),
                destination: PathBuf::from("/mnt/cache"),
            }],
            state: SessionState::Archived,
            target: Some(TargetLocator::LocalPodman {
                container_id: "container-1".into(),
            }),
            native_session_id: Some("native-1".into()),
            acp_session_title: Some("Agent title".into()),
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T01:00:00Z".into(),
            last_viewed_event_sequence: 7,
            last_error: None,
            last_checkpoint_error: Some("temporary recovery failure".into()),
            checkpoint: Some(CheckpointMetadata {
                archive_path: PathBuf::from("sessions/test.hel.zip"),
                sha256: "a".repeat(64),
                created_at: "2026-08-12T01:00:00Z".into(),
                event_sequence: 6,
            }),
        }
    }

    #[test]
    fn normalized_state_round_trip_preserves_children_and_order() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hel.sqlite3");
        let mut state = HelState::default();
        let mut record = session("session-1", "project-1");
        record.project_directory = Some(PathBuf::from("/srv/project-1"));
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
                .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
                .optional()
                .unwrap(),
            None
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
                 CREATE TABLE sessions (session_id TEXT PRIMARY KEY) STRICT;
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
                 INSERT INTO sessions VALUES ('old-session');
                 INSERT INTO session_targets(session_id, kind, resource_id)
                     VALUES ('old-session', 'local-podman', 'container-1');
                 PRAGMA user_version = 4;",
            )
            .unwrap();
        drop(connection);

        let connection = open(&database).unwrap();
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
            .execute("INSERT INTO sessions VALUES ('local-session')", [])
            .unwrap();
        connection
            .execute(
                "INSERT INTO session_targets(session_id, kind, workspace)
                 VALUES ('local-session', 'local-bare', ?1)",
                [path_to_blob(Path::new(
                    "/var/lib/hel/workers/local-session",
                ))],
            )
            .unwrap();
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
                "INSERT INTO prompt_history(session_id, event_sequence, submitted_at, text)
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
    fn prompt_recording_is_idempotent_by_session_event_sequence() {
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

        assert_eq!(load_state_from(&database).unwrap(), state);
        assert!(!legacy.exists());
        assert!(directory.path().join("state.json.migrated-v1").exists());
    }
}
