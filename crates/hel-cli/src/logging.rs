//! Durable, non-blocking diagnostics for controller-facing Hel processes.

use std::fs::{self, OpenOptions};
use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

const RETAINED_LOGS: usize = 10;

pub(crate) struct ControllerLog {
    _writer_guard: WorkerGuard,
}

impl ControllerLog {
    pub(crate) fn start(command: &'static str) -> Result<Self> {
        let directory = hel::hel_config::data_dir().join("logs");
        fs::create_dir_all(&directory)
            .with_context(|| format!("create Hel log directory {}", directory.display()))?;
        prune_logs(&directory, RETAINED_LOGS.saturating_sub(1))?;

        let path = directory.join(log_filename());
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .with_context(|| format!("create Hel log {}", path.display()))?;
        let (writer, writer_guard) = tracing_appender::non_blocking(file);
        let (filter, filter_error) = env_filter("info");
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_env_filter(filter)
            .with_writer(writer)
            .try_init()
            .map_err(|error| anyhow::anyhow!("install Hel log subscriber: {error}"))?;

        tracing::info!(
            version = env!("CARGO_PKG_VERSION"),
            process_id = std::process::id(),
            command,
            log = %path.display(),
            "Hel started"
        );
        if let Some(error) = filter_error {
            tracing::warn!(%error, "ignored invalid RUST_LOG filter");
        }
        Ok(Self {
            _writer_guard: writer_guard,
        })
    }
}

pub(crate) fn start_stderr() -> Result<()> {
    let (filter, filter_error) = env_filter("warn");
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("install Hel stderr subscriber: {error}"))?;
    if let Some(error) = filter_error {
        tracing::warn!(%error, "ignored invalid RUST_LOG filter");
    }
    Ok(())
}

fn env_filter(default: &str) -> (EnvFilter, Option<String>) {
    match std::env::var("RUST_LOG") {
        Ok(value) => match EnvFilter::try_new(value) {
            Ok(filter) => (filter, None),
            Err(error) => (EnvFilter::new(default), Some(error.to_string())),
        },
        Err(std::env::VarError::NotPresent) => (EnvFilter::new(default), None),
        Err(error @ std::env::VarError::NotUnicode(_)) => {
            (EnvFilter::new(default), Some(error.to_string()))
        }
    }
}

fn log_filename() -> String {
    format!(
        "hel-{}-{}.log",
        Utc::now().format("%Y%m%dT%H%M%S%.3fZ"),
        std::process::id()
    )
}

fn prune_logs(directory: &Path, retain: usize) -> Result<()> {
    let mut logs = Vec::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read Hel log directory {}", directory.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", directory.display()))?;
        if let Some(path) = {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            (name.starts_with("hel-") && name.ends_with(".log")).then_some(entry.path())
        } {
            logs.push(path);
        }
    }
    logs.sort_unstable();
    let remove = logs.len().saturating_sub(retain);
    for path in logs.into_iter().take(remove) {
        fs::remove_file(&path)
            .with_context(|| format!("remove expired Hel log {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prune_logs_keeps_newest_managed_logs_and_unrelated_files() {
        let directory = tempfile::tempdir().unwrap();
        for name in [
            "hel-20260824T000000.000Z-1.log",
            "hel-20260825T000000.000Z-2.log",
            "hel-20260826T000000.000Z-3.log",
            "notes.log",
        ] {
            fs::write(directory.path().join(name), name).unwrap();
        }

        prune_logs(directory.path(), 2).unwrap();

        assert!(
            !directory
                .path()
                .join("hel-20260824T000000.000Z-1.log")
                .exists()
        );
        assert!(
            directory
                .path()
                .join("hel-20260825T000000.000Z-2.log")
                .exists()
        );
        assert!(
            directory
                .path()
                .join("hel-20260826T000000.000Z-3.log")
                .exists()
        );
        assert!(directory.path().join("notes.log").exists());
    }
}
