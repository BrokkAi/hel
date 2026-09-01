//! The incident from issue #24, end to end: another process migrates the
//! daemon's store while the daemon is live.
//!
//! Before the fix the daemon stayed up indefinitely -- refusing every read,
//! writing through a connection whose schema check had passed once, and
//! warning twice a second. It must now notice and leave.

use std::{
    fs,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const METADATA_WAIT: Duration = Duration::from_secs(8);
const EXIT_WAIT: Duration = Duration::from_secs(10);

/// A daemon under test is a real process. Kill it however the test ends.
struct ReapChild(Option<Child>);

impl ReapChild {
    fn child_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("child already reaped")
    }
}

impl Drop for ReapChild {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut()
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn daemon_exits_when_its_store_is_migrated_underneath_it() {
    let storage = tempfile::tempdir().expect("create Mjolnir test storage");
    let config_directory = storage.path().join("config/mjolnir");
    let data_directory = storage.path().join("data/mjolnir");
    fs::create_dir_all(&config_directory).expect("create Mjolnir config directory");
    fs::write(
        config_directory.join("config.toml"),
        r#"version = 1

[phone]
enabled = false

[profiles.codex]
kind = "codex"
home = "/profiles/codex"
# Keep this test independent from any Codex installation on the host.
environment = { PATH = "/mjolnir-store-divergence-test-no-executables" }

[targets.podman]
kind = "local-podman"
image = "ubuntu:24.04"
"#,
    )
    .expect("write Mjolnir test config");

    // No MJ_DAEMON_EXIT_WHEN_IDLE: an idle exit would end this process for a
    // reason that has nothing to do with the store.
    let mut daemon = ReapChild(Some(
        Command::new(env!("CARGO_BIN_EXE_mj"))
            .arg("daemon-run")
            .env("MJ_CONFIG_DIR", &config_directory)
            .env("MJ_DATA_DIR", &data_directory)
            .env_remove("MJ_DAEMON_EXIT_WHEN_IDLE")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the Mjolnir daemon"),
    ));

    // The daemon writes its metadata before it listens, so this is the point
    // where it is fully up and its writer connection has verified the schema.
    let metadata = data_directory.join("daemon.json");
    let deadline = Instant::now() + METADATA_WAIT;
    while Instant::now() < deadline && !metadata.exists() {
        assert!(
            daemon
                .child_mut()
                .try_wait()
                .expect("poll the daemon")
                .is_none(),
            "the daemon exited before it was ready"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(metadata.exists(), "the daemon never became ready");

    // What another Mjolnir build's migration ladder does to a store this daemon
    // has open.
    let store = data_directory.join("mj.sqlite3");
    let connection = rusqlite::Connection::open(&store).expect("open the daemon's store");
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read the store's schema version");
    connection
        .execute_batch(&format!("PRAGMA user_version = {};", version + 1))
        .expect("migrate the store underneath the daemon");
    drop(connection);

    let deadline = Instant::now() + EXIT_WAIT;
    let status = loop {
        if let Some(status) = daemon.child_mut().try_wait().expect("poll the daemon") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the daemon is still running {EXIT_WAIT:?} after its store moved underneath it"
        );
        std::thread::sleep(Duration::from_millis(50));
    };

    assert!(
        status.success() || status.code() == Some(1),
        "the daemon left abnormally: {status:?}"
    );
    assert!(
        !metadata.exists(),
        "the daemon left its metadata behind, so clients keep dialing a dead address"
    );
}
