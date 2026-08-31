use std::fs;
use std::process::Command;

#[test]
fn top_level_failure_is_written_to_a_private_per_run_log() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let config = root.path().join("config");
    let mut command = Command::new(env!("CARGO_BIN_EXE_hel"));
    command
        .args(["checkpoint", "--session", "definitely-missing"])
        .env("HEL_DATA_DIR", &data)
        .env("HEL_CONFIG_DIR", config);

    let output = hel::hel_subprocess::run_with_input(&mut command, &[]).unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown session definitely-missing"));
    let logs = fs::read_dir(data.join("logs"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(logs.len(), 1);
    let contents = fs::read_to_string(&logs[0]).unwrap();
    assert!(contents.contains("Hel started"));
    assert!(contents.contains("command=\"checkpoint\""));
    assert!(contents.contains("Hel exited with an error"));
    assert!(contents.contains("unknown session definitely-missing"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&logs[0]).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
