//! Host-only test for importing and natively resuming a vanilla Claude rollout.
//!
//! It is intentionally ignored: it needs a signed-in `claude`, a container
//! runtime, and the agent-development image. Run it through
//! `scripts/test-import-e2e.sh` on the host.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use hel::hel_acp::RuntimeEvent;
use hel::hel_archive::{PayloadRole, read_archive_verified};
use hel::hel_config::{
    CONFIG_VERSION, ContainerTemplate, HarnessKind, HarnessProfile, HelConfig, ProjectBundle,
    ProjectRepository, TargetTemplate,
};
use hel::hel_controller::Controller;
use hel::hel_state::{HelState, SessionState};
use hel::hel_worker::WorkerEvent;
use hel::hel_worker_client::WorkerClient;

#[test]
#[ignore = "requires a signed-in Claude, Podman, and the Hel agent-development image"]
fn imported_claude_session_resumes_natively() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(imported_claude_session_resumes_natively_async())
        .unwrap();
}

async fn imported_claude_session_resumes_natively_async() -> anyhow::Result<()> {
    let root = required_path("HEL_IMPORT_E2E_ROOT");
    let claude_home = required_path("CLAUDE_CONFIG_DIR");
    let repository = std::env::var("HEL_IMPORT_E2E_REPOSITORY")?;
    let image = std::env::var("HEL_IMPORT_E2E_IMAGE")?;
    let scratch = root.join("scratch");
    if scratch.exists() {
        fs::remove_dir_all(&scratch)?;
    }
    fs::create_dir_all(&root)?;
    run(Command::new("git")
        .args([
            "clone",
            &format!("https://github.com/{repository}.git"),
            "scratch",
        ])
        .current_dir(&root))?;
    run(Command::new("claude")
        .args([
            "-p",
            "append a line to notes.md and remember the word zanzibar",
        ])
        .current_dir(&scratch)
        .env("CLAUDE_CONFIG_DIR", &claude_home))?;

    let mut config = HelConfig {
        version: CONFIG_VERSION,
        profiles: BTreeMap::from([(
            "claude-e2e".into(),
            HarnessProfile {
                kind: HarnessKind::Claude,
                home: claude_home,
                executable: None,
                environment: BTreeMap::new(),
                model: None,
                reasoning_effort: None,
            },
        )]),
        bundles: BTreeMap::new(),
        targets: BTreeMap::from([(
            "podman".into(),
            TargetTemplate::LocalPodman {
                container: ContainerTemplate {
                    image,
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: BTreeMap::new(),
                },
            },
        )]),
    };
    config.bundles.insert(
        "scratch".into(),
        ProjectBundle {
            primary_repo: "scratch".into(),
            repositories: vec![ProjectRepository {
                id: "scratch".into(),
                github: repository,
                destination: "scratch".into(),
                git_ref: None,
            }],
        },
    );
    config.save()?;
    HelState::default().save()?;

    let output = Command::new(env!("CARGO_BIN_EXE_hel"))
        .args(["import", "claude", "--latest", "--bundle", "scratch"])
        .env("CLAUDE_CONFIG_DIR", &config.profiles["claude-e2e"].home)
        .output()?;
    assert!(
        output.status.success(),
        "hel import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let state = HelState::load()?;
    let (session_id, imported) = state.sessions.iter().next().unwrap();
    assert_eq!(imported.state, SessionState::Archived);
    let checkpoint = imported.checkpoint.as_ref().unwrap();
    let archive = read_archive_verified(&checkpoint.archive_path)?;
    assert_eq!(
        archive.manifest.session.native_session_id,
        imported.native_session_id.clone().unwrap()
    );
    assert!(
        !archive
            .payload_by_role(&PayloadRole::CanonicalEvents)?
            .is_empty()
    );

    let mut controller = Controller::load()?;
    controller
        .resume_session(session_id, "claude-e2e", "podman")
        .await?;
    let command = controller.reconnect_command(session_id)?;
    let mut client = WorkerClient::connect(&command, session_id).await?;
    let bootstrap = client.bootstrap().await?;
    let mut events = bootstrap.events;
    for _ in 0..30 {
        if events.iter().any(|event| {
            matches!(
                runtime_event(event),
                Some(RuntimeEvent::SessionStarted { .. })
            )
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        events.extend(client.sync().await?);
    }
    assert!(events.iter().any(|event| {
        matches!(
            runtime_event(event),
            Some(RuntimeEvent::SessionStarted { resumed: true, .. })
        )
    }));
    assert!(
        !events
            .iter()
            .any(|event| { matches!(runtime_event(event), Some(RuntimeEvent::Warning { .. })) })
    );

    client
        .prompt("what word did I ask you to remember?".into(), Vec::new())
        .await?;
    let mut reply = String::new();
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for event in client.sync().await? {
            if let Some(text) = agent_text(&event) {
                reply.push_str(&text);
            }
        }
        if reply.to_ascii_lowercase().contains("zanzibar") {
            break;
        }
    }
    assert!(
        reply.to_ascii_lowercase().contains("zanzibar"),
        "reply: {reply}"
    );
    client.detach().await?;
    controller.close_session(session_id).await?;
    let final_state = HelState::load()?;
    let checkpoint = final_state.sessions[session_id]
        .checkpoint
        .as_ref()
        .unwrap();
    read_archive_verified(&checkpoint.archive_path)?;
    Ok(())
}

#[test]
#[ignore = "requires a signed-in Kimi Code, Podman, and the Hel agent-development image"]
fn imported_kimi_session_resumes_natively() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(imported_kimi_session_resumes_natively_async())
        .unwrap();
}

async fn imported_kimi_session_resumes_natively_async() -> anyhow::Result<()> {
    let kimi_home = required_path("KIMI_CODE_HOME");
    let native_session_id = std::env::var("HEL_IMPORT_E2E_KIMI_SESSION")?;
    let repository = std::env::var("HEL_IMPORT_E2E_KIMI_REPOSITORY")?;
    let image = std::env::var("HEL_IMPORT_E2E_IMAGE")?;
    let config = HelConfig {
        version: CONFIG_VERSION,
        profiles: BTreeMap::from([(
            "kimi-e2e".into(),
            HarnessProfile {
                kind: HarnessKind::Kimi,
                home: kimi_home.clone(),
                executable: None,
                environment: BTreeMap::new(),
                model: None,
                reasoning_effort: None,
            },
        )]),
        bundles: BTreeMap::from([(
            "kimi-source".into(),
            ProjectBundle {
                primary_repo: "kimi-source".into(),
                repositories: vec![ProjectRepository {
                    id: "kimi-source".into(),
                    github: repository,
                    destination: "kimi-source".into(),
                    git_ref: None,
                }],
            },
        )]),
        targets: BTreeMap::from([(
            "podman".into(),
            TargetTemplate::LocalPodman {
                container: ContainerTemplate {
                    image,
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: BTreeMap::new(),
                },
            },
        )]),
    };
    config.save()?;
    HelState::default().save()?;

    let output = Command::new(env!("CARGO_BIN_EXE_hel"))
        .args([
            "import",
            "kimi",
            "--session",
            &native_session_id,
            "--bundle",
            "kimi-source",
        ])
        .env("KIMI_CODE_HOME", &kimi_home)
        .output()?;
    assert!(
        output.status.success(),
        "hel import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let state = HelState::load()?;
    let (session_id, imported) = state.sessions.iter().next().unwrap();
    assert_eq!(imported.state, SessionState::Archived);
    let checkpoint = imported.checkpoint.as_ref().unwrap();
    let archive = read_archive_verified(&checkpoint.archive_path)?;
    assert_eq!(
        archive.manifest.session.native_session_id,
        imported.native_session_id.clone().unwrap()
    );

    let mut controller = Controller::load()?;
    controller
        .resume_session(session_id, "kimi-e2e", "podman")
        .await?;
    let command = controller.reconnect_command(session_id)?;
    let mut client = WorkerClient::connect(&command, session_id).await?;
    let mut events = client.bootstrap().await?.events;
    // The first container invocation downloads Kimi Code's official binary.
    for _ in 0..300 {
        if events.iter().any(|event| {
            matches!(
                runtime_event(event),
                Some(RuntimeEvent::SessionStarted { .. })
            )
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        events.extend(client.sync().await?);
    }
    assert!(events.iter().any(|event| {
        matches!(
            runtime_event(event),
            Some(RuntimeEvent::SessionStarted { resumed: true, .. })
        )
    }));
    assert!(
        !events
            .iter()
            .any(|event| matches!(runtime_event(event), Some(RuntimeEvent::Warning { .. })))
    );

    client
        .prompt("Reply exactly KIMI_NATIVE_RESUME_OK.".into(), Vec::new())
        .await?;
    let mut reply = String::new();
    for _ in 0..120 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for event in client.sync().await? {
            if let Some(text) = agent_text(&event) {
                reply.push_str(&text);
            }
        }
        if reply.contains("KIMI_NATIVE_RESUME_OK") {
            break;
        }
    }
    assert!(reply.contains("KIMI_NATIVE_RESUME_OK"), "reply: {reply}");
    client.detach().await?;
    controller.close_session(session_id).await?;
    let final_state = HelState::load()?;
    read_archive_verified(
        &final_state.sessions[session_id]
            .checkpoint
            .as_ref()
            .unwrap()
            .archive_path,
    )?;
    Ok(())
}

fn runtime_event(event: &hel::hel_worker::SequencedEvent) -> Option<RuntimeEvent> {
    let WorkerEvent::Adapter { payload, .. } = &event.event else {
        return None;
    };
    serde_json::from_value(payload.clone()).ok()
}

fn agent_text(event: &hel::hel_worker::SequencedEvent) -> Option<String> {
    let WorkerEvent::Adapter { payload, .. } = &event.event else {
        return None;
    };
    payload
        .pointer("/update/content/text")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

fn required_path(name: &str) -> PathBuf {
    PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("{name} is required")))
}

fn run(command: &mut Command) -> anyhow::Result<()> {
    let output = command.output()?;
    anyhow::ensure!(
        output.status.success(),
        "{:?} failed: {}",
        command,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
#[ignore = "requires a signed-in Codex, Podman, and the Hel agent-development image"]
fn imported_codex_session_resumes_natively() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(imported_codex_session_resumes_natively_async())
        .unwrap();
}

async fn imported_codex_session_resumes_natively_async() -> anyhow::Result<()> {
    let codex_home = required_path("CODEX_HOME");
    let native_session_id = std::env::var("HEL_IMPORT_E2E_CODEX_SESSION")?;
    let repository = std::env::var("HEL_IMPORT_E2E_CODEX_REPOSITORY")?;
    let image = std::env::var("HEL_IMPORT_E2E_IMAGE")?;
    let config = HelConfig {
        version: CONFIG_VERSION,
        profiles: BTreeMap::from([(
            "codex-e2e".into(),
            HarnessProfile {
                kind: HarnessKind::Codex,
                home: codex_home.clone(),
                executable: None,
                environment: BTreeMap::new(),
                model: None,
                reasoning_effort: None,
            },
        )]),
        bundles: BTreeMap::from([(
            "codex-source".into(),
            ProjectBundle {
                primary_repo: "codex-source".into(),
                repositories: vec![ProjectRepository {
                    id: "codex-source".into(),
                    github: repository,
                    destination: "codex-source".into(),
                    git_ref: None,
                }],
            },
        )]),
        targets: BTreeMap::from([(
            "podman".into(),
            TargetTemplate::LocalPodman {
                container: ContainerTemplate {
                    image,
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: BTreeMap::new(),
                },
            },
        )]),
    };
    config.save()?;
    HelState::default().save()?;

    let output = Command::new(env!("CARGO_BIN_EXE_hel"))
        .args([
            "import",
            "codex",
            "--session",
            &native_session_id,
            "--bundle",
            "codex-source",
        ])
        .env("CODEX_HOME", &codex_home)
        .output()?;
    assert!(
        output.status.success(),
        "hel import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let state = HelState::load()?;
    let (session_id, imported) = state.sessions.iter().next().unwrap();
    assert_eq!(imported.state, SessionState::Archived);
    let checkpoint = imported.checkpoint.as_ref().unwrap();
    let archive = read_archive_verified(&checkpoint.archive_path)?;
    assert_eq!(
        archive.manifest.session.native_session_id,
        imported.native_session_id.clone().unwrap()
    );

    let mut controller = Controller::load()?;
    controller
        .resume_session(session_id, "codex-e2e", "podman")
        .await?;
    let command = controller.reconnect_command(session_id)?;
    let mut client = WorkerClient::connect(&command, session_id).await?;
    let mut events = client.bootstrap().await?.events;
    for _ in 0..90 {
        if events.iter().any(|event| {
            matches!(
                runtime_event(event),
                Some(RuntimeEvent::SessionStarted { .. })
            )
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        events.extend(client.sync().await?);
    }
    assert!(events.iter().any(|event| {
        matches!(
            runtime_event(event),
            Some(RuntimeEvent::SessionStarted { resumed: true, .. })
        )
    }));
    assert!(
        !events
            .iter()
            .any(|event| matches!(runtime_event(event), Some(RuntimeEvent::Warning { .. })))
    );

    client
        .prompt("Reply exactly CODEX_NATIVE_RESUME_OK.".into(), Vec::new())
        .await?;
    let mut reply = String::new();
    for _ in 0..120 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for event in client.sync().await? {
            if let Some(text) = agent_text(&event) {
                reply.push_str(&text);
            }
        }
        if reply.contains("CODEX_NATIVE_RESUME_OK") {
            break;
        }
    }
    assert!(reply.contains("CODEX_NATIVE_RESUME_OK"), "reply: {reply}");
    client.detach().await?;
    controller.close_session(session_id).await?;
    let final_state = HelState::load()?;
    read_archive_verified(
        &final_state.sessions[session_id]
            .checkpoint
            .as_ref()
            .unwrap()
            .archive_path,
    )?;
    Ok(())
}
