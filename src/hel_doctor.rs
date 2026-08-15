//! Actionable host and configuration prerequisite checks.

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::Serialize;

use crate::hel_config::{ContainerTemplate, HarnessKind, HelConfig, TargetTemplate, config_path};
use crate::hel_controller::{
    WorkerBinaryAvailability, backend_ssh, worker_binary_prerequisite_for_arch,
};
use crate::hel_setup::{
    DiscoveredHome, discover_harness_homes, harness_authentication_marker, harness_is_authenticated,
};
use crate::hel_targets::{
    CommandExecutor, CommandSpec, ContainerTemplate as RuntimeContainerTemplate, ProcessExecutor,
    SshTarget as RuntimeSshTarget, TargetTemplate as RuntimeTargetTemplate, run_setup_smoke_test,
    verify_local_podman, verify_ssh_podman,
};

const DEFAULT_CONTAINER_IMAGE: &str = "ubuntu:24.04";
const APPLE_CONTAINER_INSTALL_URL: &str = "https://github.com/apple/container#initial-install";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Ready,
    Fixable,
    Unsupported,
}

impl CheckStatus {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Fixable => "fixable",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DoctorCheck {
    pub id: String,
    pub title: String,
    pub status: CheckStatus,
    pub detail: String,
    pub remediation: Option<String>,
}

impl DoctorCheck {
    fn ready(id: impl Into<String>, title: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            status: CheckStatus::Ready,
            detail: detail.into(),
            remediation: None,
        }
    }

    fn fixable(
        id: impl Into<String>,
        title: impl Into<String>,
        detail: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            status: CheckStatus::Fixable,
            detail: detail.into(),
            remediation: Some(remediation.into()),
        }
    }

    fn unsupported(
        id: impl Into<String>,
        title: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            status: CheckStatus::Unsupported,
            detail: detail.into(),
            remediation: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DoctorOptions {
    pub smoke: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplePlatform {
    Linux,
    Macos {
        architecture: String,
        major_version: u32,
    },
    Other(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstructionsPlatform {
    Linux,
    Macos,
}

pub fn run_current(options: DoctorOptions) -> Vec<DoctorCheck> {
    run_with(
        &ProcessExecutor,
        current_apple_platform(&ProcessExecutor),
        options,
    )
}

pub fn run_with(
    executor: &impl CommandExecutor,
    apple_platform: ApplePlatform,
    options: DoctorOptions,
) -> Vec<DoctorCheck> {
    let (config, mut checks) = configuration_checks();
    checks.push(harness_discovery_check(config.as_ref()));
    checks.extend(harness_checks(config.as_ref()));
    checks.extend(podman_checks(config.as_ref(), executor, options.smoke));
    checks.extend(ssh_podman_checks(config.as_ref(), executor, options.smoke));
    checks.extend(worker_binary_checks(config.as_ref()));
    checks.push(apple_container_check(
        &apple_platform,
        executor,
        options.smoke,
        apple_container_image(config.as_ref()),
    ));
    checks
}

fn harness_discovery_check(config: Option<&HelConfig>) -> DoctorCheck {
    let home = dirs::home_dir();
    let overrides = HarnessKind::ALL
        .into_iter()
        .filter_map(|kind| std::env::var_os(kind.home_env()).map(|path| (kind, path.into())));
    let discovered = discover_harness_homes(home.as_deref(), overrides);
    harness_discovery_check_from(
        &discovered,
        config.is_some_and(|config| !config.profiles.is_empty()),
    )
}

fn harness_discovery_check_from(
    discovered: &[DiscoveredHome],
    has_configured_profiles: bool,
) -> DoctorCheck {
    if discovered.is_empty() {
        return if has_configured_profiles {
            DoctorCheck::ready(
                "harness.discovery",
                "Harness home discovery",
                "No default or environment-overridden harness homes were found; configured profile homes are checked below.",
            )
        } else {
            DoctorCheck::fixable(
                "harness.discovery",
                "Harness home discovery",
                "No Codex, Claude Code, or Kimi Code home was found in the default or environment-overridden locations.",
                "Install and sign in to a supported harness, then run `hel setup`.",
            )
        };
    }

    let homes = discovered
        .iter()
        .map(|home| {
            let authentication = if home.authenticated {
                "authenticated"
            } else {
                "authentication marker missing"
            };
            format!(
                "{} at {} ({authentication})",
                harness_label(home.kind),
                home.path.display()
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    DoctorCheck::ready(
        "harness.discovery",
        "Harness home discovery",
        format!("Discovered {homes}. Configured profile authentication is checked below."),
    )
}

pub fn all_ready(checks: &[DoctorCheck]) -> bool {
    checks
        .iter()
        .all(|check| check.status != CheckStatus::Fixable)
}

pub fn render_human(checks: &[DoctorCheck], output: &mut impl Write) -> Result<()> {
    for check in checks {
        writeln!(
            output,
            "{} {}: {}",
            check.status.label(),
            check.title,
            check.detail
        )?;
        if let Some(remediation) = &check.remediation {
            writeln!(output, "  remediation: {remediation}")?;
        }
    }
    Ok(())
}

pub fn setup_instructions(platform: InstructionsPlatform) -> String {
    match platform {
        InstructionsPlatform::Linux => format!(
            "# Hel setup instructions for Linux\n\n\
This page is self-contained. Follow this exact loop as the user who will run Hel:\n\n\
1. Run `hel doctor --json`.\n\
2. Follow every `fixable` remediation from its JSON output.\n\
3. Run `hel doctor --json` again. Repeat until no check is `fixable`.\n\
4. Finish with `hel doctor --json --smoke` to verify every configured container\n\
   image end to end, and resolve anything it reports as `fixable`.\n\n\
For a coding-agent handoff, provide this entire instructions page together with\n\
the latest `hel doctor --json` output.\n\n\
## Linux Podman postconditions\n\n{}",
            include_str!("../docs/PODMAN.md")
        ),
        InstructionsPlatform::Macos => format!(
            "# Hel setup instructions for macOS\n\n\
This page is self-contained. Follow this exact loop as the user who will run Hel:\n\n\
1. Run `hel doctor --json`.\n\
2. Follow every `fixable` remediation from its JSON output.\n\
3. Run `hel doctor --json` again. Repeat until no check is `fixable`.\n\n\
For a coding-agent handoff, provide this entire instructions page together with\n\
the latest `hel doctor --json` output.\n\n\
## Apple container runtime\n\n\
Hel's Apple container target requires Apple silicon and macOS 26 or newer.\n\
On an Intel Mac or an older macOS release, the target is unsupported; use a\n\
local Podman, SSH, or AWS target instead.\n\n\
If the `container` command is absent, install only the official signed package:\n\n\
<https://github.com/apple/container#initial-install>\n\n\
Hel never downloads or installs that package. If doctor reports a stopped\n\
daemon, run exactly:\n\n```console\ncontainer system start\n```\n\n\
Finish with the opt-in disposable runtime test in JSON mode:\n\n```console\nhel doctor --json --smoke\n```\n\n\
Apple container is ready only when that smoke test creates a disposable\n\
container, executes `true` in it, and removes it successfully. Use the image\n\
configured by an `apple-container` target; without one, doctor uses\n\
`{DEFAULT_CONTAINER_IMAGE}` for the smoke test.\n\n\
## Shared Hel prerequisites\n\n\
`hel doctor --json` also checks the configuration, each configured harness home\n\
and authentication marker, selected container worker binaries, and any relevant\n\
Podman prerequisites. Resolve every `fixable` status before starting a session."
        ),
    }
}

fn configuration_checks() -> (Option<HelConfig>, Vec<DoctorCheck>) {
    let path = config_path();
    if !path.exists() {
        return (
            None,
            vec![DoctorCheck::fixable(
                "config",
                "Hel configuration",
                format!("{} does not exist", path.display()),
                "Run `hel setup` to create config.toml.",
            )],
        );
    }
    match HelConfig::load_from(&path) {
        Ok(config) => {
            let mut checks = vec![DoctorCheck::ready(
                "config",
                "Hel configuration",
                format!("{} is valid", path.display()),
            )];
            if config.profiles.is_empty() || config.bundles.is_empty() || config.targets.is_empty()
            {
                checks.push(DoctorCheck::fixable(
                    "config.session-prerequisites",
                    "Session configuration",
                    "At least one profile, bundle, and target are required.",
                    "Run `hel setup`, or add profiles, bundles, and targets to config.toml.",
                ));
            } else {
                checks.push(DoctorCheck::ready(
                    "config.session-prerequisites",
                    "Session configuration",
                    "At least one profile, bundle, and target are configured.",
                ));
            }
            (Some(config), checks)
        }
        Err(error) => (
            None,
            vec![DoctorCheck::fixable(
                "config",
                "Hel configuration",
                format!("{} is invalid: {error:#}", path.display()),
                "Fix the reported TOML error in config.toml, or run `hel setup` to replace it.",
            )],
        ),
    }
}

fn harness_checks(config: Option<&HelConfig>) -> Vec<DoctorCheck> {
    let Some(config) = config else {
        return vec![DoctorCheck::fixable(
            "harness.profiles",
            "Harness profiles",
            "Harness homes cannot be checked until config.toml is valid.",
            "Fix config.toml, then rerun `hel doctor --json`.",
        )];
    };
    if config.profiles.is_empty() {
        return vec![DoctorCheck::fixable(
            "harness.profiles",
            "Harness profiles",
            "No harness profiles are configured.",
            "Run `hel setup` to discover homes, or add a profile to config.toml.",
        )];
    }
    config
        .profiles
        .iter()
        .map(|(id, profile)| {
            let marker = harness_authentication_marker(profile.kind, &profile.home);
            let title = format!("Harness profile {id}");
            if !profile.home.is_dir() {
                return DoctorCheck::fixable(
                    format!("harness.{id}"),
                    title,
                    format!("{} does not exist", profile.home.display()),
                    format!(
                        "Create or select the {} home, then set its `home` path in config.toml.",
                        harness_label(profile.kind)
                    ),
                );
            }
            if !harness_is_authenticated(profile.kind, &profile.home) {
                return DoctorCheck::fixable(
                    format!("harness.{id}"),
                    title,
                    format!("Authentication marker {} is missing", marker.display()),
                    harness_login_remediation(profile.kind, &profile.home),
                );
            }
            DoctorCheck::ready(
                format!("harness.{id}"),
                title,
                format!(
                    "{} is present and {} exists",
                    profile.home.display(),
                    marker.display()
                ),
            )
        })
        .collect()
}

fn harness_label(kind: HarnessKind) -> &'static str {
    match kind {
        HarnessKind::Codex => "Codex",
        HarnessKind::Claude => "Claude Code",
        HarnessKind::Kimi => "Kimi Code",
    }
}

fn harness_login_remediation(kind: HarnessKind, home: &std::path::Path) -> String {
    let environment = kind.home_env();
    match kind {
        HarnessKind::Codex => {
            format!("Run `{environment}={} codex login`.", home.display())
        }
        HarnessKind::Claude => format!(
            "Run `{environment}={} claude` and complete the sign-in prompt.",
            home.display()
        ),
        HarnessKind::Kimi => format!(
            "Run `{environment}={} kimi` and complete the sign-in prompt.",
            home.display()
        ),
    }
}

/// Host Podman prerequisites, then one image check per `local-podman` target.
///
/// The image checks run only after the host preflight passes, because a broken
/// Podman installation already reports its own actionable check.
fn podman_checks(
    config: Option<&HelConfig>,
    executor: &impl CommandExecutor,
    smoke: bool,
) -> Vec<DoctorCheck> {
    let preflight = podman_check(config, executor);
    let preflight_passed = preflight.status == CheckStatus::Ready;
    let mut checks = vec![preflight];
    if preflight_passed {
        checks.extend(podman_image_checks(config, executor, smoke));
    }
    checks
}

fn podman_check(config: Option<&HelConfig>, executor: &impl CommandExecutor) -> DoctorCheck {
    let Some(config) = config else {
        return DoctorCheck::unsupported(
            "runtime.podman",
            "Rootless Podman",
            "Podman prerequisites cannot be evaluated until config.toml is valid.",
        );
    };
    if local_podman_targets(config).is_empty() {
        return DoctorCheck::unsupported(
            "runtime.podman",
            "Rootless Podman",
            "No local-podman target is configured.",
        );
    }
    match verify_local_podman(executor) {
        Ok(preflight) => DoctorCheck::ready(
            "runtime.podman",
            "Rootless Podman",
            format!("Podman {} has a valid rootless UID map.", preflight.version),
        ),
        Err(error) => {
            let detail = format!("{error:#}");
            DoctorCheck::fixable(
                "runtime.podman",
                "Rootless Podman",
                detail.clone(),
                podman_remediation(&detail),
            )
        }
    }
}

fn local_podman_targets(config: &HelConfig) -> Vec<(&String, &ContainerTemplate)> {
    config
        .targets
        .iter()
        .filter_map(|(id, target)| match target {
            TargetTemplate::LocalPodman { container } => Some((id, container)),
            _ => None,
        })
        .collect()
}

fn podman_image_checks(
    config: Option<&HelConfig>,
    executor: &impl CommandExecutor,
    smoke: bool,
) -> Vec<DoctorCheck> {
    let Some(config) = config else {
        return Vec::new();
    };
    local_podman_targets(config)
        .into_iter()
        .map(|(id, container)| podman_image_check(id, &container.image, executor, smoke))
        .collect()
}

fn podman_image_check(
    id: &str,
    image: &str,
    executor: &impl CommandExecutor,
    smoke: bool,
) -> DoctorCheck {
    let check_id = format!("runtime.podman.image.{id}");
    let title = format!("Podman image for target {id}");
    if smoke {
        let target = RuntimeTargetTemplate::LocalPodman(RuntimeContainerTemplate {
            image: image.to_owned(),
            extra_run_args: vec![],
        });
        return match run_setup_smoke_test(&target, &doctor_smoke_id(), executor) {
            Ok(()) => DoctorCheck::ready(
                check_id,
                title,
                format!("Disposable run/exec/remove smoke test passed for image {image}."),
            ),
            Err(error) => DoctorCheck::fixable(
                check_id,
                title,
                format!(
                    "Disposable run/exec/remove smoke test failed for image {image}: {error:#}"
                ),
                "Fix the configured image or Podman runtime, then run `hel doctor --json --smoke` again.",
            ),
        };
    }

    let command = CommandSpec::new("podman", ["image", "exists", image])
        .purpose("check Podman image presence");
    match executor.execute(&command) {
        Ok(output) if output.status == 0 => DoctorCheck::ready(
            check_id,
            title,
            format!("Image {image} is present in local Podman storage."),
        ),
        Ok(_) => DoctorCheck::fixable(
            check_id,
            title,
            format!("Image {image} is not present in local Podman storage."),
            missing_image_remediation(image),
        ),
        Err(error) => DoctorCheck::fixable(
            check_id,
            title,
            format!(
                "Could not check whether image {image} is present in local Podman storage: {error}"
            ),
            missing_image_remediation(image),
        ),
    }
}

fn missing_image_remediation(image: &str) -> String {
    format!(
        "Pull it with `podman pull {image}`, build it from containers/Containerfile.agent-dev, or run `hel doctor --json --smoke` to verify the full pull-and-run path."
    )
}

/// One check per `ssh-podman` target: the same Podman probes, run over SSH.
fn ssh_podman_checks(
    config: Option<&HelConfig>,
    executor: &impl CommandExecutor,
    smoke: bool,
) -> Vec<DoctorCheck> {
    let Some(config) = config else {
        return Vec::new();
    };
    config
        .targets
        .iter()
        .filter_map(|(id, target)| match target {
            TargetTemplate::SshPodman { ssh, container } => Some(ssh_podman_check(
                id,
                &backend_ssh(ssh),
                &container.image,
                executor,
                smoke,
            )),
            _ => None,
        })
        .collect()
}

fn ssh_podman_check(
    id: &str,
    ssh: &RuntimeSshTarget,
    image: &str,
    executor: &impl CommandExecutor,
    smoke: bool,
) -> DoctorCheck {
    let check_id = format!("runtime.ssh-podman.{id}");
    let title = format!("Remote Podman for target {id}");
    let destination = &ssh.destination;
    let preflight = match verify_ssh_podman(ssh, executor) {
        Ok(preflight) => preflight,
        Err(error) => {
            let detail = format!("{error:#}");
            let remediation = match podman_remediation_match(&detail) {
                Some(remediation) => format!("On {destination}: {remediation}"),
                None => format!(
                    "Verify `ssh {destination}` succeeds noninteractively from this host, then install rootless Podman 4 or newer there (see docs/PODMAN.md)."
                ),
            };
            return DoctorCheck::fixable(check_id, title, detail, remediation);
        }
    };
    if !smoke {
        return DoctorCheck::ready(
            check_id,
            title,
            format!(
                "Remote rootless Podman {} is available via {destination}. Run `hel doctor --json --smoke` to verify the image end to end.",
                preflight.version
            ),
        );
    }

    let target = RuntimeTargetTemplate::SshPodman {
        ssh: ssh.clone(),
        container: RuntimeContainerTemplate {
            image: image.to_owned(),
            extra_run_args: vec![],
        },
    };
    match run_setup_smoke_test(&target, &doctor_smoke_id(), executor) {
        Ok(()) => DoctorCheck::ready(
            check_id,
            title,
            format!(
                "Disposable run/exec/remove smoke test passed for image {image} on {destination}."
            ),
        ),
        Err(error) => DoctorCheck::fixable(
            check_id,
            title,
            format!(
                "Disposable run/exec/remove smoke test failed for image {image} on {destination}: {error:#}"
            ),
            format!(
                "Fix the configured image or Podman runtime on {destination}, then run `hel doctor --json --smoke` again."
            ),
        ),
    }
}

/// Shared disposable-container identity for every doctor smoke test.
fn doctor_smoke_id() -> String {
    format!(
        "doctor-{}-{:x}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}

fn podman_remediation(detail: &str) -> &'static str {
    podman_remediation_match(detail).unwrap_or(
        "Install Podman with `sudo apt update && sudo apt install -y podman uidmap` (Debian/Ubuntu) or `sudo dnf install -y podman shadow-utils` (Fedora).",
    )
}

/// Map a Podman preflight failure to its specific remediation, if one applies.
fn podman_remediation_match(detail: &str) -> Option<&'static str> {
    if detail.contains("Podman 4.0.0") {
        Some(
            "Upgrade Podman: Debian/Ubuntu `sudo apt update && sudo apt install -y podman uidmap`; Fedora `sudo dnf install -y podman shadow-utils`.",
        )
    } else if detail.contains("podman unshare") {
        Some(
            "Install UID mapping support with `sudo apt install -y uidmap` (Debian/Ubuntu) or `sudo dnf install -y shadow-utils` (Fedora), add `/etc/subuid` and `/etc/subgid` entries, then log out and back in.",
        )
    } else if detail.contains("Rootless") {
        Some(
            "Run Hel without `sudo`; unset `CONTAINER_HOST` and select the rootless local Podman connection.",
        )
    } else {
        None
    }
}

fn worker_binary_checks(config: Option<&HelConfig>) -> Vec<DoctorCheck> {
    let Some(config) = config else {
        return vec![DoctorCheck::fixable(
            "worker.containers",
            "Container worker binary",
            "Worker availability cannot be checked until config.toml is valid.",
            "Fix config.toml, then rerun `hel doctor --json`.",
        )];
    };
    let containers = config
        .targets
        .iter()
        .filter_map(|(id, target)| match target {
            TargetTemplate::LocalPodman { container }
            | TargetTemplate::AppleContainer { container } => Some((id, container, false)),
            TargetTemplate::SshPodman { container, .. } => Some((id, container, true)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if containers.is_empty() {
        return vec![DoctorCheck::unsupported(
            "worker.containers",
            "Container worker binary",
            "No container target is configured.",
        )];
    }
    containers
        .into_iter()
        .map(|(id, container, remote)| {
            if remote && container.platform.is_none() {
                // The remote CPU architecture is only observable once the host
                // is reachable, so an explicit `platform` is required here.
                return DoctorCheck::unsupported(
                    format!("worker.{id}"),
                    format!("Container worker binary for target {id}"),
                    "Set `platform` on this ssh-podman target to check its worker binary; the remote architecture is unknown until provisioning.",
                );
            }
            worker_binary_check(id, container)
        })
        .collect()
}

fn worker_binary_check(id: &str, container: &ContainerTemplate) -> DoctorCheck {
    let title = format!("Container worker binary for target {id}");
    let arch = match container_architecture(container.platform.as_deref()) {
        Ok(arch) => arch,
        Err(reason) => {
            return DoctorCheck::unsupported(format!("worker.{id}"), title, reason);
        }
    };
    let triple = format!("{arch}-unknown-linux-musl");
    match worker_binary_prerequisite_for_arch(arch) {
        Ok(WorkerBinaryAvailability::Local { path, source }) => DoctorCheck::ready(
            format!("worker.{id}"),
            title,
            format!(
                "{triple} worker is available from {source}: {}",
                path.display()
            ),
        ),
        Ok(WorkerBinaryAvailability::Remote { url, .. }) => DoctorCheck::ready(
            format!("worker.{id}"),
            title,
            format!("{triple} worker will be verified and downloaded from {url} when needed."),
        ),
        Err(error) => DoctorCheck::fixable(
            format!("worker.{id}"),
            title,
            format!("No usable {triple} worker source: {error:#}"),
            format!(
                "Build it with `cargo build --release --target {triple}`, install `hel-worker-{triple}` beside `hel`, or set HEL_WORKER_BINARY, HEL_WORKER_DIR, or HEL_WORKER_URL with HEL_WORKER_SHA256."
            ),
        ),
    }
}

fn container_architecture(platform: Option<&str>) -> std::result::Result<&'static str, String> {
    let candidate = platform.unwrap_or(std::env::consts::ARCH);
    let candidate = candidate
        .split('/')
        .rev()
        .find(|part| matches!(*part, "x86_64" | "amd64" | "aarch64" | "arm64"))
        .unwrap_or(candidate);
    match candidate {
        "x86_64" | "amd64" => Ok("x86_64"),
        "aarch64" | "arm64" => Ok("aarch64"),
        other => Err(format!(
            "Container architecture {other:?} is unsupported; Hel supports x86_64 and aarch64 Linux workers."
        )),
    }
}

fn apple_container_image(config: Option<&HelConfig>) -> String {
    config
        .and_then(|config| {
            config.targets.values().find_map(|target| match target {
                TargetTemplate::AppleContainer { container } => Some(container.image.clone()),
                _ => None,
            })
        })
        .unwrap_or_else(|| DEFAULT_CONTAINER_IMAGE.into())
}

pub fn apple_container_check(
    platform: &ApplePlatform,
    executor: &impl CommandExecutor,
    smoke: bool,
    image: String,
) -> DoctorCheck {
    match platform {
        ApplePlatform::Linux => {
            return DoctorCheck::unsupported(
                "runtime.apple-container",
                "Apple container runtime",
                "macOS only",
            );
        }
        ApplePlatform::Other(current) => {
            return DoctorCheck::unsupported(
                "runtime.apple-container",
                "Apple container runtime",
                format!("macOS only (current platform: {current})"),
            );
        }
        ApplePlatform::Macos {
            architecture,
            major_version,
        } if architecture != "aarch64" && architecture != "arm64" => {
            return DoctorCheck::unsupported(
                "runtime.apple-container",
                "Apple container runtime",
                "Apple container requires Apple silicon; Intel Macs are unsupported.",
            );
        }
        ApplePlatform::Macos { major_version, .. } if *major_version < 26 => {
            return DoctorCheck::unsupported(
                "runtime.apple-container",
                "Apple container runtime",
                format!("Apple container requires macOS 26 or newer (found {major_version})."),
            );
        }
        ApplePlatform::Macos { .. } => {}
    }

    let installed =
        CommandSpec::new("container", ["--version"]).purpose("check Apple container installation");
    match executor.execute(&installed) {
        Err(error) => {
            return DoctorCheck::fixable(
                "runtime.apple-container",
                "Apple container runtime",
                format!("The `container` command is not available: {error}"),
                format!("Install the official signed package: {APPLE_CONTAINER_INSTALL_URL}"),
            );
        }
        Ok(output) if output.status != 0 => {
            return DoctorCheck::fixable(
                "runtime.apple-container",
                "Apple container runtime",
                format!(
                    "The installed `container --version` command failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                format!("Reinstall the official signed package: {APPLE_CONTAINER_INSTALL_URL}"),
            );
        }
        Ok(_) => {}
    }

    let status =
        CommandSpec::new("container", ["system", "status"]).purpose("check Apple container daemon");
    match executor.execute(&status) {
        Ok(output) if output.status == 0 => {}
        Ok(output) => {
            return DoctorCheck::fixable(
                "runtime.apple-container",
                "Apple container runtime",
                format!(
                    "The Apple container daemon is stopped: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
                "Run `container system start`.",
            );
        }
        Err(error) => {
            return DoctorCheck::fixable(
                "runtime.apple-container",
                "Apple container runtime",
                format!("Could not query the Apple container daemon: {error}"),
                "Run `container system start`.",
            );
        }
    }

    if !smoke {
        return DoctorCheck::fixable(
            "runtime.apple-container",
            "Apple container runtime",
            "The daemon is running, but the required disposable smoke test was not requested.",
            "Run `hel doctor --json --smoke`.",
        );
    }

    let target = RuntimeTargetTemplate::AppleContainer(RuntimeContainerTemplate {
        image,
        extra_run_args: vec![],
    });
    match run_setup_smoke_test(&target, &doctor_smoke_id(), executor) {
        Ok(()) => DoctorCheck::ready(
            "runtime.apple-container",
            "Apple container runtime",
            "Installed, daemon running, and disposable run/exec/remove smoke test passed.",
        ),
        Err(error) => DoctorCheck::fixable(
            "runtime.apple-container",
            "Apple container runtime",
            format!("Disposable run/exec/remove smoke test failed: {error:#}"),
            "Fix the configured image or container runtime, then run `hel doctor --json --smoke` again.",
        ),
    }
}

fn current_apple_platform(executor: &impl CommandExecutor) -> ApplePlatform {
    if cfg!(target_os = "linux") {
        return ApplePlatform::Linux;
    }
    if !cfg!(target_os = "macos") {
        return ApplePlatform::Other(std::env::consts::OS.into());
    }
    let major_version = executor
        .execute(&CommandSpec::new("sw_vers", ["-productVersion"]).purpose("detect macOS version"))
        .ok()
        .filter(|output| output.status == 0)
        .and_then(|output| {
            String::from_utf8(output.stdout)
                .ok()
                .and_then(|value| value.trim().split('.').next()?.parse().ok())
        })
        .unwrap_or(0);
    ApplePlatform::Macos {
        architecture: std::env::consts::ARCH.into(),
        major_version,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use anyhow::anyhow;

    use super::*;
    use crate::hel_targets::CommandOutput;

    struct FakeExecutor {
        commands: RefCell<Vec<CommandSpec>>,
        responses: RefCell<Vec<Result<CommandOutput>>>,
    }

    impl FakeExecutor {
        fn new(responses: impl IntoIterator<Item = Result<CommandOutput>>) -> Self {
            Self {
                commands: RefCell::new(vec![]),
                responses: RefCell::new(responses.into_iter().collect()),
            }
        }
    }

    impl CommandExecutor for FakeExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            self.commands.borrow_mut().push(command.clone());
            self.responses.borrow_mut().remove(0)
        }
    }

    fn output(stdout: impl AsRef<[u8]>) -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: stdout.as_ref().to_vec(),
            stderr: vec![],
        }
    }

    fn failed(stderr: impl AsRef<[u8]>) -> CommandOutput {
        CommandOutput {
            status: 1,
            stdout: vec![],
            stderr: stderr.as_ref().to_vec(),
        }
    }

    /// The three host probes `verify_local_podman`/`verify_ssh_podman` run.
    fn passing_podman_probes() -> Vec<Result<CommandOutput>> {
        vec![
            Ok(output(b"podman version 5.4.2\n")),
            Ok(output(b"true\n")),
            Ok(output(
                b"         0       1000          1\n         1     100000      65536\n",
            )),
        ]
    }

    fn container(image: &str) -> ContainerTemplate {
        ContainerTemplate {
            image: image.to_owned(),
            platform: None,
            cpus: None,
            memory: None,
            environment: std::collections::BTreeMap::new(),
        }
    }

    fn ssh_connection() -> crate::hel_config::SshConnection {
        crate::hel_config::SshConnection {
            host: "example.test".into(),
            user: Some("dev".into()),
            identity_file: None,
            extra_args: vec![],
        }
    }

    fn config_with(targets: impl IntoIterator<Item = (&'static str, TargetTemplate)>) -> HelConfig {
        HelConfig {
            targets: targets
                .into_iter()
                .map(|(id, target)| (id.to_owned(), target))
                .collect(),
            ..HelConfig::default()
        }
    }

    fn runtime_ssh() -> RuntimeSshTarget {
        backend_ssh(&ssh_connection())
    }

    #[test]
    fn podman_check_is_unsupported_without_a_valid_config() {
        let executor = FakeExecutor::new([]);

        let check = podman_check(None, &executor);

        assert_eq!(check.status, CheckStatus::Unsupported);
        assert_eq!(
            check.detail,
            "Podman prerequisites cannot be evaluated until config.toml is valid."
        );
        assert!(executor.commands.borrow().is_empty());
    }

    #[test]
    fn podman_check_is_unsupported_without_a_local_podman_target() {
        let executor = FakeExecutor::new([]);
        let config = config_with([(
            "apple",
            TargetTemplate::AppleContainer {
                container: container("ubuntu:24.04"),
            },
        )]);

        let check = podman_check(Some(&config), &executor);

        assert_eq!(check.status, CheckStatus::Unsupported);
        assert_eq!(check.detail, "No local-podman target is configured.");
        assert!(executor.commands.borrow().is_empty());
    }

    #[test]
    fn podman_check_probes_the_host_when_a_local_podman_target_exists() {
        let executor = FakeExecutor::new(passing_podman_probes());
        let config = config_with([(
            "podman",
            TargetTemplate::LocalPodman {
                container: container("ubuntu:24.04"),
            },
        )]);

        let check = podman_check(Some(&config), &executor);

        assert_eq!(check.status, CheckStatus::Ready);
        assert!(check.detail.contains("Podman 5.4.2"));
        assert_eq!(executor.commands.borrow().len(), 3);
    }

    #[test]
    fn podman_check_is_fixable_with_an_upgrade_remediation_for_an_old_runtime() {
        let executor = FakeExecutor::new([Ok(output(b"podman version 3.4.7\n"))]);
        let config = config_with([(
            "podman",
            TargetTemplate::LocalPodman {
                container: container("ubuntu:24.04"),
            },
        )]);

        let check = podman_check(Some(&config), &executor);

        assert_eq!(check.status, CheckStatus::Fixable);
        assert!(
            check
                .remediation
                .as_deref()
                .unwrap()
                .contains("Upgrade Podman")
        );
    }

    #[test]
    fn podman_image_check_is_ready_when_the_image_is_present() {
        let executor = FakeExecutor::new([Ok(output(b""))]);

        let check =
            podman_image_check("podman", "localhost/hel/agent-dev:latest", &executor, false);

        assert_eq!(check.id, "runtime.podman.image.podman");
        assert_eq!(check.title, "Podman image for target podman");
        assert_eq!(check.status, CheckStatus::Ready);
        assert_eq!(
            executor.commands.borrow()[0].args,
            vec!["image", "exists", "localhost/hel/agent-dev:latest"]
        );
    }

    #[test]
    fn podman_image_check_is_fixable_with_a_pull_remediation_when_the_image_is_missing() {
        let executor = FakeExecutor::new([Ok(failed(b""))]);

        let check = podman_image_check("podman", "ghcr.io/example/dev:1", &executor, false);

        assert_eq!(check.status, CheckStatus::Fixable);
        assert!(
            check
                .detail
                .contains("is not present in local Podman storage")
        );
        assert_eq!(
            check.remediation.as_deref(),
            Some(
                "Pull it with `podman pull ghcr.io/example/dev:1`, build it from containers/Containerfile.agent-dev, or run `hel doctor --json --smoke` to verify the full pull-and-run path."
            )
        );
    }

    #[test]
    fn podman_image_check_smoke_runs_a_disposable_container() {
        let executor = FakeExecutor::new([
            Ok(output(b"created\n")),
            Ok(output(b"ok\n")),
            Ok(output(b"removed\n")),
        ]);

        let check = podman_image_check("podman", "ubuntu:24.04", &executor, true);

        assert_eq!(check.status, CheckStatus::Ready);
        let commands = executor.commands.borrow();
        assert_eq!(commands.len(), 3);
        assert!(commands.iter().all(|command| command.program == "podman"));
        assert_eq!(commands[0].args[0], "run");
        assert_eq!(commands[1].args[0], "exec");
        assert_eq!(commands[2].args[0], "rm");
    }

    #[test]
    fn image_checks_are_skipped_when_the_host_podman_preflight_fails() {
        let executor = FakeExecutor::new([Ok(output(b"podman version 3.4.7\n"))]);
        let config = config_with([(
            "podman",
            TargetTemplate::LocalPodman {
                container: container("ubuntu:24.04"),
            },
        )]);

        let checks = podman_checks(Some(&config), &executor, false);

        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].id, "runtime.podman");
    }

    #[test]
    fn image_checks_follow_a_passing_preflight_for_each_local_podman_target() {
        let mut responses = passing_podman_probes();
        responses.push(Ok(output(b"")));
        responses.push(Ok(failed(b"")));
        let executor = FakeExecutor::new(responses);
        let config = config_with([
            (
                "alpha",
                TargetTemplate::LocalPodman {
                    container: container("ubuntu:24.04"),
                },
            ),
            (
                "beta",
                TargetTemplate::LocalPodman {
                    container: container("ghcr.io/example/dev:1"),
                },
            ),
        ]);

        let checks = podman_checks(Some(&config), &executor, false);

        assert_eq!(
            checks
                .iter()
                .map(|check| check.id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "runtime.podman",
                "runtime.podman.image.alpha",
                "runtime.podman.image.beta"
            ]
        );
        assert_eq!(checks[1].status, CheckStatus::Ready);
        assert_eq!(checks[2].status, CheckStatus::Fixable);
    }

    #[test]
    fn ssh_podman_check_is_ready_after_ssh_wrapped_probes_without_smoke() {
        let executor = FakeExecutor::new(passing_podman_probes());

        let check = ssh_podman_check("remote", &runtime_ssh(), "ubuntu:24.04", &executor, false);

        assert_eq!(check.id, "runtime.ssh-podman.remote");
        assert_eq!(check.title, "Remote Podman for target remote");
        assert_eq!(check.status, CheckStatus::Ready);
        assert!(check.detail.contains("Remote rootless Podman 5.4.2"));
        assert!(check.detail.contains("dev@example.test"));
        let commands = executor.commands.borrow();
        assert_eq!(commands.len(), 3);
        for command in commands.iter() {
            assert_eq!(command.program, "ssh");
            assert!(command.args.contains(&"dev@example.test".to_owned()));
            assert!(command.args.last().unwrap().starts_with("'podman'"));
        }
    }

    #[test]
    fn ssh_podman_check_failure_scopes_the_remediation_to_the_remote_host() {
        let executor = FakeExecutor::new([Ok(output(b"podman version 3.4.7\n"))]);

        let check = ssh_podman_check("remote", &runtime_ssh(), "ubuntu:24.04", &executor, false);

        assert_eq!(check.status, CheckStatus::Fixable);
        assert!(check.detail.contains("dev@example.test"));
        assert!(
            check
                .remediation
                .as_deref()
                .unwrap()
                .starts_with("On dev@example.test: Upgrade Podman")
        );
    }

    #[test]
    fn ssh_podman_check_unreachable_host_recommends_verifying_ssh() {
        let executor =
            FakeExecutor::new([Err(anyhow!("ssh: connect to host example.test: timeout"))]);

        let check = ssh_podman_check("remote", &runtime_ssh(), "ubuntu:24.04", &executor, false);

        assert_eq!(check.status, CheckStatus::Fixable);
        assert_eq!(
            check.remediation.as_deref(),
            Some(
                "Verify `ssh dev@example.test` succeeds noninteractively from this host, then install rootless Podman 4 or newer there (see docs/PODMAN.md)."
            )
        );
    }

    #[test]
    fn ssh_podman_check_smoke_runs_an_ssh_wrapped_disposable_container() {
        let mut responses = passing_podman_probes();
        responses.extend([
            Ok(output(b"created\n")),
            Ok(output(b"ok\n")),
            Ok(output(b"removed\n")),
        ]);
        let executor = FakeExecutor::new(responses);

        let check = ssh_podman_check("remote", &runtime_ssh(), "ubuntu:24.04", &executor, true);

        assert_eq!(check.status, CheckStatus::Ready);
        let commands = executor.commands.borrow();
        assert_eq!(commands.len(), 6);
        for command in commands.iter().skip(3) {
            assert_eq!(command.program, "ssh");
            assert!(command.args.contains(&"dev@example.test".to_owned()));
        }
        assert!(commands[3].args.last().unwrap().contains("'run' '--init'"));
        assert!(commands[4].args.last().unwrap().ends_with("'true'"));
        assert!(commands[5].args.last().unwrap().contains("'rm' '--force'"));
    }

    #[test]
    fn ssh_podman_checks_are_skipped_without_a_valid_config() {
        let executor = FakeExecutor::new([]);

        assert!(ssh_podman_checks(None, &executor, false).is_empty());
        assert!(executor.commands.borrow().is_empty());
    }

    #[test]
    fn worker_check_for_an_ssh_podman_target_without_platform_is_unsupported() {
        let config = config_with([(
            "remote",
            TargetTemplate::SshPodman {
                ssh: ssh_connection(),
                container: container("ubuntu:24.04"),
            },
        )]);

        let checks = worker_binary_checks(Some(&config));

        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].id, "worker.remote");
        assert_eq!(checks[0].status, CheckStatus::Unsupported);
        assert_eq!(
            checks[0].detail,
            "Set `platform` on this ssh-podman target to check its worker binary; the remote architecture is unknown until provisioning."
        );
    }

    #[test]
    fn worker_check_for_an_ssh_podman_target_with_platform_uses_the_normal_check() {
        let mut remote = container("ubuntu:24.04");
        remote.platform = Some("linux/amd64".into());
        let config = config_with([(
            "remote",
            TargetTemplate::SshPodman {
                ssh: ssh_connection(),
                container: remote,
            },
        )]);

        let checks = worker_binary_checks(Some(&config));

        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].id, "worker.remote");
        assert_ne!(checks[0].status, CheckStatus::Unsupported);
        assert!(checks[0].detail.contains("x86_64-unknown-linux-musl"));
    }

    #[test]
    fn harness_discovery_reports_each_authentication_marker_state() {
        let check = harness_discovery_check_from(
            &[
                DiscoveredHome {
                    kind: HarnessKind::Codex,
                    path: "/agents/codex".into(),
                    authenticated: true,
                },
                DiscoveredHome {
                    kind: HarnessKind::Kimi,
                    path: "/agents/kimi".into(),
                    authenticated: false,
                },
            ],
            true,
        );

        assert_eq!(check.status, CheckStatus::Ready);
        assert!(
            check
                .detail
                .contains("Codex at /agents/codex (authenticated)")
        );
        assert!(
            check
                .detail
                .contains("Kimi Code at /agents/kimi (authentication marker missing)")
        );
    }

    #[test]
    fn missing_harness_homes_are_fixable_without_a_configured_profile() {
        let check = harness_discovery_check_from(&[], false);

        assert_eq!(check.status, CheckStatus::Fixable);
        assert_eq!(
            check.remediation.as_deref(),
            Some("Install and sign in to a supported harness, then run `hel setup`.")
        );
    }

    #[test]
    fn apple_container_is_unsupported_on_intel_macs() {
        let executor = FakeExecutor::new([]);

        let check = apple_container_check(
            &ApplePlatform::Macos {
                architecture: "x86_64".into(),
                major_version: 26,
            },
            &executor,
            false,
            DEFAULT_CONTAINER_IMAGE.into(),
        );

        assert_eq!(check.status, CheckStatus::Unsupported);
        assert!(check.detail.contains("Intel Macs"));
        assert!(executor.commands.borrow().is_empty());
    }

    #[test]
    fn apple_container_is_unsupported_before_macos_26() {
        let executor = FakeExecutor::new([]);

        let check = apple_container_check(
            &ApplePlatform::Macos {
                architecture: "aarch64".into(),
                major_version: 25,
            },
            &executor,
            false,
            DEFAULT_CONTAINER_IMAGE.into(),
        );

        assert_eq!(check.status, CheckStatus::Unsupported);
        assert!(check.detail.contains("macOS 26"));
    }

    #[test]
    fn apple_container_not_installed_has_official_package_remediation() {
        let executor = FakeExecutor::new([Err(anyhow!("No such file or directory"))]);

        let check = apple_container_check(
            &ApplePlatform::Macos {
                architecture: "aarch64".into(),
                major_version: 26,
            },
            &executor,
            false,
            DEFAULT_CONTAINER_IMAGE.into(),
        );

        assert_eq!(check.status, CheckStatus::Fixable);
        assert_eq!(
            check.remediation.as_deref(),
            Some(
                format!("Install the official signed package: {APPLE_CONTAINER_INSTALL_URL}")
                    .as_str()
            )
        );
    }

    #[test]
    fn apple_container_stopped_daemon_has_start_remediation() {
        let executor = FakeExecutor::new([
            Ok(output(b"container version 1\n")),
            Ok(CommandOutput {
                status: 1,
                stdout: vec![],
                stderr: b"daemon is not running".to_vec(),
            }),
        ]);

        let check = apple_container_check(
            &ApplePlatform::Macos {
                architecture: "aarch64".into(),
                major_version: 26,
            },
            &executor,
            false,
            DEFAULT_CONTAINER_IMAGE.into(),
        );

        assert_eq!(check.status, CheckStatus::Fixable);
        assert_eq!(
            check.remediation.as_deref(),
            Some("Run `container system start`.")
        );
    }

    #[test]
    fn apple_container_is_ready_only_after_the_opt_in_smoke_test() {
        let executor = FakeExecutor::new([
            Ok(output(b"container version 1\n")),
            Ok(output(b"running\n")),
            Ok(output(b"created\n")),
            Ok(output(b"ok\n")),
            Ok(output(b"removed\n")),
        ]);

        let check = apple_container_check(
            &ApplePlatform::Macos {
                architecture: "aarch64".into(),
                major_version: 26,
            },
            &executor,
            true,
            DEFAULT_CONTAINER_IMAGE.into(),
        );

        assert_eq!(check.status, CheckStatus::Ready);
        assert_eq!(executor.commands.borrow().len(), 5);
        assert_eq!(executor.commands.borrow()[2].args[0], "run");
        assert_eq!(executor.commands.borrow()[3].args[0], "exec");
        assert_eq!(executor.commands.borrow()[4].args[0], "rm");
    }

    #[test]
    fn linux_reports_apple_container_as_macos_only() {
        let executor = FakeExecutor::new([]);
        let check = apple_container_check(
            &ApplePlatform::Linux,
            &executor,
            false,
            DEFAULT_CONTAINER_IMAGE.into(),
        );
        assert_eq!(check.status, CheckStatus::Unsupported);
        assert_eq!(check.detail, "macOS only");
    }

    #[test]
    fn linux_instructions_embed_podman_postconditions_and_doctor_loop() {
        let instructions = setup_instructions(InstructionsPlatform::Linux);
        assert!(instructions.contains("hel doctor --json"));
        assert!(instructions.contains("hel doctor --json --smoke"));
        assert!(instructions.contains("podman unshare cat /proc/self/uid_map"));
        assert!(instructions.contains("Podman **4.0.0 or newer**"));
    }
}
