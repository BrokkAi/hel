//! Hel's versioned user configuration and domain model.
//!
//! This is intentionally a clean namespace. Nothing in this module reads or
//! migrates the legacy `mj` configuration tree.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

pub const CONFIG_VERSION: u32 = 1;
pub const PRODUCT_DIR: &str = "hel";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HarnessKind {
    Codex,
    Claude,
    Kimi,
    Grok,
    Deepseek,
}

/// How Hel puts a harness into the unrestricted mode where actions run without
/// per-action approval. Permission modes are not user-configurable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnrestrictedEnforcement {
    /// Selected over ACP once the session exists.
    AcpMode(&'static str),
    /// Applied to the bridge command line at launch. The harness exposes no
    /// ACP equivalent, so there is nothing to enforce after the session opens.
    LaunchFlag {
        flag: &'static str,
        label: &'static str,
    },
    /// Applied through the child environment at launch.
    LaunchEnvironment {
        key: &'static str,
        value: &'static str,
        label: &'static str,
    },
}

impl UnrestrictedEnforcement {
    /// Name reported to the UI for the mode this session runs in.
    pub const fn label(self) -> &'static str {
        match self {
            Self::AcpMode(mode) => mode,
            Self::LaunchFlag { label, .. } => label,
            Self::LaunchEnvironment { label, .. } => label,
        }
    }

    /// The ACP mode to select after the session opens, when there is one.
    pub const fn acp_mode(self) -> Option<&'static str> {
        match self {
            Self::AcpMode(mode) => Some(mode),
            Self::LaunchFlag { .. } | Self::LaunchEnvironment { .. } => None,
        }
    }

    /// The launch flag to add to the bridge command line, when there is one.
    pub const fn launch_flag(self) -> Option<&'static str> {
        match self {
            Self::AcpMode(_) => None,
            Self::LaunchFlag { flag, .. } => Some(flag),
            Self::LaunchEnvironment { .. } => None,
        }
    }

    pub const fn launch_environment(self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::LaunchEnvironment { key, value, .. } => Some((key, value)),
            Self::AcpMode(_) | Self::LaunchFlag { .. } => None,
        }
    }
}

impl HarnessKind {
    pub const ALL: [Self; 5] = [
        Self::Codex,
        Self::Claude,
        Self::Kimi,
        Self::Grok,
        Self::Deepseek,
    ];

    /// Environment variable used to isolate this harness's configuration.
    pub const fn home_env(self) -> &'static str {
        match self {
            Self::Codex => "CODEX_HOME",
            Self::Claude => "CLAUDE_CONFIG_DIR",
            Self::Kimi => "KIMI_CODE_HOME",
            Self::Grok => "GROK_HOME",
            Self::Deepseek => "DSH_HOME",
        }
    }

    /// Directory beneath the user's home the harness uses when `home_env` is
    /// unset. The single source for both setup discovery and import.
    pub const fn default_home_leaf(self) -> &'static str {
        match self {
            Self::Codex => ".codex",
            Self::Claude => ".claude",
            Self::Kimi => ".kimi-code",
            Self::Grok => ".grok",
            Self::Deepseek => ".dsh",
        }
    }

    /// Lowercase stable identifier used in config, storage, and the HTTP API.
    pub const fn id(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Kimi => "kimi",
            Self::Grok => "grok",
            Self::Deepseek => "deepseek",
        }
    }

    /// Product name shown to people.
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Codex => "Codex",
            Self::Claude => "Claude Code",
            Self::Kimi => "Kimi Code",
            Self::Grok => "Grok Build",
            Self::Deepseek => "DSH",
        }
    }

    /// How Hel forces this harness into its unrestricted mode.
    pub const fn unrestricted_enforcement(self) -> UnrestrictedEnforcement {
        match self {
            Self::Codex => UnrestrictedEnforcement::AcpMode("agent-full-access"),
            Self::Claude => UnrestrictedEnforcement::AcpMode("bypassPermissions"),
            Self::Kimi => UnrestrictedEnforcement::AcpMode("auto"),
            Self::Grok => UnrestrictedEnforcement::LaunchFlag {
                flag: "--always-approve",
                label: "always-approve",
            },
            Self::Deepseek => UnrestrictedEnforcement::LaunchEnvironment {
                key: "DSH_PERMISSION_MODE",
                value: "danger-full-access",
                label: "danger-full-access",
            },
        }
    }

    /// Whether this harness runs every action without asking, even on a bare
    /// target where Hel does not force unrestricted mode.
    ///
    /// Kimi Code arrives there through its own default `auto` mode. Grok Build
    /// arrives there because Hel always passes `--always-approve`: its default
    /// is to ask, and Hel answers a permission request by cancelling, so a
    /// session without the flag could not write anything at all. Codex and
    /// Claude Code ask on a bare target, and Hel cancels, which is a usable
    /// read-only session for them.
    pub const fn auto_approves_on_bare_targets(self) -> bool {
        match self {
            Self::Kimi | Self::Grok => true,
            Self::Codex | Self::Claude | Self::Deepseek => false,
        }
    }

    /// How this harness ends up approving everything, named for the person
    /// reading a warning about it.
    pub const fn bare_target_auto_approval(self) -> Option<&'static str> {
        match self {
            Self::Kimi => Some("its default auto mode"),
            Self::Grok => Some("Hel's --always-approve launch flag"),
            Self::Codex | Self::Claude | Self::Deepseek => None,
        }
    }

    /// The launch flag the bridge command line carries, if any.
    ///
    /// A harness that auto-approves on bare targets carries its flag on every
    /// target, because withholding it would not make that harness safer — it
    /// would only make the session useless.
    pub const fn launch_flag_for(self, unrestricted: bool) -> Option<&'static str> {
        match self.unrestricted_enforcement().launch_flag() {
            Some(flag) if unrestricted || self.auto_approves_on_bare_targets() => Some(flag),
            _ => None,
        }
    }

    /// Sub-command a `profile.executable` override needs to speak ACP. Codex
    /// and Claude override an adapter binary that already speaks it.
    pub fn bridge_override_args(self, unrestricted: bool) -> Vec<&'static str> {
        let flag = self.launch_flag_for(unrestricted);
        match self {
            Self::Codex | Self::Claude | Self::Deepseek => Vec::new(),
            Self::Kimi => vec!["acp"],
            Self::Grok => ["agent"].into_iter().chain(flag).chain(["stdio"]).collect(),
        }
    }
}

impl std::str::FromStr for HarnessKind {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.id() == value)
            .ok_or_else(|| anyhow!("unknown harness kind {value:?}"))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessProfile {
    pub kind: HarnessKind,
    /// Controller-side source home. A fresh copy is made for each target.
    pub home: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    /// Conservative byte budget for cross-harness transcript compaction.
    /// Bytes avoid pretending Hel has an accurate tokenizer for every model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window_bytes: Option<usize>,
}

impl HarnessProfile {
    pub fn home_env(&self) -> &'static str {
        self.kind.home_env()
    }

    pub fn unrestricted_enforcement(&self) -> UnrestrictedEnforcement {
        self.kind.unrestricted_enforcement()
    }

    fn validate(&self, id: &str) -> Result<()> {
        validate_id("profile", id)?;
        if self.home.as_os_str().is_empty() {
            bail!("profile {id:?} has an empty home path");
        }
        if self
            .environment
            .keys()
            .any(|key| key.trim().is_empty() || key.contains('='))
        {
            bail!("profile {id:?} contains an invalid environment variable name");
        }
        if self.environment.contains_key(self.kind.home_env()) {
            bail!(
                "profile {id:?} must use `home`, not override {} in `environment`",
                self.kind.home_env()
            );
        }
        if self
            .context_window_bytes
            .is_some_and(|bytes| bytes < 32 * 1024)
        {
            bail!("profile {id:?}: `context_window_bytes` must be at least 32768");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectRepository {
    /// Stable name within the bundle, used by `primary_repo`.
    pub id: String,
    /// GitHub HTTPS or SSH URL (or `owner/repository` shorthand).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github: Option<String>,
    /// Absolute controller-side Git repository exposed through Hel's Git proxy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<PathBuf>,
    /// Safe relative path beneath the target's bundle root.
    pub destination: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectBundle {
    /// Repository id used as the ACP session cwd.
    pub primary_repo: String,
    pub repositories: Vec<ProjectRepository>,
}

impl ProjectBundle {
    fn validate(&self, bundle_id: &str) -> Result<()> {
        validate_id("bundle", bundle_id)?;
        if self.repositories.is_empty() {
            bail!("bundle {bundle_id:?} must contain at least one repository");
        }

        let mut ids = BTreeSet::new();
        let mut destinations = Vec::<PathBuf>::new();
        for repository in &self.repositories {
            validate_id("repository", &repository.id)
                .with_context(|| format!("bundle {bundle_id:?}"))?;
            if !ids.insert(repository.id.as_str()) {
                bail!(
                    "bundle {bundle_id:?} contains duplicate repository id {:?}",
                    repository.id
                );
            }
            if repository.github.is_some() == repository.local.is_some() {
                bail!(
                    "bundle {bundle_id:?} repository {:?} must declare exactly one of `github` or `local`",
                    repository.id,
                );
            }
            if repository
                .github
                .as_deref()
                .is_some_and(|source| !is_github_source(source))
            {
                bail!(
                    "bundle {bundle_id:?} repository {:?} is not a supported GitHub source",
                    repository.id,
                );
            }
            if repository
                .local
                .as_deref()
                .is_some_and(|path| !path.is_absolute())
            {
                bail!(
                    "bundle {bundle_id:?} repository {:?} local path must be absolute",
                    repository.id,
                );
            }
            if repository.local.is_some() && repository.git_ref.is_some() {
                bail!(
                    "bundle {bundle_id:?} repository {:?} cannot use `git_ref` with `local`",
                    repository.id,
                );
            }
            validate_relative_destination(&repository.destination).with_context(|| {
                format!(
                    "bundle {bundle_id:?} repository {:?} destination",
                    repository.id
                )
            })?;
            if let Some(existing) = destinations.iter().find(|existing| {
                repository.destination.starts_with(existing)
                    || existing.starts_with(&repository.destination)
            }) {
                bail!(
                    "bundle {bundle_id:?} contains overlapping destinations {} and {}",
                    existing.display(),
                    repository.destination.display()
                );
            }
            destinations.push(repository.destination.clone());
            if repository
                .git_ref
                .as_deref()
                .is_some_and(|git_ref| git_ref.trim().is_empty())
            {
                bail!(
                    "bundle {bundle_id:?} repository {:?} has an empty git ref",
                    repository.id
                );
            }
        }
        if !ids.contains(self.primary_repo.as_str()) {
            bail!(
                "bundle {bundle_id:?} primary repository {:?} does not exist",
                self.primary_repo
            );
        }
        Ok(())
    }

    pub fn primary(&self) -> Option<&ProjectRepository> {
        self.repositories
            .iter()
            .find(|repository| repository.id == self.primary_repo)
    }
}

impl ProjectRepository {
    pub fn source_label(&self) -> String {
        self.github
            .clone()
            .or_else(|| self.local.as_ref().map(|path| path.display().to_string()))
            .unwrap_or_else(|| "invalid repository source".into())
    }

    pub fn is_local(&self) -> bool {
        self.local.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerTemplate {
    pub image: String,
    #[serde(default, skip_serializing_if = "ImagePullPolicy::is_auto")]
    pub pull_policy: ImagePullPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpus: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImagePullPolicy {
    #[default]
    Auto,
    Always,
    Newer,
    Missing,
    Never,
}

impl ImagePullPolicy {
    fn is_auto(&self) -> bool {
        *self == Self::Auto
    }
}

impl ContainerTemplate {
    fn validate(&self, template_id: &str) -> Result<()> {
        if self.image.trim().is_empty() {
            bail!("target template {template_id:?} has an empty container image");
        }
        validate_environment(template_id, &self.environment)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AwsAddressSource {
    #[default]
    PublicDns,
    PublicIp,
    PrivateDns,
    PrivateIp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SshConnection {
    /// OpenSSH destination such as `builder.example.com` or an SSH config alias.
    pub host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
}

impl SshConnection {
    fn validate(&self, template_id: &str) -> Result<()> {
        if self.host.trim().is_empty() || self.host.chars().any(char::is_whitespace) {
            bail!("target template {template_id:?} has an invalid SSH host");
        }
        if self.user.as_deref().is_some_and(|user| {
            user.is_empty() || user.chars().any(|c| c.is_whitespace() || c == '@')
        }) {
            bail!("target template {template_id:?} has an invalid SSH user");
        }
        Ok(())
    }
}

fn default_named_machine_prefix() -> PathBuf {
    PathBuf::from(".local/share/hel/workspaces")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum TargetTemplate {
    LocalBare,
    LocalPodman {
        #[serde(flatten)]
        container: ContainerTemplate,
    },
    AppleContainer {
        #[serde(flatten)]
        container: ContainerTemplate,
    },
    AwsEc2 {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        aws_profile: Option<String>,
        region: String,
        launch_template: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        launch_template_version: Option<String>,
        ssh_user: String,
        #[serde(default)]
        address_source: AwsAddressSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        identity_file: Option<PathBuf>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        ssh_args: Vec<String>,
    },
    SshBare {
        #[serde(flatten)]
        ssh: SshConnection,
        #[serde(default = "default_named_machine_prefix")]
        workspace_prefix: PathBuf,
    },
    SshPodman {
        #[serde(flatten)]
        ssh: SshConnection,
        #[serde(flatten)]
        container: ContainerTemplate,
    },
}

impl TargetTemplate {
    fn validate(&self, id: &str) -> Result<()> {
        validate_id("target template", id)?;
        match self {
            Self::LocalBare => Ok(()),
            Self::LocalPodman { container } | Self::AppleContainer { container } => {
                container.validate(id)
            }
            Self::AwsEc2 {
                aws_profile,
                region,
                launch_template,
                launch_template_version,
                ssh_user,
                ..
            } => {
                if region.trim().is_empty()
                    || launch_template.trim().is_empty()
                    || ssh_user.trim().is_empty()
                {
                    bail!(
                        "AWS target template {id:?} requires region, launch_template, and ssh_user"
                    );
                }
                if aws_profile.as_deref().is_some_and(str::is_empty)
                    || launch_template_version
                        .as_deref()
                        .is_some_and(str::is_empty)
                {
                    bail!("AWS target template {id:?} contains an empty optional value");
                }
                Ok(())
            }
            Self::SshBare {
                ssh,
                workspace_prefix,
            } => {
                ssh.validate(id)?;
                if workspace_prefix.as_os_str().is_empty()
                    || workspace_prefix
                        .components()
                        .any(|part| part == Component::ParentDir)
                    || matches!(workspace_prefix.to_str(), Some("/" | "." | "~" | "~/"))
                {
                    bail!("target template {id:?} has an unsafe workspace prefix");
                }
                Ok(())
            }
            Self::SshPodman { ssh, container } => {
                ssh.validate(id)?;
                container.validate(id)
            }
        }
    }
}

/// Whether `template` hosts a raw project checkout directly on its machine,
/// with no managed workspace. Bare targets take a project directory instead
/// of a bundle.
pub fn is_bare_project_target(template: &TargetTemplate) -> bool {
    matches!(
        template,
        TargetTemplate::LocalBare | TargetTemplate::SshBare { .. }
    )
}

/// The host name that prompt-history mounts on `template` should be filed
/// under, or `None` if the target does not support attached mounts.
pub fn mount_history_host(template: &TargetTemplate) -> Option<&str> {
    match template {
        TargetTemplate::LocalPodman { .. }
        | TargetTemplate::AppleContainer { .. }
        | TargetTemplate::AwsEc2 { .. } => Some("local"),
        TargetTemplate::SshPodman { ssh, .. } => Some(&ssh.host),
        TargetTemplate::LocalBare | TargetTemplate::SshBare { .. } => None,
    }
}

fn validate_environment(owner: &str, environment: &BTreeMap<String, String>) -> Result<()> {
    if environment
        .keys()
        .any(|key| key.trim().is_empty() || key.contains('='))
    {
        bail!("{owner:?} contains an invalid environment variable name");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelConfig {
    pub version: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub profiles: BTreeMap<String, HarnessProfile>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub bundles: BTreeMap<String, ProjectBundle>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub targets: BTreeMap<String, TargetTemplate>,
}

impl Default for HelConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            profiles: BTreeMap::new(),
            bundles: BTreeMap::new(),
            targets: BTreeMap::new(),
        }
    }
}

impl HelConfig {
    pub fn validate(&self) -> Result<()> {
        if self.version != CONFIG_VERSION {
            bail!(
                "unsupported Hel config version {}; expected {CONFIG_VERSION}",
                self.version
            );
        }
        for (id, profile) in &self.profiles {
            profile.validate(id)?;
        }
        for (id, bundle) in &self.bundles {
            bundle.validate(id)?;
        }
        for (id, target) in &self.targets {
            target.validate(id)?;
        }
        Ok(())
    }

    pub fn load() -> Result<Self> {
        Self::load_from(&config_path())
    }

    pub fn load_from(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let contents = fs::read_to_string(path)
            .with_context(|| format!("read Hel config {}", path.display()))?;
        if contents.trim().is_empty() {
            return Ok(Self::default());
        }
        reject_removed_profile_overrides(&contents)?;
        let config: Self = toml::from_str(&contents)
            .with_context(|| format!("parse Hel config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn save(&self) -> Result<()> {
        self.save_to(&config_path())
    }

    pub fn save_to(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let body = toml::to_string_pretty(self).context("serialize Hel config")?;
        atomic_write(path, body.as_bytes())
    }

    /// Rename the setup-generated local bare target without rewriting
    /// unrelated configuration. This runs under the controller store lock
    /// before SQLite is opened, so config and persisted sessions converge in
    /// one startup.
    pub fn migrate_legacy_localhost_target() -> Result<bool> {
        Self::migrate_legacy_localhost_target_at(&config_path())
    }

    fn migrate_legacy_localhost_target_at(path: &Path) -> Result<bool> {
        if !path.exists() {
            return Ok(false);
        }
        let mut config = Self::load_from(path)?;
        let Some(legacy) = config.targets.get("raw-localhost").cloned() else {
            return Ok(false);
        };
        if let Some(current) = config.targets.get("localhost")
            && current != &legacy
        {
            bail!(
                "cannot rename target `raw-localhost` to `localhost`: both exist with different configurations"
            );
        }
        config.targets.remove("raw-localhost");
        config.targets.entry("localhost".into()).or_insert(legacy);
        config.save_to(path)?;
        Ok(true)
    }
}

fn reject_removed_profile_overrides(contents: &str) -> Result<()> {
    let value: toml::Value = contents.parse().context("parse Hel config TOML")?;
    let Some(profiles) = value.get("profiles").and_then(toml::Value::as_table) else {
        return Ok(());
    };
    for (id, profile) in profiles {
        let Some(profile) = profile.as_table() else {
            continue;
        };
        for key in ["model", "reasoning_effort"] {
            if profile.contains_key(key) {
                bail!(
                    "profile {id:?}: `{key}` is no longer supported; configure it in the harness home or change it per session with `/config`"
                );
            }
        }
    }
    Ok(())
}

pub fn config_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("HEL_CONFIG_DIR") {
        return PathBuf::from(path);
    }
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from(".config"))
        .join(PRODUCT_DIR)
}

pub fn config_path() -> PathBuf {
    config_dir().join("config.toml")
}

pub fn data_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("HEL_DATA_DIR") {
        return PathBuf::from(path);
    }
    dirs::data_local_dir()
        .or_else(dirs::data_dir)
        .unwrap_or_else(|| PathBuf::from(".local/share"))
        .join(PRODUCT_DIR)
}

pub fn sessions_dir() -> PathBuf {
    data_dir().join("sessions")
}

pub fn validate_id(kind: &str, id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || matches!(id, "." | "..")
    {
        bail!("invalid {kind} id {id:?}; use 1-64 ASCII letters, digits, '.', '-' or '_'");
    }
    Ok(())
}

pub fn validate_relative_destination(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        bail!("destination must be a non-empty relative path");
    }
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => bail!("destination must not contain '.'"),
            Component::ParentDir => bail!("destination must not contain '..'"),
            Component::Prefix(_) | Component::RootDir => {
                bail!("destination must not be absolute")
            }
        }
    }
    Ok(())
}

fn is_github_source(source: &str) -> bool {
    let source = source.trim();
    if source.is_empty() || source.starts_with('-') || source.chars().any(char::is_whitespace) {
        return false;
    }
    let repository_path = source
        .strip_prefix("https://github.com/")
        .or_else(|| source.strip_prefix("git@github.com:"))
        .or_else(|| source.strip_prefix("ssh://git@github.com/"))
        .unwrap_or(source);
    let mut parts = repository_path.trim_end_matches(".git").split('/');
    matches!((parts.next(), parts.next(), parts.next()), (Some(owner), Some(repository), None) if !owner.is_empty() && !repository.is_empty())
}

/// Replace `path` without exposing a partially-written configuration/state file.
pub(crate) fn atomic_write(path: &Path, body: &[u8]) -> Result<()> {
    atomic_write_with_parent(path, body, ParentDirectory::Create)
}

/// Replace `path` only while its directory still exists.
///
/// Worker state lives inside a directory that session teardown deletes out
/// from under the running daemon. Recreating it here would resurrect a closed
/// session's relay state, so a vanished parent must be an error instead.
pub(crate) fn atomic_write_existing(path: &Path, body: &[u8]) -> Result<()> {
    atomic_write_with_parent(path, body, ParentDirectory::Require)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParentDirectory {
    Create,
    Require,
}

fn atomic_write_with_parent(
    path: &Path,
    body: &[u8],
    parent_directory: ParentDirectory,
) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    match parent_directory {
        ParentDirectory::Create => {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        ParentDirectory::Require => {
            if !parent.is_dir() {
                bail!("directory {} is missing", parent.display());
            }
        }
    }

    let mut random = [0u8; 8];
    getrandom::fill(&mut random)
        .map_err(|error| anyhow!("generate temporary filename: {error}"))?;
    let suffix = u64::from_le_bytes(random);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("hel");
    let temporary = parent.join(format!(
        ".{file_name}.{}.{suffix:016x}.tmp",
        std::process::id()
    ));

    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .with_context(|| format!("create {}", temporary.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(body)
            .with_context(|| format!("write {}", temporary.display()))?;
        file.sync_all()
            .with_context(|| format!("sync {}", temporary.display()))?;
        drop(file);
        fs::rename(&temporary, path)
            .with_context(|| format!("replace {} with {}", path.display(), temporary.display()))?;
        #[cfg(unix)]
        OpenOptions::new()
            .read(true)
            .open(parent)
            .and_then(|directory| directory.sync_all())
            .with_context(|| format!("sync {}", parent.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> HelConfig {
        HelConfig {
            version: CONFIG_VERSION,
            profiles: BTreeMap::from([(
                "codex-1".into(),
                HarnessProfile {
                    context_window_bytes: None,
                    kind: HarnessKind::Codex,
                    home: PathBuf::from("/home/test/.codex-one"),
                    executable: None,
                    environment: BTreeMap::from([("RUST_LOG".into(), "info".into())]),
                },
            )]),
            bundles: BTreeMap::from([(
                "hel".into(),
                ProjectBundle {
                    primary_repo: "app".into(),
                    repositories: vec![ProjectRepository {
                        id: "app".into(),
                        github: Some("BrokkAi/hel".into()),
                        local: None,
                        destination: PathBuf::from("app"),
                        git_ref: None,
                    }],
                },
            )]),
            targets: BTreeMap::from([(
                "podman-default".into(),
                TargetTemplate::LocalPodman {
                    container: ContainerTemplate {
                        image: "ubuntu:24.04".into(),
                        pull_policy: ImagePullPolicy::Auto,
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
    fn legacy_localhost_target_migration_is_atomic_and_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        let mut config = sample_config();
        config.targets.clear();
        config
            .targets
            .insert("raw-localhost".into(), TargetTemplate::LocalBare);
        config.save_to(&path).unwrap();

        assert!(HelConfig::migrate_legacy_localhost_target_at(&path).unwrap());
        let migrated = HelConfig::load_from(&path).unwrap();
        assert_eq!(
            migrated.targets.get("localhost"),
            Some(&TargetTemplate::LocalBare)
        );
        assert!(!migrated.targets.contains_key("raw-localhost"));
        assert!(!HelConfig::migrate_legacy_localhost_target_at(&path).unwrap());
    }

    #[test]
    fn conflicting_localhost_target_migration_leaves_config_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        let mut config = sample_config();
        config
            .targets
            .insert("raw-localhost".into(), TargetTemplate::LocalBare);
        config.targets.insert(
            "localhost".into(),
            TargetTemplate::LocalPodman {
                container: ContainerTemplate {
                    image: "different".into(),
                    pull_policy: ImagePullPolicy::Auto,
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: BTreeMap::new(),
                },
            },
        );
        config.save_to(&path).unwrap();
        let before = fs::read(&path).unwrap();

        assert!(HelConfig::migrate_legacy_localhost_target_at(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test]
    fn harness_mapping_and_permission_modes_are_fixed() {
        assert_eq!(HarnessKind::Codex.home_env(), "CODEX_HOME");
        assert_eq!(HarnessKind::Claude.home_env(), "CLAUDE_CONFIG_DIR");
        assert_eq!(HarnessKind::Kimi.home_env(), "KIMI_CODE_HOME");
        assert_eq!(HarnessKind::Grok.home_env(), "GROK_HOME");
        assert_eq!(
            HarnessKind::Codex.unrestricted_enforcement(),
            UnrestrictedEnforcement::AcpMode("agent-full-access")
        );
        assert_eq!(
            HarnessKind::Claude.unrestricted_enforcement(),
            UnrestrictedEnforcement::AcpMode("bypassPermissions")
        );
        assert_eq!(
            HarnessKind::Kimi.unrestricted_enforcement(),
            UnrestrictedEnforcement::AcpMode("auto")
        );
        assert_eq!(
            HarnessKind::Grok.unrestricted_enforcement(),
            UnrestrictedEnforcement::LaunchFlag {
                flag: "--always-approve",
                label: "always-approve",
            }
        );
    }

    #[test]
    fn harness_unrestricted_enforcement_splits_acp_modes_from_launch_flags() {
        for kind in [HarnessKind::Codex, HarnessKind::Claude, HarnessKind::Kimi] {
            let enforcement = kind.unrestricted_enforcement();
            assert_eq!(enforcement.acp_mode(), Some(enforcement.label()));
            assert_eq!(enforcement.launch_flag(), None);
        }
        let grok = HarnessKind::Grok.unrestricted_enforcement();
        assert_eq!(grok.acp_mode(), None);
        assert_eq!(grok.launch_flag(), Some("--always-approve"));
        assert_eq!(grok.label(), "always-approve");
        let deepseek = HarnessKind::Deepseek.unrestricted_enforcement();
        assert_eq!(deepseek.acp_mode(), None);
        assert_eq!(deepseek.launch_flag(), None);
        assert_eq!(
            deepseek.launch_environment(),
            Some(("DSH_PERMISSION_MODE", "danger-full-access"))
        );
    }

    #[test]
    fn harness_names_and_ids_round_trip() {
        for kind in HarnessKind::ALL {
            assert_eq!(kind.id().parse::<HarnessKind>().unwrap(), kind);
            assert_eq!(
                serde_json::to_value(kind).unwrap(),
                serde_json::Value::String(kind.id().to_owned())
            );
            assert!(!kind.display_name().is_empty());
            assert!(kind.default_home_leaf().starts_with('.'));
        }
        assert_eq!(HarnessKind::Grok.id(), "grok");
        assert_eq!(HarnessKind::Grok.display_name(), "Grok Build");
        assert_eq!(HarnessKind::Grok.default_home_leaf(), ".grok");
        assert_eq!(HarnessKind::Deepseek.display_name(), "DSH");
        assert_eq!(HarnessKind::Deepseek.home_env(), "DSH_HOME");
        assert!("nope".parse::<HarnessKind>().is_err());
    }

    #[test]
    fn bridge_override_args_carry_the_acp_subcommand_per_harness() {
        for unrestricted in [false, true] {
            assert!(
                HarnessKind::Codex
                    .bridge_override_args(unrestricted)
                    .is_empty()
            );
            assert!(
                HarnessKind::Claude
                    .bridge_override_args(unrestricted)
                    .is_empty()
            );
            assert_eq!(
                HarnessKind::Kimi.bridge_override_args(unrestricted),
                ["acp"]
            );
            assert!(
                HarnessKind::Deepseek
                    .bridge_override_args(unrestricted)
                    .is_empty()
            );
            // Grok Build asks by default and Hel answers by cancelling, so a
            // session without the flag could not write at all. It carries the
            // flag on every target, restricted ones included.
            assert_eq!(
                HarnessKind::Grok.bridge_override_args(unrestricted),
                ["agent", "--always-approve", "stdio"],
                "unrestricted: {unrestricted}"
            );
        }
    }

    #[test]
    fn only_a_blanket_approval_harness_carries_its_launch_flag_everywhere() {
        for unrestricted in [false, true] {
            assert_eq!(
                HarnessKind::Grok.launch_flag_for(unrestricted),
                Some("--always-approve")
            );
            // The ACP-mode harnesses have no launch flag to carry.
            for kind in [
                HarnessKind::Codex,
                HarnessKind::Claude,
                HarnessKind::Kimi,
                HarnessKind::Deepseek,
            ] {
                assert_eq!(kind.launch_flag_for(unrestricted), None, "{kind:?}");
            }
        }
    }

    #[test]
    fn bare_target_auto_approval_names_the_mechanism_per_harness() {
        assert_eq!(
            HarnessKind::Kimi.bare_target_auto_approval(),
            Some("its default auto mode")
        );
        assert_eq!(
            HarnessKind::Grok.bare_target_auto_approval(),
            Some("Hel's --always-approve launch flag")
        );
        for kind in [
            HarnessKind::Codex,
            HarnessKind::Claude,
            HarnessKind::Deepseek,
        ] {
            assert_eq!(kind.bare_target_auto_approval(), None, "{kind:?}");
        }
        // The warning and the flag decision read the same fact.
        for kind in HarnessKind::ALL {
            assert_eq!(
                kind.auto_approves_on_bare_targets(),
                kind.bare_target_auto_approval().is_some(),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn bundle_rejects_traversal_and_duplicate_destinations() {
        let mut config = sample_config();
        config.bundles.get_mut("hel").unwrap().repositories[0].destination =
            PathBuf::from("../escape");
        assert!(format!("{:#}", config.validate().unwrap_err()).contains("'..'"));

        let mut config = sample_config();
        let bundle = config.bundles.get_mut("hel").unwrap();
        bundle.repositories.push(ProjectRepository {
            id: "docs".into(),
            github: Some("BrokkAi/docs".into()),
            local: None,
            destination: PathBuf::from("app"),
            git_ref: None,
        });
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("overlapping destinations")
        );
    }

    #[test]
    fn bundle_requires_existing_primary_repository() {
        let mut config = sample_config();
        config.bundles.get_mut("hel").unwrap().primary_repo = "missing".into();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("does not exist")
        );
    }

    #[test]
    fn bundle_rejects_non_github_sources() {
        let mut config = sample_config();
        config.bundles.get_mut("hel").unwrap().repositories[0].github =
            Some("https://example.com/owner/repo".into());
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("not a supported GitHub source")
        );
    }

    #[test]
    fn bundle_accepts_one_absolute_local_source() {
        let mut config = sample_config();
        {
            let repository = &mut config.bundles.get_mut("hel").unwrap().repositories[0];
            repository.github = None;
            repository.local = Some(PathBuf::from("/home/test/src/app"));
        }
        config.validate().unwrap();

        config.bundles.get_mut("hel").unwrap().repositories[0].local =
            Some(PathBuf::from("relative/app"));
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("absolute")
        );
    }

    #[test]
    fn bundle_requires_exactly_one_repository_source() {
        let mut config = sample_config();
        config.bundles.get_mut("hel").unwrap().repositories[0].local =
            Some(PathBuf::from("/home/test/src/app"));
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );
    }

    #[test]
    fn config_toml_round_trip_is_atomic() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/config.toml");
        let config = sample_config();
        config.save_to(&path).unwrap();
        assert_eq!(HelConfig::load_from(&path).unwrap(), config);
        assert!(!fs::read_to_string(&path).unwrap().contains("pull_policy"));
        assert_eq!(
            fs::read_to_string(path)
                .unwrap()
                .matches("kind = \"local-podman\"")
                .count(),
            1
        );
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
    fn explicit_image_pull_policy_round_trips() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        let mut config = sample_config();
        let TargetTemplate::LocalPodman { container } =
            config.targets.get_mut("podman-default").unwrap()
        else {
            unreachable!()
        };
        container.pull_policy = ImagePullPolicy::Never;

        config.save_to(&path).unwrap();

        assert!(
            fs::read_to_string(&path)
                .unwrap()
                .contains("pull_policy = \"never\"")
        );
        assert_eq!(HelConfig::load_from(&path).unwrap(), config);
    }

    #[test]
    fn missing_config_uses_clean_v1_defaults() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            HelConfig::load_from(&directory.path().join("missing.toml")).unwrap(),
            HelConfig::default()
        );
    }

    #[test]
    fn empty_config_uses_clean_v1_defaults() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, "\n\t").unwrap();
        assert_eq!(HelConfig::load_from(&path).unwrap(), HelConfig::default());
    }

    #[test]
    fn removed_profile_overrides_have_an_actionable_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(
            &path,
            "version = 1\n[profiles.codex]\nkind = \"codex\"\nhome = \"/tmp/codex\"\nmodel = \"gpt-old\"\n",
        )
        .unwrap();
        let error = HelConfig::load_from(&path).unwrap_err().to_string();
        assert!(error.contains("`model` is no longer supported"));
        assert!(error.contains("/config"));
    }

    #[test]
    fn profile_cannot_override_its_isolated_home() {
        let mut config = sample_config();
        config
            .profiles
            .get_mut("codex-1")
            .unwrap()
            .environment
            .insert("CODEX_HOME".into(), "/shared-and-racy".into());
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("must use `home`")
        );
    }
}
