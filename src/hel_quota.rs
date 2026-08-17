//! One-pane quota collection for Hel harness profiles.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::claude_usage;
use crate::codex_usage::{self, CodexUsageClient, CodexUsageStatus};
use crate::grok_usage;
use crate::hel_config::HarnessKind;

#[derive(Debug, Clone)]
pub struct QuotaRefreshRequest {
    pub profile_id: String,
    pub harness: HarnessKind,
    pub source_home: std::path::PathBuf,
    /// Harness CLI override from the profile, for the backends that shell out
    /// to the CLI itself rather than to an adapter.
    pub executable: Option<std::path::PathBuf>,
    pub environment: BTreeMap<String, String>,
    pub cwd: std::path::PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuotaWindow {
    pub label: String,
    pub remaining_percent: Option<u8>,
    pub used: Option<i64>,
    pub limit: Option<i64>,
    pub resets: Option<String>,
    #[serde(default)]
    pub resets_at_epoch_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileQuota {
    pub profile_id: String,
    pub harness: HarnessKind,
    pub windows: Vec<QuotaWindow>,
    pub extra: Option<String>,
    pub error: Option<String>,
    pub refreshed_at_epoch_seconds: u64,
}

impl ProfileQuota {
    pub fn weekly_window(&self) -> Option<&QuotaWindow> {
        self.windows
            .iter()
            .find(|window| is_weekly_quota_window(&window.label))
    }

    pub fn five_hour_window(&self) -> Option<&QuotaWindow> {
        self.windows
            .iter()
            .find(|window| is_short_quota_window(&window.label))
    }

    pub fn five_hour_projects_exhaustion(&self) -> bool {
        self.five_hour_window().is_some_and(|window| {
            projects_exhaustion_before_reset(window, self.refreshed_at_epoch_seconds)
        })
    }

    pub fn compact(&self) -> String {
        if let Some(error) = &self.error {
            return format!("unavailable: {error}");
        }
        let mut seen_resets = BTreeSet::new();
        let mut parts = self
            .windows
            .iter()
            .filter(|window| {
                !is_short_quota_window(&window.label)
                    || projects_exhaustion_before_reset(window, self.refreshed_at_epoch_seconds)
            })
            .map(|window| {
                let usage = match (window.remaining_percent, window.used, window.limit) {
                    (Some(remaining), _, _) => format!("{remaining}% left"),
                    (_, Some(used), Some(limit)) => format!("{used}/{limit}"),
                    _ => "available".to_string(),
                };
                match window
                    .resets
                    .as_ref()
                    .filter(|reset| seen_resets.insert((*reset).clone()))
                {
                    Some(reset) => format!("{} {usage}, resets {reset}", window.label),
                    None => format!("{} {usage}", window.label),
                }
            })
            .collect::<Vec<_>>();
        if let Some(extra) = &self.extra {
            parts.push(extra.clone());
        }
        if parts.is_empty() {
            "no quota windows reported".to_string()
        } else {
            parts.join(" · ")
        }
    }
}

/// The dashboard's long-window column. A harness billed monthly rather than
/// weekly belongs in the same column; the label itself names the real period.
fn is_weekly_quota_window(label: &str) -> bool {
    matches!(
        label.to_ascii_lowercase().as_str(),
        "week" | "weekly" | "7d" | "month" | "monthly"
    )
}

fn is_short_quota_window(label: &str) -> bool {
    matches!(
        label.to_ascii_lowercase().as_str(),
        "5h" | "5-hour" | "5 hour"
    )
}

fn projects_exhaustion_before_reset(window: &QuotaWindow, now: u64) -> bool {
    const FIVE_HOURS_SECONDS: i64 = 5 * 60 * 60;
    let Some(reset) = window.resets_at_epoch_seconds else {
        return false;
    };
    let Ok(now) = i64::try_from(now) else {
        return false;
    };
    let remaining_time = reset - now;
    let elapsed = FIVE_HOURS_SECONDS - remaining_time;
    if remaining_time <= 0 || elapsed <= 0 || elapsed >= FIVE_HOURS_SECONDS {
        return false;
    }
    if let (Some(used), Some(limit)) = (window.used, window.limit)
        && limit > 0
    {
        return i128::from(used.clamp(0, limit)) * i128::from(FIVE_HOURS_SECONDS)
            > i128::from(limit) * i128::from(elapsed);
    }
    window
        .remaining_percent
        .is_some_and(|remaining| i64::from(100 - remaining) * FIVE_HOURS_SECONDS > 100 * elapsed)
}

#[derive(Default)]
pub struct QuotaManager {
    codex_clients: HashMap<String, CodexUsageClient>,
    reports: BTreeMap<String, ProfileQuota>,
}

impl QuotaManager {
    pub fn reports(&self) -> &BTreeMap<String, ProfileQuota> {
        &self.reports
    }

    /// Refresh each profile independently so one slow harness cannot delay the
    /// others. Reports are returned in completion order.
    pub async fn refresh_profiles(
        &mut self,
        requests: Vec<QuotaRefreshRequest>,
    ) -> Vec<ProfileQuota> {
        let mut tasks = tokio::task::JoinSet::new();
        for request in requests {
            let client = self.codex_clients.remove(&request.profile_id);
            tasks.spawn(refresh_profile(request, client));
        }

        let mut refreshed = Vec::new();
        while let Some(result) = tasks.join_next().await {
            let (report, client) = match result {
                Ok(output) => output,
                Err(error) => {
                    tracing::warn!(%error, "quota refresh task failed");
                    continue;
                }
            };
            if let Some(client) = client {
                self.codex_clients.insert(report.profile_id.clone(), client);
            }
            self.reports
                .insert(report.profile_id.clone(), report.clone());
            refreshed.push(report);
        }
        refreshed
    }

    pub async fn shutdown(mut self) {
        for (_, client) in self.codex_clients.drain() {
            client.shutdown().await;
        }
    }
}

async fn refresh_profile(
    request: QuotaRefreshRequest,
    mut codex_client: Option<CodexUsageClient>,
) -> (ProfileQuota, Option<CodexUsageClient>) {
    let QuotaRefreshRequest {
        profile_id,
        harness,
        source_home,
        executable,
        environment,
        cwd,
    } = request;
    let environment = environment.into_iter().collect::<HashMap<_, _>>();
    let refreshed_at_epoch_seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let result = match harness {
        HarnessKind::Codex => {
            let status = codex_usage::refresh(&mut codex_client, cwd, environment).await;
            match status {
                CodexUsageStatus::Available(report) => Ok(ProfileQuota {
                    profile_id: profile_id.clone(),
                    harness,
                    windows: [report.primary, report.secondary]
                        .into_iter()
                        .flatten()
                        .map(|window| QuotaWindow {
                            label: window.label,
                            remaining_percent: Some(window.remaining_percent),
                            used: None,
                            limit: None,
                            resets: window
                                .resets_at
                                .and_then(crate::usage_format::format_reset_local_seconds),
                            resets_at_epoch_seconds: window.resets_at,
                        })
                        .collect(),
                    extra: None,
                    error: None,
                    refreshed_at_epoch_seconds,
                }),
                CodexUsageStatus::Unavailable(error) => Err(anyhow::anyhow!(error)),
            }
        }
        HarnessKind::Claude => claude_usage::query(cwd, environment)
            .await
            .map(|report| ProfileQuota {
                profile_id: profile_id.clone(),
                harness,
                windows: [
                    report.five_hour.map(|window| ("5H", window)),
                    report.week.map(|window| ("Week", window)),
                ]
                .into_iter()
                .flatten()
                .map(|(label, window)| QuotaWindow {
                    label: label.to_string(),
                    remaining_percent: Some(window.remaining_percent),
                    used: None,
                    limit: None,
                    resets: window
                        .reset_context
                        .as_deref()
                        .and_then(crate::usage_format::normalize_reset_text),
                    resets_at_epoch_seconds: window
                        .reset_context
                        .as_deref()
                        .and_then(crate::usage_format::normalize_reset_epoch_seconds),
                })
                .collect(),
                extra: None,
                error: None,
                refreshed_at_epoch_seconds,
            })
            .map_err(|error| anyhow::anyhow!(error.to_string())),
        HarnessKind::Kimi => {
            query_kimi(&source_home, &environment)
                .await
                .map(|(windows, extra)| ProfileQuota {
                    profile_id: profile_id.clone(),
                    harness,
                    windows,
                    extra,
                    error: None,
                    refreshed_at_epoch_seconds,
                })
        }
        // Grok Build publishes no HTTP quota endpoint. Its own usage view polls
        // an ACP billing extension, and so does Hel.
        HarnessKind::Grok => {
            grok_usage::query(executable, source_home.clone(), cwd, environment)
                .await
                .map(|report| ProfileQuota {
                    profile_id: profile_id.clone(),
                    harness,
                    windows: vec![QuotaWindow {
                        label: report.period_label.clone(),
                        remaining_percent: Some(report.remaining_percent()),
                        // Grok Build reports a share of the allowance, not the
                        // credit amounts behind it.
                        used: None,
                        limit: None,
                        resets: report
                            .resets_at
                            .and_then(crate::usage_format::format_reset_local_seconds),
                        resets_at_epoch_seconds: report.resets_at,
                    }],
                    extra: report.subscription_tier,
                    error: None,
                    refreshed_at_epoch_seconds,
                })
                .map_err(|error| anyhow::anyhow!(error.to_string()))
        }
    };
    let report = result.unwrap_or_else(|error| ProfileQuota {
        profile_id,
        harness,
        windows: Vec::new(),
        extra: None,
        error: Some(error.to_string()),
        refreshed_at_epoch_seconds,
    });
    (report, codex_client)
}

async fn query_kimi(
    home: &Path,
    environment: &HashMap<String, String>,
) -> Result<(Vec<QuotaWindow>, Option<String>)> {
    let base = environment
        .get("KIMI_CODE_BASE_URL")
        .map(String::as_str)
        .unwrap_or("https://api.kimi.com/coding/v1")
        .trim_end_matches('/');
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build Kimi quota client")?;
    let credentials_path = home.join("credentials/kimi-code.json");
    let usage_url = format!("{base}/usages");
    let response = fetch_bearer_with_auth_retry(&client, &usage_url, |force, rejected_token| {
        ensure_fresh_kimi_token(
            &client,
            home,
            &credentials_path,
            environment,
            force,
            rejected_token,
        )
    })
    .await?;
    if !response.status().is_success() {
        bail!("Kimi Code quota returned HTTP {}", response.status());
    }
    let payload: Value = response.json().await.context("decode Kimi Code quota")?;
    Ok(parse_kimi_usage(&payload))
}

const KIMI_OAUTH_CLIENT_ID: &str = "17e5f671-d194-4dfb-9706-5516cb48c098";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct KimiCredentials {
    #[serde(alias = "accessToken")]
    access_token: String,
    #[serde(default, alias = "refreshToken")]
    refresh_token: String,
    #[serde(default, alias = "expiresAt")]
    expires_at: i64,
    #[serde(default)]
    scope: String,
    #[serde(default, alias = "tokenType")]
    token_type: String,
    #[serde(default, alias = "expiresIn")]
    expires_in: i64,
}

impl KimiCredentials {
    fn needs_refresh(&self) -> bool {
        if self.expires_at == 0 {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let threshold = 300.max(self.expires_in / 2);
        self.expires_at - now < threshold
    }
}

async fn read_kimi_credentials(path: &Path) -> Result<KimiCredentials> {
    let bytes = tokio::fs::read(path)
        .await
        .context("Kimi Code credentials are unavailable")?;
    let credentials: KimiCredentials =
        serde_json::from_slice(&bytes).context("Kimi Code credentials are invalid")?;
    if credentials.access_token.is_empty() {
        bail!("Kimi Code access token is missing");
    }
    Ok(credentials)
}

async fn fetch_bearer_with_auth_retry<F, Fut>(
    client: &reqwest::Client,
    url: &str,
    mut authenticate: F,
) -> Result<reqwest::Response>
where
    F: FnMut(bool, Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<String>>,
{
    let token = authenticate(false, None).await?;
    let response = client
        .get(url)
        .bearer_auth(&token)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .context("query quota")?;
    if response.status() != reqwest::StatusCode::UNAUTHORIZED {
        return Ok(response);
    }

    let refreshed = authenticate(true, Some(token)).await?;
    client
        .get(url)
        .bearer_auth(refreshed)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .context("retry quota after authentication refresh")
}

async fn ensure_fresh_kimi_token(
    client: &reqwest::Client,
    home: &Path,
    credentials_path: &Path,
    environment: &HashMap<String, String>,
    force: bool,
    rejected_token: Option<String>,
) -> Result<String> {
    let initial = read_kimi_credentials(credentials_path).await?;
    if !force && !initial.needs_refresh() {
        return Ok(initial.access_token);
    }

    let refresh_lock = KimiRefreshLock::acquire(home).await?;
    let active = read_kimi_credentials(credentials_path).await?;
    let changed_while_waiting = active.access_token != initial.access_token
        || active.refresh_token != initial.refresh_token
        || active.expires_at != initial.expires_at;
    if (!force && !active.needs_refresh())
        || (force
            && (changed_while_waiting
                || rejected_token.is_some_and(|token| token != active.access_token)))
    {
        refresh_lock.release().await;
        return Ok(active.access_token);
    }
    if active.refresh_token.is_empty() {
        refresh_lock.release().await;
        bail!("Kimi Code refresh token is missing; run `kimi login`");
    }

    let oauth_host = environment
        .get("KIMI_CODE_OAUTH_HOST")
        .or_else(|| environment.get("KIMI_OAUTH_HOST"))
        .map(String::as_str)
        .unwrap_or("https://auth.kimi.com")
        .trim_end_matches('/');
    let response = client
        .post(format!("{oauth_host}/api/oauth/token"))
        .header(reqwest::header::ACCEPT, "application/json")
        .form(&[
            ("client_id", KIMI_OAUTH_CLIENT_ID),
            ("grant_type", "refresh_token"),
            ("refresh_token", active.refresh_token.as_str()),
        ])
        .send()
        .await
        .context("refresh Kimi Code access token")?;
    if !response.status().is_success() {
        let status = response.status();
        if matches!(
            status,
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let recovery = read_kimi_credentials(credentials_path).await?;
            if recovery.refresh_token != active.refresh_token && !recovery.access_token.is_empty() {
                refresh_lock.release().await;
                return Ok(recovery.access_token);
            }
        }
        refresh_lock.release().await;
        bail!("Kimi Code token refresh returned HTTP {status}");
    }

    let payload: Value = response
        .json()
        .await
        .context("decode Kimi Code token refresh")?;
    let access_token = required_string(&payload, "access_token", "Kimi Code token refresh")?;
    let refresh_token = required_string(&payload, "refresh_token", "Kimi Code token refresh")?;
    let expires_in = payload
        .get("expires_in")
        .and_then(value_i64)
        .filter(|value| *value > 0)
        .context("Kimi Code token refresh is missing expires_in")?;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let refreshed = KimiCredentials {
        access_token: access_token.to_string(),
        refresh_token: refresh_token.to_string(),
        expires_at: now + expires_in,
        scope: payload
            .get("scope")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        token_type: payload
            .get("token_type")
            .and_then(Value::as_str)
            .unwrap_or("Bearer")
            .to_string(),
        expires_in,
    };
    let mut body = serde_json::to_vec_pretty(&refreshed)?;
    body.push(b'\n');
    crate::hel_config::atomic_write(credentials_path, &body)
        .context("save refreshed Kimi Code credentials")?;
    refresh_lock.release().await;
    Ok(refreshed.access_token)
}

fn required_string<'a>(payload: &'a Value, key: &str, context: &str) -> Result<&'a str> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{context} is missing {key}"))
}

struct KimiRefreshLock {
    path: std::path::PathBuf,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

impl KimiRefreshLock {
    async fn acquire(home: &Path) -> Result<Self> {
        let oauth_dir = home.join("oauth");
        tokio::fs::create_dir_all(&oauth_dir)
            .await
            .context("prepare Kimi Code OAuth lock")?;
        let sentinel = oauth_dir.join("kimi-code");
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&sentinel)
            .await
            .context("prepare Kimi Code OAuth lock sentinel")?;
        let path = oauth_dir.join("kimi-code.lock");
        for _ in 0..120 {
            match tokio::fs::create_dir(&path).await {
                Ok(()) => {
                    let heartbeat_path = path.clone();
                    let heartbeat = tokio::spawn(async move {
                        loop {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            if let Ok(directory) = std::fs::File::open(&heartbeat_path) {
                                let _ = directory.set_times(
                                    std::fs::FileTimes::new().set_modified(SystemTime::now()),
                                );
                            }
                        }
                    });
                    return Ok(Self {
                        path,
                        heartbeat: Some(heartbeat),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(error) => return Err(error).context("acquire Kimi Code OAuth refresh lock"),
            }
        }
        bail!("timed out waiting for Kimi Code OAuth refresh lock")
    }

    async fn release(mut self) {
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.abort();
        }
        if let Err(error) = tokio::fs::remove_dir(&self.path).await {
            tracing::warn!(path = %self.path.display(), %error, "release Kimi Code OAuth refresh lock");
        }
    }
}

impl Drop for KimiRefreshLock {
    fn drop(&mut self) {
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.abort();
        }
        let _ = std::fs::remove_dir(&self.path);
    }
}

fn parse_kimi_usage(payload: &Value) -> (Vec<QuotaWindow>, Option<String>) {
    let mut windows = Vec::new();
    if let Some(summary) = payload.get("usage")
        && let Some(window) = parse_kimi_window(summary, "Weekly limit")
    {
        windows.push(window);
    }
    if let Some(limits) = payload.get("limits").and_then(Value::as_array) {
        for (index, item) in limits.iter().enumerate() {
            let detail = item.get("detail").unwrap_or(item);
            if let Some(window) = parse_kimi_window(detail, &format!("Limit #{}", index + 1)) {
                windows.push(window);
            }
        }
    }
    let extra = payload
        .pointer("/boosterWallet/balance/amountLeft")
        .and_then(value_i64)
        .map(|value| format!("booster {} remaining", value / 1_000_000));
    (windows, extra)
}

fn parse_kimi_window(value: &Value, fallback: &str) -> Option<QuotaWindow> {
    let limit = value.get("limit").and_then(value_i64);
    let used = value.get("used").and_then(value_i64).or_else(|| {
        let remaining = value.get("remaining").and_then(value_i64)?;
        Some(limit? - remaining)
    });
    if used.is_none() && limit.is_none() {
        return None;
    }
    let provider_label = value
        .get("name")
        .or_else(|| value.get("title"))
        .and_then(Value::as_str)
        .unwrap_or(fallback);
    let label = if provider_label.to_ascii_lowercase().contains("week") {
        "Week".to_string()
    } else if provider_label.to_ascii_lowercase().contains("5h") || fallback.starts_with("Limit #")
    {
        "5H".to_string()
    } else {
        provider_label.to_string()
    };
    let reset_value = ["resetAt", "reset_at", "resetTime", "reset_time"]
        .iter()
        .find_map(|key| value.get(*key));
    let resets = reset_value.and_then(normalize_kimi_reset);
    let resets_at_epoch_seconds = reset_value.and_then(kimi_reset_epoch_seconds);
    let remaining_percent = match (used, limit) {
        (Some(used), Some(limit)) if limit > 0 => {
            Some((100 - used.clamp(0, limit) * 100 / limit) as u8)
        }
        _ => None,
    };
    Some(QuotaWindow {
        label,
        remaining_percent,
        used,
        limit,
        resets,
        resets_at_epoch_seconds,
    })
}

fn value_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.parse::<i64>().ok())
}

fn normalize_kimi_reset(value: &Value) -> Option<String> {
    value
        .as_f64()
        .and_then(crate::usage_format::format_reset_local)
        .or_else(|| {
            value
                .as_str()
                .and_then(crate::usage_format::normalize_reset_text)
        })
}

fn kimi_reset_epoch_seconds(value: &Value) -> Option<i64> {
    value
        .as_f64()
        .map(|epoch| {
            if epoch.abs() >= 1_000_000_000_000.0 {
                (epoch / 1000.0).trunc() as i64
            } else {
                epoch.trunc() as i64
            }
        })
        .or_else(|| {
            value
                .as_str()
                .and_then(crate::usage_format::normalize_reset_epoch_seconds)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use std::sync::{Arc, Mutex};

    #[test]
    fn parses_kimi_summary_limits_and_booster_without_credentials() {
        let payload = serde_json::json!({
            "usage": {"name":"Weekly", "used":40, "limit":1000, "resetAt":"tomorrow"},
            "limits": [{"detail":{"remaining":"90", "limit":"100", "name":"5h"}}],
            "boosterWallet": {"balance":{"amountLeft":42000000}}
        });
        let (windows, extra) = parse_kimi_usage(&payload);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].used, Some(40));
        assert_eq!(windows[1].used, Some(10));
        assert_eq!(windows[0].label, "Week");
        assert_eq!(windows[0].remaining_percent, Some(96));
        assert_eq!(windows[1].label, "5H");
        assert_eq!(windows[1].remaining_percent, Some(90));
        assert_eq!(extra.as_deref(), Some("booster 42 remaining"));
    }

    #[test]
    fn compact_includes_reset_and_error_states() {
        let report = ProfileQuota {
            profile_id: "codex-1".into(),
            harness: HarnessKind::Codex,
            windows: vec![QuotaWindow {
                label: "5H".into(),
                remaining_percent: Some(70),
                used: None,
                limit: None,
                resets: Some("10:00 Jun 17".into()),
                resets_at_epoch_seconds: Some(14_400),
            }],
            extra: None,
            error: None,
            refreshed_at_epoch_seconds: 0,
        };
        assert!(report.compact().contains("70% left"));
        assert!(report.compact().contains("resets 10:00 Jun 17"));
    }

    #[test]
    fn compact_displays_a_shared_reset_once() {
        let report = ProfileQuota {
            profile_id: "codex-1".into(),
            harness: HarnessKind::Codex,
            windows: vec![
                QuotaWindow {
                    label: "5H".into(),
                    remaining_percent: Some(70),
                    used: None,
                    limit: None,
                    resets: Some("10:00 Jun 17".into()),
                    resets_at_epoch_seconds: Some(14_400),
                },
                QuotaWindow {
                    label: "Week".into(),
                    remaining_percent: Some(55),
                    used: None,
                    limit: None,
                    resets: Some("10:00 Jun 17".into()),
                    resets_at_epoch_seconds: Some(14_400),
                },
            ],
            extra: None,
            error: None,
            refreshed_at_epoch_seconds: 0,
        };
        assert_eq!(
            report.compact(),
            "5H 70% left, resets 10:00 Jun 17 · Week 55% left"
        );
    }

    #[test]
    fn compact_hides_claude_short_window_when_week_is_exhausted() {
        let report = ProfileQuota {
            profile_id: "claude".into(),
            harness: HarnessKind::Claude,
            windows: vec![
                QuotaWindow {
                    label: "5H".into(),
                    remaining_percent: Some(100),
                    used: None,
                    limit: None,
                    resets: None,
                    resets_at_epoch_seconds: None,
                },
                QuotaWindow {
                    label: "Week".into(),
                    remaining_percent: Some(0),
                    used: None,
                    limit: None,
                    resets: Some("03:59 Aug 14".into()),
                    resets_at_epoch_seconds: None,
                },
            ],
            extra: None,
            error: None,
            refreshed_at_epoch_seconds: 0,
        };

        assert_eq!(report.compact(), "Week 0% left, resets 03:59 Aug 14");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_grok_profile_reports_its_billing_period_as_one_quota_window() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("grok");
        std::fs::write(
            &executable,
            "#!/bin/sh\nwhile IFS= read -r line; do\n  case \"$line\" in\n    *initialize*) printf '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\\n' ;;\n    *billing*) printf '{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"config\":{\"creditUsagePercent\":25.0,\"currentPeriod\":{\"type\":\"USAGE_PERIOD_TYPE_WEEKLY\",\"end\":\"2026-08-18T05:22:07+00:00\"}},\"subscription_tier\":\"X Premium+\"}}\\n' ;;\n  esac\ndone\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();

        let (report, _) = refresh_profile(
            QuotaRefreshRequest {
                profile_id: "grok".into(),
                harness: HarnessKind::Grok,
                source_home: directory.path().to_path_buf(),
                executable: Some(executable),
                environment: BTreeMap::new(),
                cwd: directory.path().to_path_buf(),
            },
            None,
        )
        .await;

        assert_eq!(report.error, None, "{:?}", report.error);
        // One long window and no short one: Grok Build has no 5-hour budget.
        assert_eq!(report.windows.len(), 1);
        assert_eq!(report.weekly_window().unwrap().remaining_percent, Some(75));
        assert_eq!(report.five_hour_window(), None);
        assert_eq!(report.extra.as_deref(), Some("X Premium+"));
        assert!(report.compact().starts_with("Week 75% left, resets "));
        assert!(report.compact().ends_with("X Premium+"));
    }

    #[tokio::test]
    async fn an_unreachable_grok_reports_the_failure_instead_of_a_zero_reading() {
        let directory = tempfile::tempdir().unwrap();

        let (report, _) = refresh_profile(
            QuotaRefreshRequest {
                profile_id: "grok".into(),
                harness: HarnessKind::Grok,
                source_home: directory.path().to_path_buf(),
                executable: Some(directory.path().join("no-such-grok")),
                environment: BTreeMap::new(),
                cwd: directory.path().to_path_buf(),
            },
            None,
        )
        .await;

        assert!(report.windows.is_empty());
        assert_eq!(
            report.error.as_deref(),
            Some("Grok Build executable not found")
        );
    }

    #[test]
    fn a_monthly_window_shares_the_long_window_column_with_a_weekly_one() {
        for label in ["Week", "Month"] {
            let report = ProfileQuota {
                profile_id: "grok".into(),
                harness: HarnessKind::Grok,
                windows: vec![QuotaWindow {
                    label: label.into(),
                    remaining_percent: Some(60),
                    used: None,
                    limit: None,
                    resets: None,
                    resets_at_epoch_seconds: None,
                }],
                extra: None,
                error: None,
                refreshed_at_epoch_seconds: 0,
            };

            assert!(report.weekly_window().is_some(), "{label}");
            assert_eq!(report.compact(), format!("{label} 60% left"));
        }
    }

    #[test]
    fn kimi_uses_percent_left_and_hides_a_short_window_on_sustainable_pace() {
        let report = ProfileQuota {
            profile_id: "kimi".into(),
            harness: HarnessKind::Kimi,
            windows: vec![
                QuotaWindow {
                    label: "Week".into(),
                    remaining_percent: Some(94),
                    used: Some(6),
                    limit: Some(100),
                    resets: Some("12:22 Aug 18".into()),
                    resets_at_epoch_seconds: Some(604_800),
                },
                QuotaWindow {
                    label: "5H".into(),
                    remaining_percent: Some(97),
                    used: Some(3),
                    limit: Some(100),
                    resets: Some("10:22 Aug 13".into()),
                    resets_at_epoch_seconds: Some(18_000),
                },
            ],
            extra: None,
            error: None,
            refreshed_at_epoch_seconds: 3_600,
        };

        assert_eq!(report.compact(), "Week 94% left, resets 12:22 Aug 18");
    }

    #[test]
    fn short_window_is_shown_only_when_burn_rate_projects_early_exhaustion() {
        let window = QuotaWindow {
            label: "5H".into(),
            remaining_percent: Some(70),
            used: None,
            limit: None,
            resets: Some("later".into()),
            resets_at_epoch_seconds: Some(14_400),
        };
        assert!(projects_exhaustion_before_reset(&window, 0));

        let sustainable = QuotaWindow {
            remaining_percent: Some(80),
            ..window
        };
        assert!(!projects_exhaustion_before_reset(&sustainable, 0));
    }

    #[derive(Clone, Default)]
    struct KimiServerState {
        refresh_forms: Arc<Mutex<Vec<String>>>,
    }

    async fn test_kimi_usage(headers: HeaderMap) -> (StatusCode, Json<Value>) {
        let accepted = headers
            .get(reqwest::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            == Some("Bearer fresh-access");
        if accepted {
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "usage": {"name": "Weekly", "used": 1, "limit": 100}
                })),
            )
        } else {
            (StatusCode::UNAUTHORIZED, Json(serde_json::json!({})))
        }
    }

    async fn test_kimi_refresh(State(state): State<KimiServerState>, body: Bytes) -> Json<Value> {
        state
            .refresh_forms
            .lock()
            .unwrap()
            .push(String::from_utf8(body.to_vec()).unwrap());
        Json(serde_json::json!({
            "access_token": "fresh-access",
            "refresh_token": "fresh-refresh",
            "expires_in": 900,
            "scope": "kimi-code",
            "token_type": "Bearer"
        }))
    }

    #[tokio::test]
    async fn kimi_quota_refreshes_after_unauthorized_and_retries() {
        let state = KimiServerState::default();
        let app = Router::new()
            .route("/coding/v1/usages", get(test_kimi_usage))
            .route("/api/oauth/token", post(test_kimi_refresh))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let home = tempfile::tempdir().unwrap();
        let credentials_path = home.path().join("credentials/kimi-code.json");
        tokio::fs::create_dir_all(credentials_path.parent().unwrap())
            .await
            .unwrap();
        let future_expiry = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3_600;
        tokio::fs::write(
            &credentials_path,
            serde_json::to_vec(&serde_json::json!({
                "access_token": "rejected-access",
                "refresh_token": "old-refresh",
                "expires_at": future_expiry,
                "scope": "kimi-code",
                "token_type": "Bearer",
                "expires_in": 900
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        let endpoint = format!("http://{address}");
        let environment = HashMap::from([
            ("KIMI_CODE_BASE_URL".into(), format!("{endpoint}/coding/v1")),
            ("KIMI_CODE_OAUTH_HOST".into(), endpoint),
        ]);

        let (windows, _) = query_kimi(home.path(), &environment).await.unwrap();

        assert_eq!(windows[0].used, Some(1));
        let form = {
            let forms = state.refresh_forms.lock().unwrap();
            assert_eq!(forms.len(), 1);
            url::form_urlencoded::parse(forms[0].as_bytes())
                .into_owned()
                .collect::<HashMap<_, _>>()
        };
        assert_eq!(
            form.get("grant_type").map(String::as_str),
            Some("refresh_token")
        );
        assert_eq!(
            form.get("refresh_token").map(String::as_str),
            Some("old-refresh")
        );
        let saved = read_kimi_credentials(&credentials_path).await.unwrap();
        assert_eq!(saved.access_token, "fresh-access");
        assert_eq!(saved.refresh_token, "fresh-refresh");
        assert!(!home.path().join("oauth/kimi-code.lock").exists());
        server.abort();
    }
}
