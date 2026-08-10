//! One-pane quota collection for Hel harness profiles.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::claude_usage;
use crate::codex_usage::{self, CodexUsageClient, CodexUsageStatus};
use crate::hel_config::HarnessKind;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuotaWindow {
    pub label: String,
    pub remaining_percent: Option<u8>,
    pub used: Option<i64>,
    pub limit: Option<i64>,
    pub resets: Option<String>,
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
    pub fn compact(&self) -> String {
        if let Some(error) = &self.error {
            return format!("unavailable: {error}");
        }
        let mut seen_resets = BTreeSet::new();
        let mut parts = self
            .windows
            .iter()
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

#[derive(Default)]
pub struct QuotaManager {
    codex_clients: HashMap<String, CodexUsageClient>,
    reports: BTreeMap<String, ProfileQuota>,
}

impl QuotaManager {
    pub fn reports(&self) -> &BTreeMap<String, ProfileQuota> {
        &self.reports
    }

    pub async fn refresh(
        &mut self,
        profile_id: &str,
        harness: HarnessKind,
        source_home: &Path,
        environment: &BTreeMap<String, String>,
        cwd: &Path,
    ) -> ProfileQuota {
        let environment = environment
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        let refreshed_at_epoch_seconds = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let result = match harness {
            HarnessKind::Codex => {
                let mut client = self.codex_clients.remove(profile_id);
                let status =
                    codex_usage::refresh(&mut client, cwd.to_path_buf(), environment).await;
                if let Some(client) = client {
                    self.codex_clients.insert(profile_id.to_string(), client);
                }
                match status {
                    CodexUsageStatus::Available(report) => Ok(ProfileQuota {
                        profile_id: profile_id.to_string(),
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
                            })
                            .collect(),
                        extra: None,
                        error: None,
                        refreshed_at_epoch_seconds,
                    }),
                    CodexUsageStatus::Unavailable(error) => Err(anyhow::anyhow!(error)),
                }
            }
            HarnessKind::Claude => claude_usage::query(cwd.to_path_buf(), environment)
                .await
                .map(|report| ProfileQuota {
                    profile_id: profile_id.to_string(),
                    harness,
                    windows: [
                        report.five_hour.map(|window| ("5H", window)),
                        report.week.map(|window| ("week", window)),
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
                    })
                    .collect(),
                    extra: None,
                    error: None,
                    refreshed_at_epoch_seconds,
                })
                .map_err(|error| anyhow::anyhow!(error.to_string())),
            HarnessKind::Kimi => {
                query_kimi(source_home, &environment)
                    .await
                    .map(|(windows, extra)| ProfileQuota {
                        profile_id: profile_id.to_string(),
                        harness,
                        windows,
                        extra,
                        error: None,
                        refreshed_at_epoch_seconds,
                    })
            }
        };
        let report = result.unwrap_or_else(|error| ProfileQuota {
            profile_id: profile_id.to_string(),
            harness,
            windows: Vec::new(),
            extra: None,
            error: Some(error.to_string()),
            refreshed_at_epoch_seconds,
        });
        self.reports.insert(profile_id.to_string(), report.clone());
        report
    }

    pub async fn shutdown(mut self) {
        for (_, client) in self.codex_clients.drain() {
            client.shutdown().await;
        }
    }
}

async fn query_kimi(
    home: &Path,
    environment: &HashMap<String, String>,
) -> Result<(Vec<QuotaWindow>, Option<String>)> {
    let credentials = tokio::fs::read(home.join("credentials/kimi-code.json"))
        .await
        .context("Kimi Code credentials are unavailable")?;
    let credentials: Value =
        serde_json::from_slice(&credentials).context("Kimi Code credentials are invalid")?;
    let token = credentials
        .get("accessToken")
        .or_else(|| credentials.get("access_token"))
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .context("Kimi Code access token is missing")?;
    let base = environment
        .get("KIMI_CODE_BASE_URL")
        .map(String::as_str)
        .unwrap_or("https://api.kimi.com/coding/v1")
        .trim_end_matches('/');
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build Kimi quota client")?;
    let response = client
        .get(format!("{base}/usages"))
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .context("query Kimi Code quota")?;
    if !response.status().is_success() {
        bail!("Kimi Code quota returned HTTP {}", response.status());
    }
    let payload: Value = response.json().await.context("decode Kimi Code quota")?;
    Ok(parse_kimi_usage(&payload))
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
    let label = value
        .get("name")
        .or_else(|| value.get("title"))
        .and_then(Value::as_str)
        .unwrap_or(fallback)
        .to_string();
    let resets = ["resetAt", "reset_at", "resetTime", "reset_time"]
        .iter()
        .find_map(|key| value.get(*key))
        .and_then(normalize_kimi_reset);
    Some(QuotaWindow {
        label,
        remaining_percent: None,
        used,
        limit,
        resets,
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(extra.as_deref(), Some("booster 42 remaining"));
    }

    #[test]
    fn compact_includes_reset_and_error_states() {
        let report = ProfileQuota {
            profile_id: "codex-1".into(),
            harness: HarnessKind::Codex,
            windows: vec![QuotaWindow {
                label: "5H".into(),
                remaining_percent: Some(80),
                used: None,
                limit: None,
                resets: Some("10:00 Jun 17".into()),
            }],
            extra: None,
            error: None,
            refreshed_at_epoch_seconds: 0,
        };
        assert!(report.compact().contains("80% left"));
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
                    remaining_percent: Some(80),
                    used: None,
                    limit: None,
                    resets: Some("10:00 Jun 17".into()),
                },
                QuotaWindow {
                    label: "week".into(),
                    remaining_percent: Some(55),
                    used: None,
                    limit: None,
                    resets: Some("10:00 Jun 17".into()),
                },
            ],
            extra: None,
            error: None,
            refreshed_at_epoch_seconds: 0,
        };
        assert_eq!(
            report.compact(),
            "5H 80% left, resets 10:00 Jun 17 · week 55% left"
        );
    }
}
