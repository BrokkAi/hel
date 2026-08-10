//! Shared formatting for provider quota and rate-limit displays.

use chrono::{DateTime, Datelike, Days, FixedOffset, Local, NaiveDate, NaiveTime, TimeZone};

/// Format a Unix reset timestamp as wall-clock time in the machine's local
/// time zone. Accepts seconds or milliseconds and rejects non-finite or
/// out-of-range values.
pub(crate) fn format_reset_local(epoch: f64) -> Option<String> {
    if !epoch.is_finite() {
        return None;
    }
    let seconds = if epoch.abs() >= 1_000_000_000_000.0 {
        (epoch / 1000.0).trunc() as i64
    } else {
        epoch.trunc() as i64
    };
    let local = Local.timestamp_opt(seconds, 0).single()?;
    Some(format_reset_label(local.fixed_offset()))
}

pub(crate) fn format_reset_local_seconds(epoch: i64) -> Option<String> {
    format_reset_local(epoch as f64)
}

/// Normalize a provider's textual reset value to the compact 24-hour form
/// used by the dashboard. A time-only value is the next occurrence of that
/// wall-clock time; Claude Code uses this shape for its five-hour window.
pub(crate) fn normalize_reset_text(value: &str) -> Option<String> {
    normalize_reset_text_at(value, Local::now().fixed_offset())
}

fn normalize_reset_text_at(value: &str, now: DateTime<FixedOffset>) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(epoch) = value.parse::<f64>() {
        return format_reset_local(epoch);
    }
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Some(format_reset_label(
            timestamp.with_timezone(&Local).fixed_offset(),
        ));
    }

    let value = value
        .strip_prefix("at ")
        .unwrap_or(value)
        .split('(')
        .next()
        .unwrap_or(value)
        .trim()
        .trim_end_matches(',');
    let parse_time = |value: &str| {
        let value = value
            .to_ascii_lowercase()
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .collect::<String>();
        let value = ["am", "pm"]
            .into_iter()
            .find_map(|suffix| {
                let hour = value.strip_suffix(suffix)?;
                (!hour.contains(':')).then(|| format!("{hour}:00{suffix}"))
            })
            .unwrap_or(value);
        ["%I:%M%P", "%I%P", "%H:%M"]
            .iter()
            .find_map(|format| NaiveTime::parse_from_str(&value, format).ok())
    };

    if let Some((date, time)) = value.split_once(" at ") {
        let time = parse_time(time.trim())?;
        let date = date.trim().trim_end_matches(',');
        let date = match date.to_ascii_lowercase().as_str() {
            "today" => now.date_naive(),
            "tomorrow" => now.date_naive().checked_add_days(Days::new(1))?,
            _ => NaiveDate::parse_from_str(
                &format!("{} {}", date.replace(',', ""), now.year()),
                "%b %e %Y",
            )
            .ok()?,
        };
        return now
            .timezone()
            .from_local_datetime(&date.and_time(time))
            .single()
            .map(format_reset_label);
    }

    let time = parse_time(value)?;
    let mut date = now.date_naive();
    let mut reset = now
        .timezone()
        .from_local_datetime(&date.and_time(time))
        .single()?;
    if reset <= now {
        date = date.checked_add_days(Days::new(1))?;
        reset = now
            .timezone()
            .from_local_datetime(&date.and_time(time))
            .single()?;
    }
    Some(format_reset_label(reset))
}

/// Render a session's current-turn or idle clock from wall-clock seconds.
pub(crate) fn format_turn_clock(
    now_epoch_seconds: u64,
    current_turn_started_at: Option<u64>,
    last_turn_completed_at: Option<u64>,
) -> String {
    if let Some(started_at) = current_turn_started_at {
        let elapsed = now_epoch_seconds.saturating_sub(started_at);
        return format!("turn {:02}:{:02}", elapsed / 60, elapsed % 60);
    }
    let Some(completed_at) = last_turn_completed_at else {
        return "idle".to_string();
    };
    let elapsed = now_epoch_seconds.saturating_sub(completed_at);
    match elapsed {
        0..=59 => format!("idle {elapsed}s"),
        60..=3_599 => format!("idle {}m", elapsed / 60),
        3_600..=86_399 => format!("idle {}h {}m", elapsed / 3_600, (elapsed % 3_600) / 60),
        _ => format!("idle {}d {}h", elapsed / 86_400, (elapsed % 86_400) / 3_600),
    }
}

/// Pure formatter split from local-zone discovery for deterministic tests.
fn format_reset_label(reset: DateTime<FixedOffset>) -> String {
    reset.format("%H:%M %b %-d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_time_normalization_uses_24_hour_month_day_format() {
        let paris = FixedOffset::east_opt(2 * 3_600).expect("offset");
        let reset = paris
            .with_ymd_and_hms(2026, 6, 17, 16, 49, 0)
            .single()
            .expect("instant");
        assert_eq!(format_reset_label(reset), "16:49 Jun 17");
        assert_eq!(
            normalize_reset_text("Jun 17 at 4:49pm").as_deref(),
            Some("16:49 Jun 17")
        );
    }

    #[test]
    fn reset_timestamp_accepts_seconds_and_milliseconds() {
        let seconds = 1_781_712_540_f64;
        assert_eq!(
            format_reset_local(seconds),
            format_reset_local(seconds * 1_000.0)
        );
        assert_eq!(
            format_reset_local(seconds),
            format_reset_local_seconds(seconds as i64)
        );
    }

    #[test]
    fn time_only_reset_is_rendered_as_the_next_datetime() {
        let zone = FixedOffset::west_opt(5 * 3_600).expect("offset");
        let now = zone
            .with_ymd_and_hms(2026, 8, 10, 14, 0, 0)
            .single()
            .expect("now");
        assert_eq!(
            normalize_reset_text_at("3:30 PM (America/Chicago)", now).as_deref(),
            Some("15:30 Aug 10")
        );
        assert_eq!(
            normalize_reset_text_at("at 1pm (America/Chicago)", now).as_deref(),
            Some("13:00 Aug 11")
        );
    }

    #[test]
    fn turn_clock_formats_running_and_idle_periods() {
        assert_eq!(format_turn_clock(500, Some(375), None), "turn 02:05");
        assert_eq!(format_turn_clock(5_000, None, Some(4_280)), "idle 12m");
        assert_eq!(format_turn_clock(5_000, None, None), "idle");
    }
}
