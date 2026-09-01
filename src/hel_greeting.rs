//! Context-aware dashboard greetings.

use chrono::{Datelike, Local, Timelike, Weekday};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RepositoryGreetingFacts {
    pub clean: bool,
    pub dirty: bool,
    pub ahead: bool,
    pub behind: bool,
    pub diverged_or_conflicted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GreetingFacts {
    pub first_name: Option<String>,
    pub returning: bool,
    pub profile_count: usize,
    pub active_sessions: usize,
    pub stopped_sessions: usize,
    pub active_turn_profile: Option<String>,
    pub queued_prompts: bool,
    pub raw_localhost_active: bool,
    pub container_active: bool,
    pub remote_active: bool,
    pub remote_turn_active: bool,
    pub low_quota: bool,
    /// Profile that is under 10% quota remaining while another profile is
    /// over 50%. Compute it with [`token_earner`].
    pub token_earner: Option<String>,
    pub high_context_usage: bool,
    pub repository: Option<RepositoryGreetingFacts>,
    pub latest_build_passed: Option<bool>,
    pub ci_passed: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Group {
    Always,
    Time,
    State,
    Repository,
    Target,
    Usage,
}

#[derive(Debug, Clone, Copy)]
struct Greeting {
    text: &'static str,
    group: Group,
    eligible: fn(&GreetingFacts, &Clock) -> bool,
}

#[derive(Debug, Clone, Copy)]
struct Clock {
    hour: u32,
    weekday: Weekday,
}

/// The profile working hardest for its keep: one under 10% quota remaining
/// while another sits above 50%. `None` unless both extremes exist.
pub fn token_earner<'a>(quotas: impl IntoIterator<Item = (&'a str, u8)> + Clone) -> Option<String> {
    let rested = quotas
        .clone()
        .into_iter()
        .any(|(_, remaining_percent)| remaining_percent > 50);
    rested
        .then(|| {
            quotas
                .into_iter()
                .filter(|(_, remaining_percent)| *remaining_percent < 10)
                .min_by_key(|(_, remaining_percent)| *remaining_percent)
                .map(|(profile_id, _)| profile_id.to_owned())
        })
        .flatten()
}

pub fn select(facts: &GreetingFacts, seed: u64) -> String {
    let now = Local::now();
    select_at(
        facts,
        Clock {
            hour: now.hour(),
            weekday: now.weekday(),
        },
        seed,
    )
}

fn select_at(facts: &GreetingFacts, clock: Clock, seed: u64) -> String {
    let groups = [
        Group::Usage,
        Group::Target,
        Group::Repository,
        Group::State,
        Group::Time,
        Group::Always,
    ]
    .into_iter()
    .filter(|group| {
        GREETINGS
            .iter()
            .any(|greeting| greeting.group == *group && (greeting.eligible)(facts, &clock))
    })
    .collect::<Vec<_>>();
    let group = groups[seed as usize % groups.len()];
    let choices = GREETINGS
        .iter()
        .filter(|greeting| greeting.group == group && (greeting.eligible)(facts, &clock))
        .collect::<Vec<_>>();
    expand(
        choices[(seed as usize / groups.len()) % choices.len()].text,
        facts,
    )
}

fn expand(template: &str, facts: &GreetingFacts) -> String {
    template
        .replace(
            "$firstname",
            facts.first_name.as_deref().unwrap_or("friend"),
        )
        .replace("$stopped_count", &facts.stopped_sessions.to_string())
        .replace(
            "$profile_name",
            facts.active_turn_profile.as_deref().unwrap_or("An agent"),
        )
        .replace(
            "$token_earner",
            facts.token_earner.as_deref().unwrap_or("An agent"),
        )
}

const fn yes(_: &GreetingFacts, _: &Clock) -> bool {
    true
}

fn after_nine(_: &GreetingFacts, clock: &Clock) -> bool {
    clock.hour >= 21
}
fn morning(_: &GreetingFacts, clock: &Clock) -> bool {
    (5..12).contains(&clock.hour)
}
fn afternoon(_: &GreetingFacts, clock: &Clock) -> bool {
    (12..17).contains(&clock.hour)
}
fn evening(_: &GreetingFacts, clock: &Clock) -> bool {
    (17..21).contains(&clock.hour)
}
fn friday_evening(_: &GreetingFacts, clock: &Clock) -> bool {
    clock.weekday == Weekday::Fri && clock.hour >= 17
}
fn weekday_morning(_: &GreetingFacts, clock: &Clock) -> bool {
    !matches!(clock.weekday, Weekday::Sat | Weekday::Sun) && (5..12).contains(&clock.hour)
}
fn midnight(_: &GreetingFacts, clock: &Clock) -> bool {
    clock.hour < 3
}
fn returning(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.returning
}
fn multiple_profiles(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.profile_count > 1
}
fn multiple_active(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.active_sessions > 1
}
fn no_active(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.active_sessions == 0
}
fn active_turn(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.active_turn_profile.is_some()
}
fn queued(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.queued_prompts
}
fn multiple_stopped(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.stopped_sessions > 1
}
fn multiple_sessions(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.active_sessions + facts.stopped_sessions > 1
}
fn clean(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.repository.is_some_and(|repository| repository.clean)
}
fn dirty(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.repository.is_some_and(|repository| repository.dirty)
}
fn ahead(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.repository.is_some_and(|repository| repository.ahead)
}
fn behind(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.repository.is_some_and(|repository| repository.behind)
}
fn diverged(facts: &GreetingFacts, _: &Clock) -> bool {
    facts
        .repository
        .is_some_and(|repository| repository.diverged_or_conflicted)
}
fn build_passed(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.latest_build_passed == Some(true)
}
fn build_failed(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.latest_build_passed == Some(false)
}
fn ci_passed(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.ci_passed == Some(true)
}
fn ci_failed(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.ci_passed == Some(false)
}
fn raw_localhost(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.raw_localhost_active
}
fn remote(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.remote_active
}
fn remote_turn(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.remote_turn_active
}
fn long_night(facts: &GreetingFacts, clock: &Clock) -> bool {
    facts.high_context_usage && clock.hour >= 18
}
fn low_quota(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.low_quota
}
fn earning_tokens(facts: &GreetingFacts, _: &Clock) -> bool {
    facts.profile_count > 1 && facts.token_earner.is_some()
}

const fn greeting(
    text: &'static str,
    group: Group,
    eligible: fn(&GreetingFacts, &Clock) -> bool,
) -> Greeting {
    Greeting {
        text,
        group,
        eligible,
    }
}

const GREETINGS: [Greeting; 45] = [
    greeting("Welcome to Mjolnir, $firstname", Group::Always, yes),
    greeting(
        "Abandon boilerplate, all ye who enter here",
        Group::Always,
        yes,
    ),
    greeting("Thunderstruck, $firstname", Group::Always, yes),
    greeting("Take a hammer to the boilerplate", Group::Always, yes),
    greeting("Here be daemons", Group::Always, yes),
    greeting("Time to forge something, $firstname", Group::Always, yes),
    greeting("The devil is in the diff", Group::Always, yes),
    greeting(
        "The code won't write itself, but the agents might",
        Group::Always,
        yes,
    ),
    greeting("One prompt closer to done", Group::Always, yes),
    greeting(
        "It's a good night to write code, $firstname",
        Group::Time,
        after_nine,
    ),
    greeting(
        "Good morning, $firstname. The agents are stirring",
        Group::Time,
        morning,
    ),
    greeting("Good afternoon, $firstname", Group::Time, afternoon),
    greeting("Good evening, $firstname", Group::Time, evening),
    greeting(
        "Bring the thunder, $firstname",
        Group::Time,
        friday_evening,
    ),
    greeting("Give 'em the hammer, $firstname", Group::Time, weekday_morning),
    greeting(
        "Midnight at the forge. Perfect coding weather",
        Group::Time,
        midnight,
    ),
    greeting(
        "Welcome back, $firstname. The forge kept the fires burning",
        Group::State,
        returning,
    ),
    greeting(
        "Many hands, one hammer",
        Group::State,
        multiple_profiles,
    ),
    greeting("No pitchforks. Just forks", Group::State, multiple_active),
    greeting(
        "Mjolnir awaits your command, $firstname",
        Group::State,
        no_active,
    ),
    greeting("Mjolnir is busy on your behalf", Group::State, active_turn),
    greeting("The agents are restless, $firstname", Group::State, queued),
    greeting(
        "$stopped_count agents sleep beneath the mountain",
        Group::State,
        multiple_stopped,
    ),
    greeting(
        "Many agents, one dashboard",
        Group::State,
        multiple_sessions,
    ),
    greeting(
        "$profile_name has entered the forge",
        Group::State,
        active_turn,
    ),
    greeting(
        "$profile_name gazes into the diff",
        Group::State,
        active_turn,
    ),
    greeting(
        "The timelines have split, $firstname",
        Group::Repository,
        diverged,
    ),
    greeting(
        "The road to Valhalla is paved with good intentions",
        Group::Repository,
        clean,
    ),
    greeting(
        "The road to Valhalla is paved with uncommitted intentions",
        Group::Repository,
        dirty,
    ),
    greeting("It's a cold day in Niflheim", Group::Repository, ci_passed),
    greeting(
        "The build passed. Miracles do happen",
        Group::Repository,
        build_passed,
    ),
    greeting(
        "The build failed. Into the fire we go",
        Group::Repository,
        build_failed,
    ),
    greeting(
        "CI is red. The forge has seen worse",
        Group::Repository,
        ci_failed,
    ),
    greeting(
        "CI is green. Suspicious, but welcome",
        Group::Repository,
        ci_passed,
    ),
    greeting("Clean tree, clear mind", Group::Repository, clean),
    greeting(
        "There are changes in the mortal realm",
        Group::Repository,
        dirty,
    ),
    greeting(
        "Fresh commits have arrived at the gates",
        Group::Repository,
        ahead,
    ),
    greeting(
        "The outside world has new commits",
        Group::Repository,
        behind,
    ),
    greeting(
        "The portal to localhost is open",
        Group::Target,
        raw_localhost,
    ),
    greeting(
        "The agent walks among us, $firstname",
        Group::Target,
        raw_localhost,
    ),
    greeting(
        "There's a remote agent at the other end of the wire",
        Group::Target,
        remote,
    ),
    greeting(
        "Across the network, something is thinking",
        Group::Target,
        remote_turn,
    ),
    greeting(
        "The context is long, but the night is young",
        Group::Usage,
        long_night,
    ),
    greeting(
        "Mind the quota, $firstname. Even Mjolnir has limits",
        Group::Usage,
        low_quota,
    ),
    greeting(
        "$token_earner is earning its tokens",
        Group::Usage,
        earning_tokens,
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_has_forty_five_unpunctuated_greetings_and_required_first_entry() {
        assert_eq!(GREETINGS.len(), 45);
        assert_eq!(GREETINGS[0].text, "Welcome to Mjolnir, $firstname");
        assert!(
            GREETINGS
                .iter()
                .all(|greeting| !greeting.text.ends_with('.'))
        );
    }

    #[test]
    fn selector_uses_eligible_groups_and_expands_metadata() {
        let facts = GreetingFacts {
            first_name: Some("Ada".into()),
            stopped_sessions: 3,
            ..GreetingFacts::default()
        };
        let clock = Clock {
            hour: 14,
            weekday: Weekday::Tue,
        };
        assert_eq!(
            select_at(&facts, clock, 3),
            "3 agents sleep beneath the mountain"
        );

        let facts = GreetingFacts {
            first_name: Some("Ada".into()),
            low_quota: true,
            raw_localhost_active: true,
            ..GreetingFacts::default()
        };
        assert_eq!(
            select_at(&facts, clock, 0),
            "Mind the quota, Ada. Even Mjolnir has limits"
        );
    }

    #[test]
    fn token_earner_needs_one_profile_under_ten_and_another_over_fifty() {
        assert_eq!(
            token_earner([("codex", 4u8), ("claude", 80u8)]),
            Some("codex".to_owned())
        );
        // The most-drained profile wins when several are under 10%.
        assert_eq!(
            token_earner([("codex", 4u8), ("kimi", 2u8), ("claude", 80u8)]),
            Some("kimi".to_owned())
        );
        assert_eq!(token_earner([("codex", 4u8), ("claude", 40u8)]), None);
        assert_eq!(token_earner([("codex", 30u8), ("claude", 80u8)]), None);
        assert_eq!(token_earner([]), None);
    }

    #[test]
    fn the_token_earner_greeting_needs_multiple_profiles_and_names_the_drained_one() {
        let clock = Clock {
            hour: 14,
            weekday: Weekday::Tue,
        };
        let facts = GreetingFacts {
            profile_count: 2,
            token_earner: Some("codex-work".into()),
            ..GreetingFacts::default()
        };
        assert_eq!(
            select_at(&facts, clock, 0),
            "codex-work is earning its tokens"
        );

        let single_profile = GreetingFacts {
            profile_count: 1,
            token_earner: Some("codex-work".into()),
            ..GreetingFacts::default()
        };
        assert!(!earning_tokens(&single_profile, &clock));
    }
}
