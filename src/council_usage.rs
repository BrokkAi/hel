use std::collections::{BTreeMap, HashMap};

use agent_client_protocol::schema::v1::{Usage, UsageUpdate};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Thor,
    Loki,
    Eitri,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    Code,
    Explore,
    Review,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub role: Role,
    pub purpose: Option<Purpose>,
    pub usage: Option<Usage>,
    pub update: Option<UsageUpdate>,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RoleUsage {
    pub prompts: u64,
    pub total_tokens: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub thought_tokens: u64,
    pub context_used: u64,
    pub context_size: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub costs: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub thor: RoleUsage,
    pub loki: RoleUsage,
    pub eitri_code: RoleUsage,
    pub eitri_explore: RoleUsage,
    pub eitri_review: RoleUsage,
    #[serde(skip)]
    baselines: HashMap<(Role, Option<Purpose>), Baseline>,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct Baseline {
    session_id: String,
    total_tokens: u64,
    input_tokens: u64,
    output_tokens: u64,
    thought_tokens: u64,
    costs: BTreeMap<String, f64>,
}

fn counter_delta(current: u64, previous: u64) -> u64 {
    current.checked_sub(previous).unwrap_or(current)
}

fn cost_delta(current: f64, previous: f64) -> f64 {
    if current >= previous {
        current - previous
    } else {
        current
    }
}

impl Snapshot {
    pub fn observe(&mut self, record: Record) {
        let lane = (record.role, record.purpose);
        let usage = match lane {
            (Role::Thor, _) => &mut self.thor,
            (Role::Loki, _) => &mut self.loki,
            (Role::Eitri, Some(Purpose::Explore)) => &mut self.eitri_explore,
            (Role::Eitri, Some(Purpose::Review)) => &mut self.eitri_review,
            (Role::Eitri, _) => &mut self.eitri_code,
        };
        usage.prompts += 1;
        let same_session = record.session_id.as_ref().is_some_and(|session_id| {
            self.baselines
                .get(&lane)
                .is_some_and(|baseline| baseline.session_id == *session_id)
        });
        let previous = same_session
            .then(|| self.baselines.get(&lane).cloned())
            .flatten()
            .unwrap_or_default();
        let mut next = record.session_id.as_ref().map(|session_id| {
            if same_session {
                let mut next = previous.clone();
                next.session_id = session_id.clone();
                next
            } else {
                Baseline {
                    session_id: session_id.clone(),
                    ..Baseline::default()
                }
            }
        });
        if let Some(value) = record.usage {
            usage.total_tokens += counter_delta(value.total_tokens, previous.total_tokens);
            usage.input_tokens += counter_delta(value.input_tokens, previous.input_tokens);
            usage.output_tokens += counter_delta(value.output_tokens, previous.output_tokens);
            usage.thought_tokens += counter_delta(
                value.thought_tokens.unwrap_or_default(),
                previous.thought_tokens,
            );
            if let Some(next) = next.as_mut() {
                next.total_tokens = value.total_tokens;
                next.input_tokens = value.input_tokens;
                next.output_tokens = value.output_tokens;
                next.thought_tokens = value.thought_tokens.unwrap_or_default();
            }
        }
        if let Some(update) = record.update {
            usage.context_used = update.used;
            usage.context_size = update.size;
            if let Some(cost) = update.cost {
                let previous_cost = previous
                    .costs
                    .get(&cost.currency)
                    .copied()
                    .unwrap_or_default();
                *usage.costs.entry(cost.currency.clone()).or_default() +=
                    cost_delta(cost.amount, previous_cost);
                if let Some(next) = next.as_mut() {
                    next.costs.insert(cost.currency, cost.amount);
                }
            }
        }
        if let Some(next) = next {
            self.baselines.insert(lane, next);
        }
    }

    pub fn eitri(&self) -> RoleUsage {
        let mut total = self.eitri_code.clone();
        total.prompts += self.eitri_explore.prompts;
        total.total_tokens += self.eitri_explore.total_tokens;
        total.input_tokens += self.eitri_explore.input_tokens;
        total.output_tokens += self.eitri_explore.output_tokens;
        total.thought_tokens += self.eitri_explore.thought_tokens;
        total.context_used += self.eitri_explore.context_used;
        total.context_size += self.eitri_explore.context_size;
        for (currency, amount) in &self.eitri_explore.costs {
            *total.costs.entry(currency.clone()).or_default() += amount;
        }
        total.prompts += self.eitri_review.prompts;
        total.total_tokens += self.eitri_review.total_tokens;
        total.input_tokens += self.eitri_review.input_tokens;
        total.output_tokens += self.eitri_review.output_tokens;
        total.thought_tokens += self.eitri_review.thought_tokens;
        total.context_used += self.eitri_review.context_used;
        total.context_size += self.eitri_review.context_size;
        for (currency, amount) in &self.eitri_review.costs {
            *total.costs.entry(currency.clone()).or_default() += amount;
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eitri_code_and_explore_are_separate_and_aggregate() {
        let mut snapshot = Snapshot::default();
        snapshot.observe(Record {
            role: Role::Eitri,
            purpose: Some(Purpose::Code),
            usage: Some(Usage::new(10, 7, 3)),
            update: None,
            session_id: None,
        });
        snapshot.observe(Record {
            role: Role::Eitri,
            purpose: Some(Purpose::Explore),
            usage: Some(Usage::new(20, 15, 5)),
            update: None,
            session_id: None,
        });
        snapshot.observe(Record {
            role: Role::Eitri,
            purpose: Some(Purpose::Review),
            usage: Some(Usage::new(30, 25, 5)),
            update: None,
            session_id: None,
        });

        assert_eq!(snapshot.eitri_code.total_tokens, 10);
        assert_eq!(snapshot.eitri_explore.total_tokens, 20);
        assert_eq!(snapshot.eitri_review.total_tokens, 30);
        assert_eq!(snapshot.eitri().total_tokens, 60);
    }

    #[test]
    fn cumulative_session_usage_is_added_as_deltas() {
        let mut snapshot = Snapshot::default();
        for total in [100, 140, 140] {
            snapshot.observe(Record {
                role: Role::Loki,
                purpose: None,
                usage: Some(Usage::new(total, total, 0)),
                update: None,
                session_id: Some("loki-1".into()),
            });
        }
        assert_eq!(snapshot.loki.total_tokens, 140);
    }

    #[test]
    fn a_new_session_establishes_a_new_usage_baseline() {
        let mut snapshot = Snapshot::default();
        for (session_id, total) in [("one", 100), ("two", 25)] {
            snapshot.observe(Record {
                role: Role::Loki,
                purpose: None,
                usage: Some(Usage::new(total, total, 0)),
                update: None,
                session_id: Some(session_id.into()),
            });
        }
        assert_eq!(snapshot.loki.total_tokens, 125);
    }
}
