//! Persistent read-only council advisor. Thor and Eitri stream hidden transcript
//! deltas into one long-lived Loki ACP session; Loki reviews them asynchronously
//! at his own pace, dispatching whatever checkpoints have accumulated as soon
//! as the worker is free -- there is no group-size gate, so a busy worker
//! naturally coalesces whatever arrived while it was occupied into its next
//! review round. Advice accumulates in a queue and is pulled by the
//! orchestrators at natural turn boundaries. Loki never interrupts a running
//! target; a plain `pull`/`pull_manual` may wait briefly for the one review
//! already in flight. At an injection point (a Thor turn boundary, or the
//! pre-shutdown drain) `Handle::rendezvous` instead waits -- bounded by
//! `RENDEZVOUS_TIMEOUT` -- for every review already in flight or still
//! sitting unprocessed in the worker's request channel to finish, and
//! returns it all as one digest.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use agent_client_protocol::schema::v1::{HttpHeader, McpServer, McpServerHttp};
use anyhow::{Context, Result, anyhow};
use axum::extract::{Request as HttpRequest, State};
use axum::http::{StatusCode, header::AUTHORIZATION};
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    handler::server::{router::tool::ToolRouter, tool::ToolCallContext, wrapper::Parameters},
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
    tool, tool_router,
    transport::{
        StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_server::session::local::LocalSessionManager,
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use similar::TextDiff;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::acp::{RuntimeAccessMode, RuntimeRoleConfig};
use crate::council::ResolvedRole;
use crate::council_usage::{Record, Role};
use crate::event::{AgentCommandOutcome, CompactTrigger, LokiActivity, LokiIdentity, UiEvent};
use crate::ragnarok::{AgentHandle, Launch, TurnEvent};

const REVIEW_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const MCP_PATH: &str = "/mcp";
const MCP_SERVER_NAME: &str = "mj-loki-advisor";
const PULL_MCP_SERVER_NAME: &str = "mj-loki-pull";
const ADVICE_QUEUE_CAPACITY: usize = 10;
const PULL_WAIT: Duration = Duration::from_secs(10);
/// Hard ceiling on `Handle::rendezvous`'s wait for Loki to finish every
/// review already in flight or still sitting unprocessed in the worker's
/// request channel at an injection point (turn boundaries, the pre-shutdown
/// advice drain). Mirrors `ragnarok::ADVERTISED_COMMAND_TIMEOUT`'s role as
/// the last line of defense between a hung Loki review and a permanently
/// blocked Thor: on timeout the rendezvous logs a warning and returns
/// whatever advice is already queued instead of hanging.
const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(120);
/// Loki's session context, in tokens, above which we request an automatic
/// `/compact`. DeepSeek's server-side prompt cache evicts our retained
/// prefix under memory pressure on roughly 1 in 11 requests; when that
/// happens the *entire* retained context re-bills as a cache miss, which
/// dominates Loki's cost on long runs. Keeping the retained context small
/// caps the size of that re-bill, so we compact far earlier than any real
/// model context limit rather than waiting for something close to it.
/// (Anvil's own `/compact` has no minimum-context skip floor -- see the
/// investigation notes on `run_advertised_command` -- so there is no larger
/// floor this needs to clear.)
const LOKI_COMPACT_THRESHOLD: u64 = 32_000;

#[derive(Debug)]
struct CompactThreshold {
    armed: bool,
    /// Epoch (Loki turn) of the last retry-after-failure rearm, so a
    /// persistently broken compact path gets one extra attempt per turn
    /// instead of hammering the agent on every review round.
    retried_epoch: Option<u64>,
}

impl Default for CompactThreshold {
    fn default() -> Self {
        Self {
            armed: true,
            retried_epoch: None,
        }
    }
}

impl CompactThreshold {
    fn observe(&mut self, used: u64) -> bool {
        if used < LOKI_COMPACT_THRESHOLD {
            self.armed = true;
            return false;
        }
        std::mem::replace(&mut self.armed, false)
    }

    /// Re-arm after an automatic compact came back `Failed` or `Skipped`, so
    /// the next completed review round's `observe` call retries rather than
    /// leaving the threshold permanently disarmed. Returns whether a fresh
    /// retry was granted; `false` means this epoch already used its one
    /// retry and must wait for the next turn (or a genuine usage dip) before
    /// trying again.
    fn rearm_for_retry(&mut self, epoch: u64) -> bool {
        if self.retried_epoch == Some(epoch) {
            return false;
        }
        self.retried_epoch = Some(epoch);
        self.armed = true;
        true
    }
}

fn preferred_compact_trigger(
    requests: &[(CompactTrigger, Option<oneshot::Sender<AgentCommandOutcome>>)],
) -> Option<CompactTrigger> {
    requests
        .iter()
        .map(|(trigger, _)| *trigger)
        .min_by_key(|trigger| match trigger {
            CompactTrigger::Manual => 0,
            CompactTrigger::ThorCompacted => 1,
            CompactTrigger::Loki128k => 2,
        })
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AdviseArgs {
    /// Material advice keyed to exact steps from this review batch.
    advice: Vec<AdviseItem>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AdviseItem {
    step: StepRef,
    note: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StepRef {
    pub god: Target,
    pub ordinal: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewedSpan {
    pub target: Target,
    pub invocation: Option<u64>,
    pub first_step: u64,
    pub last_step: u64,
    pub activities: Vec<String>,
}

impl ReviewedSpan {
    #[cfg(test)]
    pub(crate) fn for_test(target: Target, ordinal: u64) -> Self {
        Self {
            target,
            invocation: None,
            first_step: ordinal,
            last_step: ordinal,
            activities: vec!["test".to_string()],
        }
    }

    fn marker(&self) -> String {
        let actor = match self.invocation {
            Some(invocation) => format!("{} #{invocation}", self.target.label()),
            None => self.target.label().to_string(),
        };
        let steps = if self.first_step == self.last_step {
            format!("step {}", self.first_step)
        } else {
            format!("steps {}-{}", self.first_step, self.last_step)
        };
        format!("{actor} · {steps}: {}", self.activities.join(", "))
    }
}

#[derive(Debug, Clone)]
pub struct Advice {
    pub id: u64,
    pub epoch: u64,
    pub target: Target,
    pub note: String,
    pub span: ReviewedSpan,
    /// Set when a later `advise` submission for the same (god, ordinal) step
    /// replaced this note's text in place while it was still queued (see
    /// `AdviceSlot::accept`), rather than being appended as a new entry.
    /// Rendered as "(updated)" in the advice ledger.
    pub updated: bool,
}

impl Advice {
    #[cfg(test)]
    pub(crate) fn for_test(
        id: u64,
        epoch: u64,
        target: Target,
        note: &str,
        span: ReviewedSpan,
    ) -> Self {
        Self {
            id,
            epoch,
            target,
            note: note.to_string(),
            span,
            updated: false,
        }
    }

    fn deferred_text(&self, current_epoch: u64) -> String {
        let provenance = if self.epoch == current_epoch {
            format!("[reviewed {}]", self.span.marker())
        } else {
            format!("[reviewed in an earlier turn: {}]", self.span.marker())
        };
        format!("{provenance}\n{}", self.note)
    }
}

#[derive(Debug, Default)]
struct AdviceState {
    thor: VecDeque<Advice>,
    eitri: VecDeque<Advice>,
    dropped_thor: u64,
    dropped_eitri: u64,
    thor_cutoff: u64,
    next_advice_id: u64,
    /// Every (epoch, step) reviewed in any batch this session, keyed to the
    /// epoch it was reviewed in, with the span data needed to render it.
    /// Lets `AdviceSlot::accept` validate a step reference against the whole
    /// epoch's reviewed history, not just the current review batch, so Loki
    /// can re-raise an unresolved issue keyed to an older step (see
    /// `AdviceSlot::begin`). Never cleared -- entries from a prior epoch
    /// simply stop matching once the active epoch moves on.
    reviewed_spans: HashMap<(u64, StepRef), ReviewedSpan>,
    /// Notes actually handed to their target since the last time a review
    /// prompt drained this log (see `take_ledger_snapshot`). Recorded in
    /// `drain_for`, the single choke point every delivery path -- automatic
    /// turn-boundary pulls and the manual `pull_advice` tool -- goes through.
    delivered_log: Vec<DeliveredEntry>,
    /// Advise-tool submissions the acceptance gate rejected since the last
    /// ledger drain, so Loki keeps a persistent record of a diagnosis that
    /// would otherwise be silently lost. Recorded in `AdviceSlot::accept`.
    rejected_log: Vec<RejectedEntry>,
}

#[derive(Debug, Clone)]
struct DeliveredEntry {
    id: u64,
    target: Target,
    note: String,
}

#[derive(Debug, Clone)]
struct RejectedEntry {
    /// Which review batch the rejected submission was attempted against, for
    /// correlation with the `review_id` tracing field; "no active review"
    /// when the submission arrived outside any review turn.
    context: String,
    reason: &'static str,
    /// Truncated at render time; kept untruncated here so later truncation
    /// changes do not need to touch the recording site.
    text: String,
}

type SharedAdviceState = Arc<std::sync::Mutex<AdviceState>>;

#[derive(Debug, Clone)]
struct ActiveAdvice {
    id: u64,
    epoch: u64,
    spans: Vec<ReviewedSpan>,
    accepted_steps: HashSet<StepRef>,
    accepted_advice: Vec<Advice>,
    submitted: bool,
}

#[derive(Debug, Clone, Default)]
struct AdviceSlot {
    active: Arc<Mutex<Option<ActiveAdvice>>>,
    state: SharedAdviceState,
    /// Incremented for every queued note so idle orchestrators can wake up
    /// and interject advice that became ready after their turn completed.
    posted: Option<watch::Sender<u64>>,
    finished_reviews: Option<watch::Sender<u64>>,
}

impl AdviceSlot {
    async fn begin(&self, id: u64, epoch: u64, spans: Vec<ReviewedSpan>) {
        {
            let mut state = self.state.lock().expect("Loki advice state poisoned");
            for span in &spans {
                state.reviewed_spans.insert(
                    (
                        epoch,
                        StepRef {
                            god: span.target,
                            ordinal: span.first_step,
                        },
                    ),
                    span.clone(),
                );
            }
        }
        *self.active.lock().await = Some(ActiveAdvice {
            id,
            epoch,
            spans,
            accepted_steps: HashSet::new(),
            accepted_advice: Vec::new(),
            submitted: false,
        });
    }

    async fn accept(&self, args: AdviseArgs) -> std::result::Result<Vec<Advice>, &'static str> {
        let mut active_guard = self.active.lock().await;
        let mut state = self.state.lock().expect("Loki advice state poisoned");
        let submission_summary = summarize_submission(&args.advice);
        let review_context = active_guard.as_ref().map_or_else(
            || "no active review".to_string(),
            |active| format!("review #{}", active.id),
        );
        let reject = |state: &mut AdviceState, reason: &'static str| -> &'static str {
            state.rejected_log.push(RejectedEntry {
                context: review_context.clone(),
                reason,
                text: submission_summary.clone(),
            });
            reason
        };
        let Some(active) = active_guard.as_mut() else {
            return Err(reject(&mut state, "no advisor update is active"));
        };
        if active.submitted {
            return Err(reject(
                &mut state,
                "only one structured advice list is allowed per review turn",
            ));
        }
        let mut referenced = HashSet::new();
        for item in &args.advice {
            let in_batch = active.spans.iter().any(|span| {
                span.target == item.step.god
                    && span.first_step == item.step.ordinal
                    && span.last_step == item.step.ordinal
            });
            // A step reviewed in any earlier batch within the *same* epoch
            // is also a valid reference, so Loki can re-raise an unresolved
            // issue keyed to an older step instead of being forced to drop
            // it. Steps from a prior epoch, or never reviewed at all, still
            // fail this check.
            let previously_reviewed = state
                .reviewed_spans
                .contains_key(&(active.epoch, item.step));
            if !in_batch && !previously_reviewed {
                return Err(reject(
                    &mut state,
                    "advice referenced a step outside this review batch",
                ));
            }
            if item.note.trim().is_empty() || is_content_free(&item.note) {
                continue;
            }
            if !referenced.insert(item.step) || active.accepted_steps.contains(&item.step) {
                return Err(reject(
                    &mut state,
                    "only one advice note is allowed per reviewed step",
                ));
            }
        }
        let mut accepted = Vec::new();
        active.submitted = true;
        for item in args.advice {
            let span = active
                .spans
                .iter()
                .find(|span| {
                    span.target == item.step.god
                        && span.first_step == item.step.ordinal
                        && span.last_step == item.step.ordinal
                })
                .cloned()
                .or_else(|| {
                    state
                        .reviewed_spans
                        .get(&(active.epoch, item.step))
                        .cloned()
                })
                .expect("advice step validated before queue mutation");
            if item.note.trim().is_empty() || is_content_free(&item.note) {
                continue;
            }
            active.accepted_steps.insert(item.step);
            state.next_advice_id = state.next_advice_id.saturating_add(1);
            let advice = Advice {
                id: state.next_advice_id,
                epoch: active.epoch,
                target: item.step.god,
                note: item.note.trim().to_string(),
                span,
                updated: false,
            };
            if advice.target == Target::Thor && advice.span.first_step <= state.thor_cutoff {
                tracing::info!(
                    event = "advice_cutoff_drop",
                    advice_id = advice.id,
                    step = advice.span.first_step,
                    "discarded stale Thor advice after Eitri handoff"
                );
                continue;
            }
            let AdviceState {
                thor,
                eitri,
                dropped_thor,
                dropped_eitri,
                ..
            } = &mut *state;
            let (queue, dropped) = match advice.target {
                Target::Thor => (thor, dropped_thor),
                Target::Eitri => (eitri, dropped_eitri),
            };
            // A note keyed to the same (god, ordinal) step already sitting
            // in the queue is a re-raise, not new material: update it in
            // place so Loki's re-raises are idempotent instead of growing
            // the queue and risking an unrelated note's eviction (see
            // `push_bounded`). Only a genuinely new step goes through the
            // bounded push, so overflow accounting still only counts real
            // new arrivals.
            match queue.iter_mut().find(|queued| {
                queued.span.target == advice.span.target
                    && queued.span.first_step == advice.span.first_step
            }) {
                Some(queued) => {
                    queued.note = advice.note.clone();
                    queued.epoch = advice.epoch;
                    queued.span = advice.span.clone();
                    queued.updated = true;
                    tracing::info!(
                        event = "advice_updated",
                        advice_id = queued.id,
                        epoch = advice.epoch,
                        review_target = advice.target.label(),
                        reviewed_step = advice.span.first_step,
                        advice = %advice.note,
                        "Loki advice updated in place (deduplicated)"
                    );
                }
                None => {
                    push_bounded(queue, dropped, advice.clone());
                    tracing::info!(
                        event = "advice_routed",
                        advice_id = advice.id,
                        epoch = advice.epoch,
                        review_target = advice.target.label(),
                        reviewed_step = advice.span.first_step,
                        delivery_route = match advice.target {
                            Target::Thor => "thor_queue",
                            Target::Eitri => "eitri_queue",
                        },
                        advice = %advice.note,
                        "Loki advice routed"
                    );
                }
            }
            active.accepted_advice.push(advice.clone());
            accepted.push(advice);
        }
        drop(state);
        if !accepted.is_empty()
            && let Some(posted) = self.posted.as_ref()
        {
            posted.send_modify(|count| *count += 1);
        }
        Ok(accepted)
    }

    async fn finish(&self, id: u64) -> Vec<Advice> {
        let mut active = self.active.lock().await;
        if active.as_ref().is_some_and(|active| active.id == id) {
            let advice = active
                .take()
                .map_or_else(Vec::new, |active| active.accepted_advice);
            if let Some(finished) = self.finished_reviews.as_ref() {
                let _ = finished.send(id);
            }
            return advice;
        }
        Vec::new()
    }
}

fn push_bounded(queue: &mut VecDeque<Advice>, dropped: &mut u64, advice: Advice) {
    if queue.len() == ADVICE_QUEUE_CAPACITY {
        queue.pop_front();
        *dropped = dropped.saturating_add(1);
        tracing::warn!(
            event = "advice_queue_overflow",
            target = advice.target.label(),
            "unseen Loki advice dropped; Thor should pull advice more often"
        );
    }
    queue.push_back(advice);
}

#[derive(Clone)]
struct McpHandler {
    advice: AdviceSlot,
    tool_router: ToolRouter<Self>,
}

#[tool_router(router = tool_router)]
impl McpHandler {
    fn new(advice: AdviceSlot) -> Self {
        Self {
            advice,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "advise",
        description = "Queue material advice as a list of exact reviewed steps and notes. Include at most one note per step, omit steps without material correctness, safety, scope, or strategy advice, and call this tool at most once per review turn."
    )]
    async fn advise(
        &self,
        Parameters(args): Parameters<AdviseArgs>,
    ) -> std::result::Result<CallToolResult, McpError> {
        match self.advice.accept(args).await {
            Ok(advice) => Ok(CallToolResult::success(vec![Content::text(format!(
                "Accepted {} advice item(s).",
                advice.len()
            ))])),
            Err(message) => Ok(CallToolResult::error(vec![Content::text(message)])),
        }
    }
}

impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "mj-loki-advisor",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "You are a read-only advisor. The advise tool is your only channel back to Thor or Eitri. Stay silent unless a concrete material suggestion is necessary.",
            )
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListToolsResult, McpError>> + Send + '_ {
        std::future::ready(Ok(ListToolsResult::with_all_items(
            self.tool_router.list_all(),
        )))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<CallToolResult, McpError>> + Send + '_ {
        self.tool_router
            .call(ToolCallContext::new(self, request, context))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_router.get(name).cloned()
    }
}

// rmcp's LocalSessionManager evicts idle sessions after
// SessionConfig::keep_alive (default 300s). Benchmark evidence showed a
// retained mj-code-agent session evicted at exactly 300.014s idle, 404ing
// every later delegation call; Loki's advisor and pull MCP servers are the
// same single-tenant, process-lifetime shape, so disable idle eviction here
// too rather than tuning the timeout. (`SessionConfig`/`LocalSessionManager`
// are `#[non_exhaustive]`, so they must be built via `Default::default()`
// plus field assignment rather than struct-literal syntax.)
fn session_manager_without_idle_eviction() -> LocalSessionManager {
    let mut manager = LocalSessionManager::default();
    manager.session_config.keep_alive = None;
    manager
}

struct HttpServer {
    advertised: McpServer,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl HttpServer {
    async fn start(advice: AdviceSlot) -> Result<Self> {
        let mut token_bytes = [0_u8; 32];
        getrandom::fill(&mut token_bytes)
            .map_err(|error| anyhow!("generate Loki advisor MCP bearer token: {error}"))?;
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_bytes);
        let authorization = format!("Bearer {token}");
        let handler = McpHandler::new(advice);
        let cancellation = CancellationToken::new();
        let mut config = StreamableHttpServerConfig::default();
        config.cancellation_token = cancellation.clone();
        let service = StreamableHttpService::new(
            move || Ok(handler.clone()),
            Arc::new(session_manager_without_idle_eviction()),
            config,
        );
        let protected = axum::Router::new().nest_service(MCP_PATH, service).layer(
            axum::middleware::from_fn_with_state(authorization.clone(), require_bearer),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind Loki advisor MCP listener")?;
        let addr = listener
            .local_addr()
            .context("read Loki advisor MCP listener address")?;
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, protected)
                .with_graceful_shutdown(task_cancellation.cancelled_owned())
                .await
            {
                tracing::warn!("Loki advisor MCP listener stopped: {error}");
            }
        });
        let advertised = McpServer::Http(
            McpServerHttp::new(MCP_SERVER_NAME, format!("http://{addr}{MCP_PATH}"))
                .headers(vec![HttpHeader::new("Authorization", authorization)]),
        );
        Ok(Self {
            advertised,
            cancellation,
            task,
        })
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

#[derive(Clone)]
struct PullMcpHandler {
    reviewer: Handle,
    consumer: Consumer,
    tool_router: ToolRouter<Self>,
}

#[tool_router(router = tool_router)]
impl PullMcpHandler {
    fn new(reviewer: Handle, consumer: Consumer) -> Self {
        Self {
            reviewer,
            consumer,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "pull_advice",
        description = "Drain Loki advice at a good stopping point. Pull after failed validation before retrying, and before finalizing when a semantic step has elapsed since the last pull or automatic receipt. Avoid redundant pulls at consecutive semantic steps and do not let more than eight semantic steps pass without pulling. Automatic Council delivery receipts already drain the named queues, so do not immediately pull again after one."
    )]
    async fn pull_advice(&self) -> std::result::Result<CallToolResult, McpError> {
        let outcome = self.reviewer.pull_manual(self.consumer).await;
        tracing::info!(
            event = "advice_pulled",
            consumer = self.consumer.label(),
            count = outcome.advice.len(),
            dropped = outcome.dropped,
            waited = outcome.waited,
            "Loki advice pulled"
        );
        let text = format_pull_outcome(&outcome, self.reviewer.current_epoch(), self.consumer);
        let items = outcome
            .advice
            .iter()
            .map(|advice| {
                serde_json::json!({
                    "adviceId": advice.id,
                    "step": { "god": advice.target, "ordinal": advice.span.first_step },
                    "note": advice.note,
                })
            })
            .collect::<Vec<_>>();
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(serde_json::json!({
            "consumer": self.consumer.label().to_ascii_lowercase(),
            "advice": items,
            "dropped": outcome.dropped,
            "waitedForLoki": outcome.waited,
            "drainedQueues": match self.consumer { Consumer::Thor => vec!["thor", "eitri"], Consumer::Eitri => vec!["eitri"] },
        }));
        Ok(result)
    }
}

impl ServerHandler for PullMcpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(PULL_MCP_SERVER_NAME, env!("CARGO_PKG_VERSION")))
            .with_instructions("Use pull_advice at natural stopping points. This endpoint is scoped to your Council role.")
    }

    fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListToolsResult, McpError>> + Send + '_ {
        std::future::ready(Ok(ListToolsResult::with_all_items(
            self.tool_router.list_all(),
        )))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<CallToolResult, McpError>> + Send + '_ {
        self.tool_router
            .call(ToolCallContext::new(self, request, context))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_router.get(name).cloned()
    }
}

pub struct PullServer {
    advertised: McpServer,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl PullServer {
    pub fn start(reviewer: Handle, consumer: Consumer) -> Result<Self> {
        let mut token_bytes = [0_u8; 32];
        getrandom::fill(&mut token_bytes)
            .map_err(|error| anyhow!("generate Loki pull MCP bearer token: {error}"))?;
        let authorization = format!(
            "Bearer {}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_bytes)
        );
        let handler = PullMcpHandler::new(reviewer, consumer);
        let cancellation = CancellationToken::new();
        let mut config = StreamableHttpServerConfig::default();
        config.cancellation_token = cancellation.clone();
        let service = StreamableHttpService::new(
            move || Ok(handler.clone()),
            Arc::new(session_manager_without_idle_eviction()),
            config,
        );
        let protected = axum::Router::new().nest_service(MCP_PATH, service).layer(
            axum::middleware::from_fn_with_state(authorization.clone(), require_bearer),
        );
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").context("bind Loki pull MCP listener")?;
        listener
            .set_nonblocking(true)
            .context("configure Loki pull MCP listener")?;
        let listener = tokio::net::TcpListener::from_std(listener)
            .context("register Loki pull MCP listener")?;
        let addr = listener
            .local_addr()
            .context("read Loki pull MCP listener address")?;
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, protected)
                .with_graceful_shutdown(task_cancellation.cancelled_owned())
                .await
            {
                tracing::warn!("Loki pull MCP listener stopped: {error}");
            }
        });
        let advertised = McpServer::Http(
            McpServerHttp::new(PULL_MCP_SERVER_NAME, format!("http://{addr}{MCP_PATH}"))
                .headers(vec![HttpHeader::new("Authorization", authorization)]),
        );
        Ok(Self {
            advertised,
            cancellation,
            task,
        })
    }

    pub fn advertised(&self) -> &McpServer {
        &self.advertised
    }
}

impl Drop for PullServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

async fn require_bearer(
    State(expected): State<String>,
    request: HttpRequest,
    next: Next,
) -> std::result::Result<Response, (StatusCode, &'static str)> {
    let authorized = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.as_bytes() == expected.as_bytes());
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err((StatusCode::UNAUTHORIZED, "unauthorized"))
    }
}

#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub step: u64,
    pub text: String,
    pub activities: Vec<String>,
}

#[derive(Default)]
pub struct BoundaryTracker {
    trajectory: String,
    review_trajectory: String,
    final_message: String,
    segment: String,
    lane: Option<SegmentLane>,
    tools: HashMap<String, agent_client_protocol::schema::v1::ToolCall>,
    terminals: HashMap<String, TerminalTool>,
    next_step: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SegmentLane {
    Message,
    Thought,
}

#[derive(Clone)]
struct TerminalTool {
    activity: String,
    head: String,
}

impl BoundaryTracker {
    pub fn observe(&mut self, event: &UiEvent) -> Option<Checkpoint> {
        use agent_client_protocol::schema::v1::{
            SessionUpdate, ToolCall, ToolCallContent, ToolCallStatus,
        };
        let flush = |this: &mut Self| {
            this.lane.take()?;
            if this.segment.trim().is_empty() {
                this.segment.clear();
                return None;
            }
            Some(std::mem::take(&mut this.segment))
        };
        let append = |this: &mut Self, lane: SegmentLane, text: &str| {
            if this.segment.is_empty() {
                this.segment.push_str("**agent**:\n");
            }
            if this.lane != Some(lane) {
                if this.lane.is_some() {
                    this.segment.push('\n');
                }
                if lane == SegmentLane::Thought {
                    this.segment.push_str("_thinking:_ ");
                }
                this.lane = Some(lane);
            }
            this.segment.push_str(text);
        };
        let boundary: Option<(String, String, Vec<String>)> = match event {
            UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(chunk)) => {
                let text = crate::event::content_block_text(&chunk.content);
                append(self, SegmentLane::Message, &text);
                self.final_message.push_str(&text);
                None
            }
            UiEvent::SessionUpdate(SessionUpdate::AgentThoughtChunk(chunk)) => {
                append(
                    self,
                    SegmentLane::Thought,
                    &crate::event::content_block_text(&chunk.content),
                );
                None
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCall(call)) => {
                let terminal_backed = call
                    .content
                    .iter()
                    .any(|content| matches!(content, ToolCallContent::Terminal(_)));
                for content in &call.content {
                    if let ToolCallContent::Terminal(terminal) = content {
                        self.terminals.insert(
                            terminal.terminal_id.to_string(),
                            TerminalTool {
                                activity: tool_activity(call),
                                head: tool_call_head(call),
                            },
                        );
                    }
                }
                let complete = matches!(
                    call.status,
                    ToolCallStatus::Completed | ToolCallStatus::Failed
                );
                self.tools
                    .insert(call.tool_call_id.to_string(), call.clone());
                (complete && !terminal_backed && !is_pull_advice(call)).then(|| {
                    self.final_message.clear();
                    let activity = tool_activity(call);
                    let previous = flush(self);
                    (
                        join_agent_boundary(previous.clone(), render_tool_delta(call)),
                        join_agent_boundary(previous, render_review_tool_delta(call)),
                        vec![activity],
                    )
                })
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCallUpdate(update)) => {
                let id = update.tool_call_id.to_string();
                let completed = matches!(
                    update.fields.status,
                    Some(ToolCallStatus::Completed | ToolCallStatus::Failed)
                );
                let rendered = {
                    let tool = self
                        .tools
                        .entry(id.clone())
                        .or_insert_with(|| ToolCall::new(id.clone(), "tool"));
                    tool.update(update.fields.clone());
                    (completed && !tool_has_terminal(tool) && !is_pull_advice(tool))
                        .then(|| (render_tool_delta(tool), render_review_tool_delta(tool)))
                };
                rendered.map(|(rendered, review_rendered)| {
                    self.final_message.clear();
                    let activity = self
                        .tools
                        .get(&id)
                        .map_or_else(|| "tool".to_string(), tool_activity);
                    let previous = flush(self);
                    (
                        join_agent_boundary(previous.clone(), rendered),
                        join_agent_boundary(previous, review_rendered),
                        vec![activity],
                    )
                })
            }
            UiEvent::SessionUpdate(SessionUpdate::Plan(plan)) => {
                self.final_message.clear();
                let text = join_boundary(flush(self), format!("plan update:\n{plan:?}"));
                Some((text.clone(), text, vec!["plan update".to_string()]))
            }
            UiEvent::TerminalOutput(snapshot) if snapshot.exit_status.is_some() => {
                let terminal = self
                    .terminals
                    .get(&snapshot.terminal_id)
                    .cloned()
                    .unwrap_or_else(|| TerminalTool {
                        activity: "terminal".to_string(),
                        head: "→ terminal()".to_string(),
                    });
                let text = join_agent_boundary(
                    flush(self),
                    render_terminal_result(&terminal.head, snapshot),
                );
                Some((text.clone(), text, vec![terminal.activity]))
            }
            UiEvent::PromptDone { stop_reason, .. } => {
                use agent_client_protocol::schema::v1::StopReason;
                // The concluding message otherwise never reaches a tool
                // boundary, so flush it as its own reviewable checkpoint.
                // Cancelled turns are either user aborts or Loki interrupts;
                // both discard the partial segment through reset_attempt.
                (!matches!(stop_reason, StopReason::Cancelled))
                    .then(|| flush(self))
                    .flatten()
                    .map(|segment| (segment.clone(), segment, vec!["final response".to_string()]))
            }
            UiEvent::PromptFailed { .. } => None,
            _ => None,
        };
        boundary.map(|(text, review_text, activities)| {
            self.next_step += 1;
            self.trajectory.push_str(&text);
            self.trajectory.push('\n');
            self.review_trajectory.push_str(&review_text);
            self.review_trajectory.push('\n');
            Checkpoint {
                step: self.next_step,
                text,
                activities,
            }
        })
    }

    #[cfg(test)]
    pub fn trajectory(&self) -> String {
        self.trajectory.clone()
    }

    pub fn review_trajectory(&self) -> String {
        self.review_trajectory.clone()
    }

    pub fn final_message(&self) -> String {
        self.final_message.clone()
    }

    pub fn reset_attempt(&mut self) {
        self.final_message.clear();
        self.segment.clear();
        self.lane = None;
        self.tools.clear();
        self.terminals.clear();
    }
}

fn tool_has_terminal(tool: &agent_client_protocol::schema::v1::ToolCall) -> bool {
    use agent_client_protocol::schema::v1::ToolCallContent;
    tool.content
        .iter()
        .any(|content| matches!(content, ToolCallContent::Terminal(_)))
}

fn is_pull_advice(tool: &agent_client_protocol::schema::v1::ToolCall) -> bool {
    let activity = tool_activity(tool);
    activity == "pull_advice" || activity.ends_with("/pull_advice")
}

fn render_tool_delta(tool: &agent_client_protocol::schema::v1::ToolCall) -> String {
    render_tool_delta_with_diffs(tool, true)
}

fn render_review_tool_delta(tool: &agent_client_protocol::schema::v1::ToolCall) -> String {
    render_tool_delta_with_diffs(tool, false)
}

fn render_tool_delta_with_diffs(
    tool: &agent_client_protocol::schema::v1::ToolCall,
    include_diffs: bool,
) -> String {
    use agent_client_protocol::schema::v1::{ToolCallContent, ToolCallStatus};
    let output = tool_output_text(tool);
    let lines = line_count(&output);
    let count = line_count_label(lines);
    let mut text = match tool.status {
        ToolCallStatus::Completed => format!("{} ⇒ ok · {count}", tool_call_head(tool)),
        ToolCallStatus::Failed => format!("{} ⇒ error · {count}", tool_call_head(tool)),
        _ => format!("{} ⇒ pending", tool_call_head(tool)),
    };
    if matches!(tool.status, ToolCallStatus::Failed)
        && let Some(first) = tool
            .raw_output
            .as_ref()
            .and_then(first_error_value)
            .or_else(|| {
                output
                    .lines()
                    .find(|line| !line.trim().is_empty())
                    .map(str::to_string)
            })
    {
        text.push_str(" — ");
        text.push_str(&one_line(first.trim(), 120));
    }
    if include_diffs {
        for content in &tool.content {
            if let ToolCallContent::Diff(diff) = content {
                let diff = unified_diff(diff);
                if !diff.trim().is_empty() {
                    text.push('\n');
                    text.push_str(&fence_diff(&diff));
                }
            }
        }
    }
    if let Some(intent) = tool
        .raw_input
        .as_ref()
        .and_then(|value| find_json_string(value, &["i"]))
        .filter(|intent| !intent.trim().is_empty())
    {
        format!("// {}\n{text}", one_line(&intent, 80))
    } else {
        text
    }
}

fn tool_call_head(tool: &agent_client_protocol::schema::v1::ToolCall) -> String {
    let activity = tool_activity(tool);
    let primary = tool
        .raw_input
        .as_ref()
        .and_then(|value| primary_arg(&activity, value))
        .unwrap_or_default();
    format!("→ {activity}({primary})")
}

fn render_terminal_result(
    activity: &str,
    snapshot: &crate::event::TerminalOutputSnapshot,
) -> String {
    let lines = line_count(&snapshot.output);
    let count = line_count_label(lines);
    let failed = snapshot.exit_status.as_ref().is_some_and(|status| {
        status.exit_code.is_some_and(|code| code != 0) || status.signal.is_some()
    });
    let mut text = format!(
        "{activity} ⇒ {} · {count}",
        if failed { "error" } else { "ok" }
    );
    if failed && let Some(first) = snapshot.output.lines().find(|line| !line.trim().is_empty()) {
        text.push_str(" — ");
        text.push_str(&one_line(first, 120));
    }
    text
}

fn unified_diff(diff: &agent_client_protocol::schema::v1::Diff) -> String {
    let path = diff.path.display().to_string();
    let relative = path.trim_start_matches('/');
    let old_header = diff
        .old_text
        .as_ref()
        .map_or_else(|| "/dev/null".to_string(), |_| format!("a/{relative}"));
    let new_header = format!("b/{relative}");
    TextDiff::from_lines(diff.old_text.as_deref().unwrap_or(""), &diff.new_text)
        .unified_diff()
        .context_radius(3)
        .header(&old_header, &new_header)
        .to_string()
}

fn fence_diff(diff: &str) -> String {
    let longest = diff.split(|ch| ch != '`').map(str::len).max().unwrap_or(0);
    let fence = "`".repeat(longest.saturating_add(1).max(3));
    format!("{fence}diff\n{diff}{fence}")
}

fn line_count(text: &str) -> usize {
    if text.is_empty() {
        0
    } else {
        text.split('\n').count()
    }
}

fn line_count_label(lines: usize) -> String {
    format!("{lines} {}", if lines == 1 { "line" } else { "lines" })
}

fn first_error_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => ["error", "stderr", "message"]
            .iter()
            .find_map(|key| map.get(*key))
            .and_then(|value| match value {
                serde_json::Value::String(text) => text.lines().next().map(str::to_string),
                value => Some(value.to_string()),
            })
            .or_else(|| map.values().find_map(first_error_value)),
        serde_json::Value::Array(values) => values.iter().find_map(first_error_value),
        _ => None,
    }
}

fn tool_output_text(tool: &agent_client_protocol::schema::v1::ToolCall) -> String {
    use agent_client_protocol::schema::v1::ToolCallContent;
    let mut parts = Vec::new();
    for content in &tool.content {
        if let ToolCallContent::Content(content) = content {
            parts.push(crate::event::content_block_text(&content.content));
        }
    }
    if let Some(output) = tool.raw_output.as_ref() {
        collect_json_text(output, &mut parts);
    }
    parts.join("\n")
}

fn collect_json_text(value: &serde_json::Value, parts: &mut Vec<String>) {
    match value {
        serde_json::Value::String(value) => parts.push(value.clone()),
        serde_json::Value::Array(values) => {
            for value in values {
                collect_json_text(value, parts);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_json_text(value, parts);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

fn tool_activity(tool: &agent_client_protocol::schema::v1::ToolCall) -> String {
    let metadata = tool
        .meta
        .as_ref()
        .and_then(|meta| serde_json::to_value(meta).ok());
    metadata
        .as_ref()
        .and_then(|value| find_json_string(value, &["toolName", "tool_name", "name"]))
        .or_else(|| {
            tool.raw_input
                .as_ref()
                .and_then(|value| find_json_string(value, &["toolName", "tool_name"]))
        })
        .unwrap_or_else(|| tool.title.clone())
}

fn find_json_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            for key in keys {
                if let Some(value) = map.get(*key).and_then(serde_json::Value::as_str) {
                    return Some(value.to_string());
                }
            }
            map.values().find_map(|value| find_json_string(value, keys))
        }
        serde_json::Value::Array(values) => values
            .iter()
            .find_map(|value| find_json_string(value, keys)),
        _ => None,
    }
}

fn primary_arg(activity: &str, value: &serde_json::Value) -> Option<String> {
    if activity == "grep" {
        let pattern = find_json_primary_value(value, "pattern");
        let paths = find_json_primary_value(value, "path")
            .or_else(|| find_json_primary_value(value, "paths"));
        match (pattern, paths) {
            (Some(pattern), Some(paths)) => {
                return Some(one_line(&format!("{pattern} @ {paths}"), 120));
            }
            (Some(pattern), None) => return Some(one_line(&pattern, 120)),
            (None, Some(paths)) => return Some(one_line(&paths, 120)),
            (None, None) => {}
        }
    }
    if activity == "glob"
        && let Some(paths) = find_json_primary_value(value, "path")
            .or_else(|| find_json_primary_value(value, "paths"))
    {
        return Some(one_line(&paths, 120));
    }
    if activity == "ast_grep"
        && let Some(pattern) = find_json_primary_value(value, "pat")
    {
        return Some(one_line(&pattern, 120));
    }
    for key in [
        "path",
        "file_path",
        "filePath",
        "command",
        "cmd",
        "pattern",
        "url",
        "query",
        "prompt",
        "assignment",
        "note",
        "message",
        "op",
        "name",
        "id",
    ] {
        if let Some(primary) = find_json_primary_value(value, key) {
            return Some(one_line(&primary, 120));
        }
    }
    first_non_intent_string(value)
        .map(|value| one_line(&value, 120))
        .or_else(|| {
            (!matches!(value, serde_json::Value::Null)).then(|| one_line(&value.to_string(), 120))
        })
}

fn first_non_intent_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => map.iter().find_map(|(key, value)| {
            if key == "i" {
                None
            } else if let Some(value) = value.as_str().filter(|value| !value.is_empty()) {
                Some(value.to_string())
            } else {
                first_non_intent_string(value)
            }
        }),
        serde_json::Value::Array(values) => values.iter().find_map(first_non_intent_string),
        _ => None,
    }
}

fn find_json_primary_value(value: &serde_json::Value, key: &str) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => map
            .get(key)
            .and_then(|value| match value {
                serde_json::Value::String(value) if !value.is_empty() => Some(value.clone()),
                serde_json::Value::Array(values)
                    if !values.is_empty()
                        && values.iter().all(|value| value.as_str().is_some()) =>
                {
                    Some(
                        values
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .collect::<Vec<_>>()
                            .join(", "),
                    )
                }
                _ => None,
            })
            .or_else(|| {
                map.values()
                    .find_map(|value| find_json_primary_value(value, key))
            }),
        serde_json::Value::Array(values) => values
            .iter()
            .find_map(|value| find_json_primary_value(value, key)),
        _ => None,
    }
}

fn one_line(text: &str, max: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        return flat;
    }
    let mut shortened = flat.chars().take(max.saturating_sub(1)).collect::<String>();
    shortened.push('…');
    shortened
}

fn join_boundary(previous: Option<String>, current: String) -> String {
    previous.map_or(current.clone(), |previous| format!("{previous}\n{current}"))
}

fn join_agent_boundary(previous: Option<String>, current: String) -> String {
    previous.map_or_else(
        || format!("**agent**:\n{current}"),
        |previous| format!("{previous}\n{current}"),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    Thor,
    Eitri,
}

impl Target {
    pub fn label(self) -> &'static str {
        match self {
            Self::Thor => "Thor",
            Self::Eitri => "Eitri",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consumer {
    Thor,
    Eitri,
}

impl Consumer {
    fn can_see(self, target: Target) -> bool {
        self == Self::Thor || target == Target::Eitri
    }

    fn label(self) -> &'static str {
        match self {
            Self::Thor => "Thor",
            Self::Eitri => "Eitri",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PullOutcome {
    pub advice: Vec<Advice>,
    pub dropped: u64,
    pub waited: bool,
}

impl PullOutcome {
    pub fn is_empty(&self) -> bool {
        self.advice.is_empty() && self.dropped == 0
    }
}

fn drain_for(state: &mut AdviceState, consumer: Consumer) -> PullOutcome {
    let mut advice: Vec<Advice> = match consumer {
        Consumer::Thor => state.thor.drain(..).chain(state.eitri.drain(..)).collect(),
        Consumer::Eitri => state.eitri.drain(..).collect(),
    };
    advice.sort_by_key(|advice: &Advice| advice.id);
    for item in &advice {
        state.delivered_log.push(DeliveredEntry {
            id: item.id,
            target: item.target,
            note: item.note.clone(),
        });
    }
    let dropped = match consumer {
        Consumer::Thor => std::mem::take(&mut state.dropped_thor)
            .saturating_add(std::mem::take(&mut state.dropped_eitri)),
        Consumer::Eitri => std::mem::take(&mut state.dropped_eitri),
    };
    PullOutcome {
        advice,
        dropped,
        waited: false,
    }
}

enum Request {
    Warmup,
    Begin {
        epoch: u64,
        task: String,
    },
    TargetContext {
        epoch: u64,
        target: Target,
        invocation: Option<u64>,
        text: String,
    },
    Review {
        id: u64,
        epoch: u64,
        target: Target,
        invocation: Option<u64>,
        checkpoint: Checkpoint,
    },
    /// Sent by `Handle::rendezvous` as a synchronization barrier: acked once
    /// the worker has classified every request that was enqueued ahead of
    /// it into this wake-up's dispatch decision -- claimed into the batch
    /// about to be reviewed, or discarded as belonging to a stale epoch.
    /// The main line has no group-size gate to force open (unlike the
    /// grouped-review design this is ported from), so `Flush` never changes
    /// what gets dispatched or when -- every checkpoint already goes out as
    /// soon as the worker is free. What it gives `Handle::rendezvous` is a
    /// definite fence: "the worker has now drained its request channel up
    /// to this point," so step 1 of a rendezvous (confirm every checkpoint
    /// produced so far is at least in the worker's hands) is a positive
    /// confirmation rather than an inference from `pending_thor`/
    /// `pending_eitri` counters that happen to also be accurate here.
    /// `ack` is purely a synchronization point, not a payload: dropping it
    /// without sending (the request is discarded as stale-epoch, or the
    /// worker shuts down first) is equivalent to sending it.
    Flush {
        epoch: u64,
        ack: oneshot::Sender<()>,
    },
    Compact {
        trigger: CompactTrigger,
        responder: Option<oneshot::Sender<AgentCommandOutcome>>,
    },
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct Handle {
    requests: mpsc::UnboundedSender<Request>,
    ids: Arc<AtomicU64>,
    epochs: Arc<AtomicU64>,
    eitri_invocations: Arc<AtomicU64>,
    thor_steps: Arc<AtomicU64>,
    eitri_steps: Arc<AtomicU64>,
    thor_cadence_steps: Arc<AtomicU64>,
    eitri_cadence_steps: Arc<AtomicU64>,
    pending_thor: Arc<AtomicU64>,
    pending_eitri: Arc<AtomicU64>,
    last_thor_pull: Arc<AtomicU64>,
    last_eitri_pull: Arc<AtomicU64>,
    advice_state: SharedAdviceState,
    active: Arc<Mutex<Option<ActiveAdvice>>>,
    abort: watch::Sender<bool>,
    finished: watch::Receiver<bool>,
    posted: watch::Receiver<u64>,
    finished_reviews: watch::Receiver<u64>,
    review_started: watch::Receiver<u64>,
}

impl Handle {
    pub fn start(
        role_pool: crate::quota::RolePool,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        ui_tx: mpsc::UnboundedSender<UiEvent>,
        council_session: String,
    ) -> Self {
        let (requests, rx) = mpsc::unbounded_channel();
        let (abort, abort_rx) = watch::channel(false);
        let (finished_tx, finished) = watch::channel(false);
        let (posted_tx, posted) = watch::channel(0_u64);
        let (finished_reviews_tx, finished_reviews) = watch::channel(0_u64);
        let (review_started_tx, review_started) = watch::channel(0_u64);
        let advice_state = SharedAdviceState::default();
        let active = Arc::new(Mutex::new(None));
        let pending_thor = Arc::new(AtomicU64::new(0));
        let pending_eitri = Arc::new(AtomicU64::new(0));
        let handle = Self {
            requests,
            ids: Arc::new(AtomicU64::new(1)),
            epochs: Arc::new(AtomicU64::new(1)),
            eitri_invocations: Arc::new(AtomicU64::new(1)),
            thor_steps: Arc::new(AtomicU64::new(1)),
            eitri_steps: Arc::new(AtomicU64::new(1)),
            thor_cadence_steps: Arc::new(AtomicU64::new(0)),
            eitri_cadence_steps: Arc::new(AtomicU64::new(0)),
            pending_thor: pending_thor.clone(),
            pending_eitri: pending_eitri.clone(),
            last_thor_pull: Arc::new(AtomicU64::new(u64::MAX)),
            last_eitri_pull: Arc::new(AtomicU64::new(u64::MAX)),
            advice_state: advice_state.clone(),
            active: active.clone(),
            abort,
            finished,
            posted,
            finished_reviews,
            review_started,
        };
        tokio::spawn(worker(
            role_pool,
            cwd,
            additional_directories,
            ui_tx,
            rx,
            advice_state,
            active,
            abort_rx,
            finished_tx,
            posted_tx,
            finished_reviews_tx,
            review_started_tx,
            pending_thor,
            pending_eitri,
            council_session,
        ));
        let _ = handle.requests.send(Request::Warmup);
        handle
    }

    /// Watch that ticks whenever Loki queues a new advice note. Idle
    /// orchestrators use it to interject late advice between user turns.
    pub fn subscribe_advice(&self) -> watch::Receiver<u64> {
        self.posted.clone()
    }

    pub fn begin_turn(&self, task: String) -> u64 {
        let _ = self.abort.send(false);
        let epoch = self.epochs.fetch_add(1, Ordering::Relaxed);
        self.eitri_invocations.store(1, Ordering::Relaxed);
        let _ = self.requests.send(Request::Begin { epoch, task });
        epoch
    }

    pub fn begin_eitri(&self, epoch: u64, context: String) -> u64 {
        let invocation = self.eitri_invocations.fetch_add(1, Ordering::Relaxed);
        let _ = self.requests.send(Request::TargetContext {
            epoch,
            target: Target::Eitri,
            invocation: Some(invocation),
            text: format!("**user**:\n{context}"),
        });
        invocation
    }

    pub async fn pull(&self, consumer: Consumer) -> PullOutcome {
        self.pull_inner(consumer).await
    }

    /// Blocking rendezvous for an injection point (turn boundary, the
    /// pre-shutdown advice drain): confirm the worker has drained its
    /// request channel of everything submitted so far (see `Request::Flush`),
    /// then wait -- bounded by `RENDEZVOUS_TIMEOUT` -- for every review that
    /// is either already in flight or was just picked up to finish, draining
    /// and returning all of their advice as one outcome. Unlike `pull`, this
    /// does not stop at the first review round relevant to `consumer`: it
    /// keeps waiting as long as another review is pending or active, so a
    /// caller gets one complete digest instead of having to pull repeatedly.
    /// On timeout it gives up and returns whatever advice is already queued
    /// rather than hanging the caller.
    pub async fn rendezvous(&self, consumer: Consumer) -> PullOutcome {
        self.rendezvous_bounded(consumer, RENDEZVOUS_TIMEOUT).await
    }

    /// `rendezvous`'s implementation, parameterized on the bound so tests
    /// can exercise the timeout path without waiting out the real
    /// `RENDEZVOUS_TIMEOUT`.
    async fn rendezvous_bounded(&self, consumer: Consumer, timeout: Duration) -> PullOutcome {
        let start = tokio::time::Instant::now();
        let deadline = start + timeout;
        let reviews_flushed = match consumer {
            Consumer::Thor => {
                self.pending_thor.load(Ordering::Relaxed)
                    + self.pending_eitri.load(Ordering::Relaxed)
            }
            Consumer::Eitri => self.pending_eitri.load(Ordering::Relaxed),
        };
        let mut started = self.review_started.clone();
        let _ = *started.borrow_and_update();
        let mut finished = self.finished_reviews.clone();

        let (ack_tx, ack_rx) = oneshot::channel();
        let _ = self.requests.send(Request::Flush {
            epoch: self.current_epoch(),
            ack: ack_tx,
        });
        // Wait (bounded) for the worker to actually drain its request
        // channel up to this point before consulting
        // `has_pending_review`/`active_review_relevant` below -- see
        // `Request::Flush`'s doc comment. Any resolution (acked, or the
        // sender dropped because the request was discarded or the worker
        // shut down) is equivalent here: it is purely a synchronization
        // point, not a payload. A timeout leaves `remaining` at ~0 for the
        // loop below, so it reports `timed_out` rather than silently
        // proceeding as if nothing were pending.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let _ = tokio::time::timeout(remaining, ack_rx).await;

        let mut outcome = {
            let mut state = self
                .advice_state
                .lock()
                .expect("Loki advice state poisoned");
            drain_for(&mut state, consumer)
        };
        let mut did_wait = false;
        let mut timed_out = false;

        loop {
            if tokio::time::Instant::now() >= deadline {
                if self.has_pending_review(consumer)
                    || self.active_review_relevant(consumer).await.is_some()
                {
                    timed_out = true;
                }
                break;
            }
            let review_id = match self.active_review_relevant(consumer).await {
                Some(id) => Some(id),
                None if self.has_pending_review(consumer) => {
                    did_wait = true;
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if tokio::time::timeout(remaining, started.changed())
                        .await
                        .is_err()
                    {
                        timed_out = true;
                        break;
                    }
                    self.active_review_relevant(consumer).await
                }
                None => None,
            };
            let Some(review_id) = review_id else { break };
            did_wait = true;
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if tokio::time::timeout(
                remaining,
                finished.wait_for(|finished_id| *finished_id >= review_id),
            )
            .await
            .is_err()
            {
                timed_out = true;
                break;
            }
            let drained = {
                let mut state = self
                    .advice_state
                    .lock()
                    .expect("Loki advice state poisoned");
                drain_for(&mut state, consumer)
            };
            outcome.advice.extend(drained.advice);
            outcome.dropped = outcome.dropped.saturating_add(drained.dropped);
        }

        outcome.advice.sort_by_key(|advice| advice.id);
        outcome.waited = did_wait;
        let waited_ms = start.elapsed().as_millis() as u64;
        let advice_delivered = outcome.advice.len();
        if timed_out {
            tracing::warn!(
                event = "loki_rendezvous",
                consumer = consumer.label(),
                waited_ms,
                reviews_flushed,
                advice_delivered,
                timed_out,
                "Loki rendezvous hit its bound; delivering whatever advice is already queued"
            );
        } else {
            tracing::info!(
                event = "loki_rendezvous",
                consumer = consumer.label(),
                waited_ms,
                reviews_flushed,
                advice_delivered,
                timed_out,
                "Loki rendezvous completed"
            );
        }
        outcome
    }

    pub async fn pull_manual(&self, consumer: Consumer) -> PullOutcome {
        let current = match consumer {
            Consumer::Thor => self.thor_cadence_steps.load(Ordering::Relaxed),
            Consumer::Eitri => self.eitri_cadence_steps.load(Ordering::Relaxed),
        };
        let last = match consumer {
            Consumer::Thor => self.last_thor_pull.swap(current, Ordering::Relaxed),
            Consumer::Eitri => self.last_eitri_pull.swap(current, Ordering::Relaxed),
        };
        let gap = if last == u64::MAX {
            current
        } else {
            current.saturating_sub(last)
        };
        if last != u64::MAX && gap == 0 {
            tracing::warn!(
                event = "advice_pull_cadence_violation",
                consumer = consumer.label(),
                gap,
                reason = "consecutive",
                "pull_advice called without an intervening semantic step"
            );
        } else if gap > 8 {
            tracing::warn!(
                event = "advice_pull_cadence_violation",
                consumer = consumer.label(),
                gap,
                reason = "overdue",
                "more than eight semantic steps elapsed between pull_advice calls"
            );
        }
        self.pull_inner(consumer).await
    }

    pub async fn compact(&self, trigger: CompactTrigger) -> AgentCommandOutcome {
        let (responder, response) = oneshot::channel();
        if self
            .requests
            .send(Request::Compact {
                trigger,
                responder: Some(responder),
            })
            .is_err()
        {
            return AgentCommandOutcome::Failed("Loki worker closed".to_string());
        }
        response.await.unwrap_or_else(|_| {
            AgentCommandOutcome::Failed("Loki compact response was dropped".to_string())
        })
    }

    pub fn request_compact(&self, trigger: CompactTrigger) {
        let _ = self.requests.send(Request::Compact {
            trigger,
            responder: None,
        });
    }

    async fn pull_inner(&self, consumer: Consumer) -> PullOutcome {
        let deadline = tokio::time::Instant::now() + PULL_WAIT;
        let mut started = self.review_started.clone();
        let _ = *started.borrow_and_update();
        let initial = {
            let mut state = self
                .advice_state
                .lock()
                .expect("Loki advice state poisoned");
            drain_for(&mut state, consumer)
        };
        if !initial.advice.is_empty() || initial.dropped > 0 {
            return initial;
        }
        let mut review_id = self.active_review_relevant(consumer).await;
        if review_id.is_none() && self.has_pending_review(consumer) {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let _ = tokio::time::timeout(remaining, started.changed()).await;
            let queued = {
                let mut state = self
                    .advice_state
                    .lock()
                    .expect("Loki advice state poisoned");
                drain_for(&mut state, consumer)
            };
            if !queued.is_empty() {
                return PullOutcome {
                    waited: true,
                    ..queued
                };
            }
            review_id = self.active_review_relevant(consumer).await;
        }
        let Some(review_id) = review_id else {
            return initial;
        };
        let mut finished = self.finished_reviews.clone();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let _ = tokio::time::timeout(
            remaining,
            finished.wait_for(|finished_id| *finished_id >= review_id),
        )
        .await;
        let mut outcome = {
            let mut state = self
                .advice_state
                .lock()
                .expect("Loki advice state poisoned");
            drain_for(&mut state, consumer)
        };
        outcome.waited = true;
        outcome
    }

    fn has_pending_review(&self, consumer: Consumer) -> bool {
        match consumer {
            Consumer::Thor => {
                self.pending_thor.load(Ordering::Relaxed) > 0
                    || self.pending_eitri.load(Ordering::Relaxed) > 0
            }
            Consumer::Eitri => self.pending_eitri.load(Ordering::Relaxed) > 0,
        }
    }

    async fn active_review_relevant(&self, consumer: Consumer) -> Option<u64> {
        let active_advice = self.active_advice();
        let active = active_advice.lock().await;
        active.as_ref().and_then(|active| {
            active
                .spans
                .iter()
                .any(|span| consumer.can_see(span.target))
                .then_some(active.id)
        })
    }

    fn active_advice(&self) -> Arc<Mutex<Option<ActiveAdvice>>> {
        // The worker and handle share this through AdviceState registration below.
        self.active.clone()
    }

    pub fn begin_eitri_handoff(&self) {
        let cutoff = self.thor_steps.load(Ordering::Relaxed).saturating_sub(1);
        let mut state = self
            .advice_state
            .lock()
            .expect("Loki advice state poisoned");
        state.thor.clear();
        state.thor_cutoff = state.thor_cutoff.max(cutoff);
    }

    pub fn current_epoch(&self) -> u64 {
        self.epochs.load(Ordering::Relaxed).saturating_sub(1)
    }

    pub fn cancel_turn(&self) {
        let _ = self.abort.send(true);
    }

    pub fn observe(
        &self,
        epoch: u64,
        target: Target,
        invocation: Option<u64>,
        checkpoint: Checkpoint,
    ) {
        self.submit(epoch, target, invocation, checkpoint);
    }

    fn submit(
        &self,
        epoch: u64,
        target: Target,
        invocation: Option<u64>,
        mut checkpoint: Checkpoint,
    ) {
        checkpoint.step = match target {
            Target::Thor => {
                self.pending_thor.fetch_add(1, Ordering::Relaxed);
                self.thor_steps.fetch_add(1, Ordering::Relaxed)
            }
            Target::Eitri => {
                self.pending_eitri.fetch_add(1, Ordering::Relaxed);
                self.eitri_steps.fetch_add(1, Ordering::Relaxed)
            }
        };
        if checkpoint_counts_toward_pull_cadence(&checkpoint) {
            match target {
                Target::Thor => self.thor_cadence_steps.fetch_add(1, Ordering::Relaxed),
                Target::Eitri => self.eitri_cadence_steps.fetch_add(1, Ordering::Relaxed),
            };
        }
        let id = self.ids.fetch_add(1, Ordering::Relaxed);
        let _ = self.requests.send(Request::Review {
            id,
            epoch,
            target,
            invocation,
            checkpoint,
        });
    }

    pub async fn shutdown_and_wait(&self) {
        let _ = self.abort.send(true);
        let _ = self.requests.send(Request::Shutdown);
        let mut finished = self.finished.clone();
        if *finished.borrow() {
            return;
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), finished.changed()).await;
    }
}

fn checkpoint_counts_toward_pull_cadence(checkpoint: &Checkpoint) -> bool {
    checkpoint
        .activities
        .iter()
        .any(|activity| activity != "Guardian Review")
}

#[allow(clippy::too_many_arguments)]
async fn worker(
    role_pool: crate::quota::RolePool,
    cwd: PathBuf,
    additional_directories: Vec<PathBuf>,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    mut requests: mpsc::UnboundedReceiver<Request>,
    advice_state: SharedAdviceState,
    active_advice: Arc<Mutex<Option<ActiveAdvice>>>,
    abort_rx: watch::Receiver<bool>,
    finished: watch::Sender<bool>,
    posted: watch::Sender<u64>,
    finished_reviews: watch::Sender<u64>,
    review_started: watch::Sender<u64>,
    pending_thor: Arc<AtomicU64>,
    pending_eitri: Arc<AtomicU64>,
    council_session: String,
) {
    let mut role = role_pool.current();
    let mut epoch = 0;
    let mut pending_context: HashMap<(Target, Option<u64>), String> = HashMap::new();
    let mut session: Option<AgentHandle> = None;
    let mut review_state = LokiReviewState::default();
    let mut compact_threshold = CompactThreshold::default();
    let mut automatic_compact = None;
    // Last-known `context_used` from a review's usage report, logged as the
    // "before" side of a compact's context_used pair. There is no cheap way
    // to get an "after" figure: `AgentCommandOutcome` carries no usage, and
    // issuing an extra prompt just to read post-compact usage would spend
    // the very tokens compaction exists to save. The next review's own
    // `review_started`/`review_finished`+`CouncilUsage` logging is the
    // observable "after" signal for benchmark analysis.
    let mut last_context_used: Option<u64> = None;
    let advice = AdviceSlot {
        active: active_advice,
        state: advice_state,
        posted: Some(posted),
        finished_reviews: Some(finished_reviews),
    };
    let server = match HttpServer::start(advice.clone()).await {
        Ok(server) => Some(server),
        Err(error) => {
            emit_warning(
                &ui_tx,
                &role,
                format!("Loki advisor tool could not start: {error:#}"),
            );
            None
        }
    };
    'worker: loop {
        let request = match automatic_compact.take() {
            Some(trigger) => Request::Compact {
                trigger,
                responder: None,
            },
            None => {
                let Some(request) = requests.recv().await else {
                    break;
                };
                request
            }
        };
        let mut batch = Vec::new();
        let mut compact_requests = Vec::new();
        let mut flush_acks: Vec<oneshot::Sender<()>> = Vec::new();
        let mut incoming = VecDeque::from([request]);
        while let Ok(next) = requests.try_recv() {
            incoming.push_back(next);
        }
        while let Some(request) = incoming.pop_front() {
            match request {
                Request::Warmup => {}
                Request::Begin {
                    epoch: next,
                    task: next_task,
                } => {
                    epoch = next;
                    begin_request(&mut pending_context, next_task);
                }
                Request::TargetContext {
                    epoch: request_epoch,
                    target,
                    invocation,
                    text,
                } if request_epoch == epoch => {
                    pending_context
                        .entry((target, invocation))
                        .and_modify(|pending| {
                            pending.push_str("\n\n");
                            pending.push_str(&text);
                        })
                        .or_insert(text);
                }
                Request::Review {
                    id,
                    epoch: request_epoch,
                    target,
                    invocation,
                    checkpoint,
                } if request_epoch == epoch => {
                    batch.push(ReviewItem {
                        id,
                        target,
                        invocation,
                        checkpoint,
                    });
                }
                Request::Review { target, .. } => match target {
                    Target::Thor => decrement_pending(&pending_thor),
                    Target::Eitri => decrement_pending(&pending_eitri),
                },
                Request::TargetContext { .. } => {}
                Request::Flush {
                    epoch: request_epoch,
                    ack,
                } if request_epoch == epoch => {
                    flush_acks.push(ack);
                }
                // Stale epoch: nothing of this turn's for the flush to act
                // on. Ack immediately rather than dropping silently, so the
                // caller's synchronization barrier resolves promptly instead
                // of relying on drop-triggered `RecvError`.
                Request::Flush { ack, .. } => {
                    let _ = ack.send(());
                }
                Request::Compact { trigger, responder } => {
                    compact_requests.push((trigger, responder));
                }
                Request::Shutdown => break 'worker,
            }
        }
        // Release every rendezvous waiting on this flush now that every
        // request enqueued ahead of it has been classified: `batch` holds
        // everything that will be dispatched this wake-up, and
        // `pending_thor`/`pending_eitri` (decremented below, once the batch
        // is claimed) will correctly settle to reflect it. See
        // `Request::Flush`.
        for ack in flush_acks.drain(..) {
            let _ = ack.send(());
        }
        if !batch.is_empty() {
            match role_pool.select_for_work().await {
                Ok(selection) => {
                    if selection.role.model.model != role.model.model
                        || selection.role.launch.source_id != role.launch.source_id
                    {
                        if let Some(old) = session.take() {
                            old.dismiss().await;
                        }
                        role = selection.role;
                        review_state = LokiReviewState::default();
                        compact_threshold = CompactThreshold::default();
                        automatic_compact = None;
                    }
                }
                Err(_) => {
                    for item in &batch {
                        match item.target {
                            Target::Thor => decrement_pending(&pending_thor),
                            Target::Eitri => decrement_pending(&pending_eitri),
                        }
                    }
                    continue;
                }
            }
        }
        let Some(server) = server.as_ref() else {
            for item in &batch {
                match item.target {
                    Target::Thor => decrement_pending(&pending_thor),
                    Target::Eitri => decrement_pending(&pending_eitri),
                }
            }
            for (_, responder) in compact_requests {
                if let Some(responder) = responder {
                    let _ = responder.send(AgentCommandOutcome::Failed(
                        "Loki advisor server is unavailable".to_string(),
                    ));
                }
            }
            continue;
        };
        if session.is_none() {
            let requested_session_id = review_state.resume_session();
            match connect(
                &role,
                &cwd,
                &additional_directories,
                abort_rx.clone(),
                server.advertised.clone(),
                &council_session,
                requested_session_id.clone(),
            )
            .await
            {
                Ok(agent) => match review_state.accept_session(agent.session_started()) {
                    Ok(()) => {
                        session = Some(agent);
                    }
                    Err(error) => {
                        if !*abort_rx.borrow() {
                            emit_warning(
                                &ui_tx,
                                &role,
                                format!("Loki continuity failure: {error}"),
                            );
                        }
                        agent.dismiss().await;
                    }
                },
                Err(error) => {
                    if !*abort_rx.borrow() {
                        emit_warning(&ui_tx, &role, format!("Loki could not start: {error:#}"));
                    }
                }
            }
        }
        if !compact_requests.is_empty() {
            let trigger = preferred_compact_trigger(&compact_requests)
                .expect("non-empty Loki compact request set");
            let is_automatic = compact_requests
                .iter()
                .any(|(trigger, _)| *trigger == CompactTrigger::Loki128k);
            tracing::info!(
                event = "council_control",
                god = "Loki",
                command = "compact",
                trigger = trigger.label(),
                action = "request",
                coalesced = compact_requests.len().saturating_sub(1),
                context_used_before = last_context_used,
                "Council role control command"
            );
            let compact_started = tokio::time::Instant::now();
            let outcome = match session.as_mut() {
                Some(agent) => agent.run_advertised_command("compact", trigger).await,
                None => AgentCommandOutcome::Failed("Loki session is unavailable".to_string()),
            };
            let duration_ms = compact_started.elapsed().as_millis() as u64;
            match &outcome {
                AgentCommandOutcome::Completed => {
                    tracing::info!(
                        event = "council_control",
                        god = "Loki",
                        command = "compact",
                        trigger = trigger.label(),
                        action = "completion",
                        error = Option::<&str>::None,
                        duration_ms,
                        context_used_before = last_context_used,
                        "Council role control command"
                    );
                }
                AgentCommandOutcome::Skipped => {
                    // Not a failure (e.g. Anvil's own no-op "nothing to
                    // compress" case), but automatic compaction must not
                    // stay disarmed forever just because one round skipped.
                    let retry_armed = is_automatic && compact_threshold.rearm_for_retry(epoch);
                    tracing::info!(
                        event = "council_control",
                        god = "Loki",
                        command = "compact",
                        trigger = trigger.label(),
                        action = "skip",
                        error = Option::<&str>::None,
                        duration_ms,
                        context_used_before = last_context_used,
                        retry_armed,
                        "Council role control command"
                    );
                }
                AgentCommandOutcome::Failed(error) => {
                    let retry_armed = is_automatic && compact_threshold.rearm_for_retry(epoch);
                    tracing::warn!(
                        event = "council_control",
                        god = "Loki",
                        command = "compact",
                        trigger = trigger.label(),
                        action = "failure",
                        error = error.as_str(),
                        duration_ms,
                        context_used_before = last_context_used,
                        retry_armed,
                        "Council role control command"
                    );
                }
            }
            if !batch.is_empty() {
                tracing::info!(
                    event = "loki_round_delayed",
                    god = "Loki",
                    compact_duration_ms = duration_ms,
                    batch_size = batch.len(),
                    "Council review round dispatch delayed by a compact command"
                );
            }
            for (_, responder) in compact_requests {
                if let Some(responder) = responder {
                    let _ = responder.send(outcome.clone());
                }
            }
        }
        if batch.is_empty() || *abort_rx.borrow() {
            for item in &batch {
                match item.target {
                    Target::Thor => decrement_pending(&pending_thor),
                    Target::Eitri => decrement_pending(&pending_eitri),
                }
            }
            continue;
        }
        let spans = reviewed_spans(&batch);
        let id = batch[0].id;
        advice.begin(id, epoch, spans.clone()).await;
        for item in &batch {
            match item.target {
                Target::Thor => decrement_pending(&pending_thor),
                Target::Eitri => decrement_pending(&pending_eitri),
            }
        }
        let _ = review_started.send(id);
        let Some(agent) = session.as_mut() else {
            advice.finish(id).await;
            continue;
        };
        let mut context = Vec::new();
        for span in &spans {
            if let Some(value) = pending_context.remove(&(span.target, span.invocation)) {
                context.push(value);
            }
        }
        let include_contract = review_state.include_contract();
        let ledger = {
            let mut state = advice.state.lock().expect("Loki advice state poisoned");
            take_ledger_snapshot(&mut state)
        };
        let prompt = review_prompt(&batch, &spans, &context, include_contract, &ledger);
        tracing::info!(event = "review_started", council_session = %council_session, god = "Loki", model = %role.model.model, adapter = %role.launch.source_id, review_id = id, epoch, batch_size = batch.len(), review_target = %spans.iter().map(ReviewedSpan::marker).collect::<Vec<_>>().join(" | "), "Loki review started");
        let result = agent
            .prompt(prompt, REVIEW_TIMEOUT, |event| match event {
                TurnEvent::Permission {
                    prompt,
                    access_mode,
                } => {
                    let decision =
                        crate::ragnarok::permission_decision_for_access(access_mode, &prompt);
                    let _ = prompt.responder.send(decision);
                }
                TurnEvent::Message(_)
                | TurnEvent::Thought(_)
                | TurnEvent::Tool { .. }
                | TurnEvent::Note(_) => {}
            })
            .await;
        if let Ok(outcome) = &result {
            review_state.contract_succeeded(include_contract);
            let _ = ui_tx.send(UiEvent::CouncilUsage(Record {
                role: Role::Loki,
                purpose: None,
                usage: outcome.usage.clone(),
                update: outcome.usage_update.clone(),
                session_id: agent
                    .session_started()
                    .map(|(session_id, _)| session_id.to_string()),
            }));
            if let Some(used) = outcome.usage_update.as_ref().map(|usage| usage.used) {
                last_context_used = Some(used);
                if compact_threshold.observe(used) {
                    automatic_compact = Some(CompactTrigger::Loki128k);
                }
            }
        }
        let accepted = advice.finish(id).await;
        if let Err(error) = result {
            let quota_failure = role_pool.observe_failure(&role).await;
            if !quota_failure && !*abort_rx.borrow() {
                emit_warning(&ui_tx, &role, format!("Loki review failed open: {error}"));
            }
            if let Some(old) = session.take() {
                old.dismiss().await;
            }
        }
        tracing::info!(event = "review_finished", council_session = %council_session, god = "Loki", model = %role.model.model, adapter = %role.launch.source_id, review_id = id, epoch, batch_size = batch.len(), advice_accepted = !accepted.is_empty(), advice_count = accepted.len(), "Loki review finished");
    }
    if let Some(agent) = session {
        agent.dismiss().await;
    }
    let _ = finished.send(true);
}

fn decrement_pending(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(1))
    });
}

fn begin_request(pending_context: &mut HashMap<(Target, Option<u64>), String>, task: String) {
    pending_context.clear();
    pending_context.insert((Target::Thor, None), format!("**user**:\n{task}"));
}

/// Tracks the only continuity Loki permits: an exact ACP session resume.
/// This state deliberately outlives request resets and failed replacement
/// attempts so a later retry asks for the original backing session again.
#[derive(Default)]
struct LokiSessionContinuity {
    session_id: Option<String>,
}

impl LokiSessionContinuity {
    fn resume_session(&self) -> Option<String> {
        self.session_id.clone()
    }

    /// Accept a connection's `SessionStarted` notification.
    fn accept(&mut self, started: Option<(&str, bool)>) -> Result<()> {
        let Some((session_id, resumed)) = started else {
            return Err(anyhow!(
                "agent connected without a SessionStarted notification"
            ));
        };
        match self.session_id.as_deref() {
            None if !resumed => {
                self.session_id = Some(session_id.to_string());
                Ok(())
            }
            None => Err(anyhow!(
                "agent reported an unexpected resumed ACP session '{session_id}'"
            )),
            Some(requested) if !resumed => Err(anyhow!(
                "requested ACP session '{requested}' but agent reported a new session '{session_id}'"
            )),
            Some(requested) if session_id != requested => Err(anyhow!(
                "requested ACP session '{requested}' but agent resumed '{session_id}'"
            )),
            Some(_) => Ok(()),
        }
    }
}

/// The logical contract state is separate from ACP session continuity. It is
/// advanced only after the prompt that carries the contract succeeds.
#[derive(Default)]
struct LokiReviewState {
    continuity: LokiSessionContinuity,
    primed: bool,
}

impl LokiReviewState {
    fn resume_session(&self) -> Option<String> {
        self.continuity.resume_session()
    }

    fn accept_session(&mut self, started: Option<(&str, bool)>) -> Result<()> {
        self.continuity.accept(started)
    }

    fn include_contract(&self) -> bool {
        !self.primed
    }

    fn contract_succeeded(&mut self, include_contract: bool) {
        if include_contract {
            self.primed = true;
        }
    }
}

struct ReviewItem {
    id: u64,
    target: Target,
    invocation: Option<u64>,
    checkpoint: Checkpoint,
}

fn reviewed_spans(batch: &[ReviewItem]) -> Vec<ReviewedSpan> {
    batch
        .iter()
        .map(|item| ReviewedSpan {
            target: item.target,
            invocation: item.invocation,
            first_step: item.checkpoint.step,
            last_step: item.checkpoint.step,
            activities: item.checkpoint.activities.clone(),
        })
        .collect()
}

async fn connect(
    role: &ResolvedRole,
    cwd: &std::path::Path,
    additional_directories: &[PathBuf],
    abort: watch::Receiver<bool>,
    advise_server: McpServer,
    council_session: &str,
    resume_session: Option<String>,
) -> Result<AgentHandle> {
    let launch = Launch {
        program: role.launch.command.clone(),
        args: role.launch.args.clone(),
        env: role.launch.env.clone(),
    };
    AgentHandle::connect_with_role_config_and_mcp_resuming(
        &launch,
        cwd,
        additional_directories,
        abort,
        RuntimeAccessMode::ReadOnly,
        HashMap::new(),
        Some(RuntimeRoleConfig {
            label: "Loki".to_string(),
            model_id: role.model.model.clone(),
            model_value: role.model_value.clone(),
            adapter_source_id: role.launch.source_id.clone(),
            permission: None,
            council_session: Some(council_session.to_string()),
            reasoning_effort: role.reasoning_effort.clone(),
        }),
        vec![advise_server],
        resume_session,
    )
    .await
}

/// A submission argument summarized for the rejected-advice ledger: joins
/// each item's step and truncated note, independent of whether the item
/// itself later turns out to be the one that violated the gate.
fn summarize_submission(items: &[AdviseItem]) -> String {
    if items.is_empty() {
        return "(empty submission)".to_string();
    }
    items
        .iter()
        .map(|item| {
            format!(
                "{} #{}: {}",
                item.step.god.label(),
                item.step.ordinal,
                one_line(&item.note, 80)
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Everything the advice-ledger section needs, snapshotted once per review
/// prompt via `take_ledger_snapshot`. `queued` is always the *full* current
/// queue contents (not a delta), so a compacted Loki -- whose model context
/// no longer remembers earlier turns -- still gets the complete outstanding
/// picture on its first post-compact review rather than only what changed
/// since the previous prompt.
#[derive(Debug, Default, Clone)]
struct LedgerSnapshot {
    queued: Vec<Advice>,
    delivered: Vec<DeliveredEntry>,
    rejected: Vec<RejectedEntry>,
}

impl LedgerSnapshot {
    fn is_empty(&self) -> bool {
        self.queued.is_empty() && self.delivered.is_empty() && self.rejected.is_empty()
    }
}

/// Snapshot the ledger for the next review prompt. `delivered_log` and
/// `rejected_log` are drained (cleared) so each is reported exactly once,
/// covering everything since the previous review prompt was built; `queued`
/// is read non-destructively since it must always reflect the full standing
/// queue. This state lives in `AdviceState`, not in Loki's model context, so
/// it is unaffected by `/compact` -- draining still only ever removes what
/// this function itself already reported.
fn take_ledger_snapshot(state: &mut AdviceState) -> LedgerSnapshot {
    let mut queued: Vec<Advice> = state
        .thor
        .iter()
        .chain(state.eitri.iter())
        .cloned()
        .collect();
    queued.sort_by_key(|advice| advice.id);
    LedgerSnapshot {
        queued,
        delivered: std::mem::take(&mut state.delivered_log),
        rejected: std::mem::take(&mut state.rejected_log),
    }
}

/// Render the "### Your advice ledger" section, or an empty string when all
/// three categories are empty (the section is omitted entirely rather than
/// shown hollow).
fn render_ledger(ledger: &LedgerSnapshot) -> String {
    if ledger.is_empty() {
        return String::new();
    }
    let mut section = String::from("### Your advice ledger\n\n");
    if !ledger.delivered.is_empty() {
        section.push_str("Delivered to their target (no action needed):\n");
        for entry in &ledger.delivered {
            section.push_str(&format!(
                "- #{} → {}: {}\n",
                entry.id,
                entry.target.label(),
                one_line(&entry.note, 80)
            ));
        }
        section.push('\n');
    }
    if !ledger.queued.is_empty() {
        section.push_str("Queued, awaiting a turn boundary (do NOT resubmit):\n");
        for advice in &ledger.queued {
            let updated = if advice.updated { " (updated)" } else { "" };
            section.push_str(&format!(
                "- #{} → {}: {}{}\n",
                advice.id,
                advice.target.label(),
                one_line(&advice.note, 80),
                updated
            ));
        }
        section.push('\n');
    }
    if !ledger.rejected.is_empty() {
        section.push_str("Rejected (resubmit only after fixing the reason, or if materially new information changes the advice):\n");
        for entry in &ledger.rejected {
            section.push_str(&format!(
                "- [{}] \"{}\" — {}\n",
                entry.context,
                entry.reason,
                one_line(&entry.text, 80)
            ));
        }
        section.push('\n');
    }
    section
}

fn review_prompt(
    batch: &[ReviewItem],
    spans: &[ReviewedSpan],
    context: &[String],
    include_contract: bool,
    ledger: &LedgerSnapshot,
) -> String {
    let mut prompt = String::new();
    if include_contract {
        prompt.push_str("You are Loki, Mjolnir's one persistent read-only advisor. Take a different, user-aligned angle from Thor and implementation Eitri and verify assumptions when useful. You do not observe read-only Explore runs. Do not restate failures they already know. Stay silent for style, uncertainty, optional improvements, incomplete work, or activity that is on track. The advise tool accepts one structured list containing at most one note for each exact reviewed (god, ordinal) step. Advice is queued for the associated role and pulled at natural stopping points, never delivered as an interruption, so reserve it for material correctness, safety, scope, or strategy problems that remain worth raising even if later work may have already addressed them. Never implement changes yourself. Advice is not delivered the moment you submit it: it is handed to its target only at that role's next turn boundary. Every review prompt you receive includes a \"Your advice ledger\" section reporting what happened to your recent advice. A note listed there as QUEUED has already been accepted and WILL be delivered at the next boundary -- do not resubmit it; near-duplicate resubmissions can overflow the queue and silently drop other advice. Only resubmit a note if the ledger shows its submission was REJECTED (and you have fixed the rejection reason) or if materially new information changes the advice.\n\n");
        prompt.push_str("Three additional patterns are always material even though they look like passing verification, and you see the terminal commands and tool titles needed to catch them in your checkpoints. Regenerating golden files or snapshots (for example `-update` or snapshot-update flags) to match new output instead of investigating the mismatch is self-referential, not verification. Declaring a multi-part spec complete when the new feature's own tests were never executed is not verification, even when build, lint, or pre-existing suites passed. Narrowing the test-run scope after a failure (for example switching to `-run` filters that exclude previously failing or hanging tests) and then claiming completeness without disclosing the exclusion is also material.\n\n");
    }
    for context in context {
        prompt.push_str(context);
        prompt.push_str("\n\n");
    }
    let ledger_section = render_ledger(ledger);
    if !ledger_section.is_empty() {
        prompt.push_str(&ledger_section);
        prompt.push('\n');
    }
    prompt.push_str("### Chronological session update\n\n");
    for item in batch {
        let span = spans
            .iter()
            .find(|span| {
                span.target == item.target
                    && span.invocation == item.invocation
                    && span.first_step == item.checkpoint.step
            })
            .expect("batch span");
        prompt.push_str(&format!(
            "[{}]\n{}\n\n",
            span.marker(),
            item.checkpoint.text
        ));
    }
    prompt.push_str("Consider only these new checkpoints in light of your existing context. If material actionable guidance is needed, call advise once with a list containing at most one note for each exact (god, ordinal) step above. Otherwise finish silently.");
    prompt
}

fn is_content_free(note: &str) -> bool {
    matches!(
        note.trim()
            .trim_end_matches(['.', '!'])
            .to_ascii_lowercase()
            .as_str(),
        "looks good" | "good" | "no issues" | "no issue" | "nothing to add" | "all good" | "lgtm"
    )
}

pub fn format_deferred(advice: &[Advice], current_epoch: u64) -> String {
    advice
        .iter()
        .map(|advice| advice.deferred_text(current_epoch))
        .collect::<Vec<_>>()
        .join("\n\n")
}

pub fn format_pull_outcome(
    outcome: &PullOutcome,
    current_epoch: u64,
    consumer: Consumer,
) -> String {
    let queues = match consumer {
        Consumer::Thor => "Thor and Eitri",
        Consumer::Eitri => "Eitri",
    };
    let mut sections = vec![format!(
        "Loki advice receipt: drained {queues} queues ({} note{}).",
        outcome.advice.len(),
        if outcome.advice.len() == 1 { "" } else { "s" }
    )];
    if outcome.dropped > 0 {
        sections.push(format!(
            "Warning: {} unseen Loki advice note{} dropped because the queue reached its limit. Pull advice more often.",
            outcome.dropped,
            if outcome.dropped == 1 { " was" } else { "s were" }
        ));
    }
    if !outcome.advice.is_empty() {
        sections.push(format_deferred(&outcome.advice, current_epoch));
        // Verification protocol: offline loss-point replays showed the
        // dominant advice-uptake failure is concluding "already resolved"
        // from insufficient evidence (narrow greps, truncated reads, trusting
        // quoted test results). Appending this protocol cut declare-resolved-
        // without-evidence from 91% to 55% and quadrupled execute-first
        // responses in those replays.
        sections.push(
            "Verification protocol for these notes: (1) To conclude any note is already \
             resolved, run a test or reproduction that would fail if the note were still \
             valid; reading or searching the code is not sufficient evidence. (2) If a note \
             names one instance of a repeating pattern (one dialect, one branch, one parser \
             production), enumerate and check every sibling instance before declaring \
             resolution. (3) Never rely on test results quoted in this conversation, \
             including in this receipt; re-run them yourself before citing them. (4) A \
             rebuttal must quote the exact source text (file line or spec sentence) it \
             relies on; if the disagreement reduces to an ambiguity, write a discriminating \
             test instead of arguing."
                .to_string(),
        );
    }
    sections.join("\n\n")
}

fn identity(role: &ResolvedRole) -> LokiIdentity {
    LokiIdentity {
        role: "Loki".to_string(),
        connection_id: "loki".to_string(),
        source_id: Some(role.launch.source_id.clone()),
        model_name: Some(role.model.model.clone()),
        model_value: Some(role.model_value.clone()),
    }
}

fn emit_warning(ui_tx: &mpsc::UnboundedSender<UiEvent>, role: &ResolvedRole, message: String) {
    let _ = ui_tx.send(UiEvent::LokiActivity(LokiActivity::Warning {
        actor: identity(role),
        message,
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{SessionUpdate, ToolCall, ToolCallStatus};

    fn test_handle(epoch: u64) -> (Handle, mpsc::UnboundedReceiver<Request>) {
        let (requests, request_rx) = mpsc::unbounded_channel();
        let (abort, _) = watch::channel(false);
        let (_, finished) = watch::channel(false);
        let (_, posted) = watch::channel(0_u64);
        let (_, finished_reviews) = watch::channel(0_u64);
        let (_, review_started) = watch::channel(0_u64);
        (
            Handle {
                requests,
                ids: Arc::new(AtomicU64::new(1)),
                epochs: Arc::new(AtomicU64::new(epoch.saturating_add(1))),
                eitri_invocations: Arc::new(AtomicU64::new(1)),
                thor_steps: Arc::new(AtomicU64::new(1)),
                eitri_steps: Arc::new(AtomicU64::new(1)),
                thor_cadence_steps: Arc::new(AtomicU64::new(0)),
                eitri_cadence_steps: Arc::new(AtomicU64::new(0)),
                pending_thor: Arc::new(AtomicU64::new(0)),
                pending_eitri: Arc::new(AtomicU64::new(0)),
                last_thor_pull: Arc::new(AtomicU64::new(u64::MAX)),
                last_eitri_pull: Arc::new(AtomicU64::new(u64::MAX)),
                advice_state: SharedAdviceState::default(),
                active: Arc::default(),
                abort,
                finished,
                posted,
                finished_reviews,
                review_started,
            },
            request_rx,
        )
    }

    #[tokio::test]
    async fn advice_tool_slot_accepts_one_material_note_per_update() {
        let slot = AdviceSlot::default();
        let span = ReviewedSpan {
            target: Target::Eitri,
            invocation: Some(1),
            first_step: 4,
            last_step: 4,
            activities: vec!["cargo test".to_string()],
        };
        slot.begin(4, 2, vec![span]).await;
        let advice = slot
            .accept(AdviseArgs {
                advice: vec![AdviseItem {
                    step: StepRef {
                        god: Target::Eitri,
                        ordinal: 4,
                    },
                    note: "fix the race".to_string(),
                }],
            })
            .await
            .unwrap();
        assert_eq!(advice[0].epoch, 2);
        assert_eq!(advice[0].target, Target::Eitri);
        assert_eq!(advice[0].note, "fix the race");
        assert!(
            slot.accept(AdviseArgs {
                advice: vec![AdviseItem {
                    step: StepRef {
                        god: Target::Eitri,
                        ordinal: 4
                    },
                    note: "second note".to_string(),
                }],
            })
            .await
            .is_err()
        );
        assert_eq!(slot.finish(4).await[0].note, "fix the race");
    }

    #[tokio::test]
    async fn advice_tool_rejects_an_invalid_empty_item_before_queueing_valid_items() {
        let state = SharedAdviceState::default();
        let slot = AdviceSlot {
            active: Arc::default(),
            state: state.clone(),
            posted: None,
            finished_reviews: None,
        };
        slot.begin(
            4,
            2,
            vec![ReviewedSpan {
                target: Target::Thor,
                invocation: None,
                first_step: 4,
                last_step: 4,
                activities: vec!["cargo test".to_string()],
            }],
        )
        .await;

        let result = slot
            .accept(AdviseArgs {
                advice: vec![
                    AdviseItem {
                        step: StepRef {
                            god: Target::Thor,
                            ordinal: 4,
                        },
                        note: "fix the race".to_string(),
                    },
                    AdviseItem {
                        step: StepRef {
                            god: Target::Thor,
                            ordinal: 99,
                        },
                        note: String::new(),
                    },
                ],
            })
            .await;

        assert!(result.is_err());
        assert!(state.lock().unwrap().thor.is_empty());
        assert!(slot.finish(4).await.is_empty());
    }

    #[tokio::test]
    async fn gate_accepts_a_step_reviewed_earlier_this_epoch_and_still_rejects_never_reviewed_steps()
     {
        let state = SharedAdviceState::default();
        let slot = AdviceSlot {
            active: Arc::default(),
            state: state.clone(),
            posted: None,
            finished_reviews: None,
        };
        let span = |ordinal| ReviewedSpan {
            target: Target::Thor,
            invocation: None,
            first_step: ordinal,
            last_step: ordinal,
            activities: vec!["edit".into()],
        };

        // Batch 1 reviews step 1 only.
        slot.begin(1, 1, vec![span(1)]).await;
        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: 1,
                },
                note: "first diagnosis".to_string(),
            }],
        })
        .await
        .expect("step reviewed in this batch is accepted");
        slot.finish(1).await;

        // Batch 2, same epoch, reviews step 5 only -- but Loki re-raises the
        // unresolved issue keyed to step 1 from batch 1. That must now be
        // accepted because step 1 was reviewed earlier in this same epoch.
        slot.begin(2, 1, vec![span(5)]).await;
        let reraised = slot
            .accept(AdviseArgs {
                advice: vec![AdviseItem {
                    step: StepRef {
                        god: Target::Thor,
                        ordinal: 1,
                    },
                    note: "still unresolved".to_string(),
                }],
            })
            .await;
        assert!(
            reraised.is_ok(),
            "re-raising a step reviewed earlier this epoch must be accepted"
        );
        slot.finish(2).await;

        // Batch 3 tries to reference a step that was never reviewed at all
        // (not in this batch, not in any earlier batch this epoch).
        slot.begin(3, 1, vec![span(9)]).await;
        let never_reviewed = slot
            .accept(AdviseArgs {
                advice: vec![AdviseItem {
                    step: StepRef {
                        god: Target::Thor,
                        ordinal: 42,
                    },
                    note: "fabricated step".to_string(),
                }],
            })
            .await;
        assert_eq!(
            never_reviewed.unwrap_err(),
            "advice referenced a step outside this review batch"
        );
    }

    #[tokio::test]
    async fn dedup_replaces_a_queued_note_for_the_same_step_instead_of_growing_the_queue() {
        let state = SharedAdviceState::default();
        let slot = AdviceSlot {
            active: Arc::default(),
            state: state.clone(),
            posted: None,
            finished_reviews: None,
        };
        let span = |ordinal| ReviewedSpan {
            target: Target::Thor,
            invocation: None,
            first_step: ordinal,
            last_step: ordinal,
            activities: vec!["edit".into()],
        };

        slot.begin(1, 1, vec![span(1)]).await;
        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: 1,
                },
                note: "original text".to_string(),
            }],
        })
        .await
        .unwrap();
        slot.finish(1).await;
        assert_eq!(state.lock().unwrap().thor.len(), 1);

        // A later batch this epoch re-raises the same step with new text.
        // It must replace the queued note in place, not append a second one.
        slot.begin(2, 1, vec![span(2)]).await;
        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: 1,
                },
                note: "updated text".to_string(),
            }],
        })
        .await
        .unwrap();
        slot.finish(2).await;

        let guard = state.lock().unwrap();
        assert_eq!(guard.thor.len(), 1, "dedup must not grow the queue");
        assert_eq!(guard.thor[0].note, "updated text");
        assert!(
            guard.thor[0].updated,
            "replaced note must render as updated"
        );
        assert_eq!(
            guard.dropped_thor, 0,
            "an in-place update is not an overflow"
        );
    }

    #[tokio::test]
    async fn overflow_only_fires_for_genuinely_new_notes_on_a_full_queue() {
        let state = SharedAdviceState::default();
        let slot = AdviceSlot {
            active: Arc::default(),
            state: state.clone(),
            posted: None,
            finished_reviews: None,
        };
        let span = |ordinal| ReviewedSpan {
            target: Target::Thor,
            invocation: None,
            first_step: ordinal,
            last_step: ordinal,
            activities: vec!["edit".into()],
        };

        // Fill the queue to capacity with distinct steps.
        for ordinal in 1..=ADVICE_QUEUE_CAPACITY as u64 {
            slot.begin(ordinal, 1, vec![span(ordinal)]).await;
            slot.accept(AdviseArgs {
                advice: vec![AdviseItem {
                    step: StepRef {
                        god: Target::Thor,
                        ordinal,
                    },
                    note: format!("note {ordinal}"),
                }],
            })
            .await
            .unwrap();
            slot.finish(ordinal).await;
        }
        assert_eq!(state.lock().unwrap().thor.len(), ADVICE_QUEUE_CAPACITY);
        assert_eq!(state.lock().unwrap().dropped_thor, 0);

        // Re-raising a step already queued must dedup, not overflow, even
        // though the queue is full.
        slot.begin(100, 1, vec![span(1)]).await;
        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: 1,
                },
                note: "re-raised while full".to_string(),
            }],
        })
        .await
        .unwrap();
        slot.finish(100).await;
        assert_eq!(state.lock().unwrap().thor.len(), ADVICE_QUEUE_CAPACITY);
        assert_eq!(
            state.lock().unwrap().dropped_thor,
            0,
            "dedup on a full queue must not count as overflow"
        );

        // A genuinely new step on a full queue must still evict the oldest
        // and count as overflow.
        let new_ordinal = ADVICE_QUEUE_CAPACITY as u64 + 1;
        slot.begin(101, 1, vec![span(new_ordinal)]).await;
        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: new_ordinal,
                },
                note: "genuinely new".to_string(),
            }],
        })
        .await
        .unwrap();
        slot.finish(101).await;
        let guard = state.lock().unwrap();
        assert_eq!(guard.thor.len(), ADVICE_QUEUE_CAPACITY);
        assert_eq!(
            guard.dropped_thor, 1,
            "a genuinely new note on a full queue overflows"
        );
    }

    #[test]
    fn tool_reviews_only_run_at_completed_or_failed_boundaries() {
        let mut tracker = BoundaryTracker::default();
        let tool = |status| {
            UiEvent::SessionUpdate(SessionUpdate::ToolCall(
                ToolCall::new("tool-1", "build").status(status),
            ))
        };

        assert!(tracker.observe(&tool(ToolCallStatus::Pending)).is_none());
        assert!(tracker.observe(&tool(ToolCallStatus::InProgress)).is_none());
        assert!(tracker.observe(&tool(ToolCallStatus::Completed)).is_some());
        assert!(tracker.observe(&tool(ToolCallStatus::Failed)).is_some());
    }

    #[test]
    fn completed_tool_delta_uses_compact_omp_shape() {
        let mut tracker = BoundaryTracker::default();
        let event = UiEvent::SessionUpdate(SessionUpdate::ToolCall(
            ToolCall::new("tool-1", "run tests")
                .raw_input(serde_json::json!({"command": "cargo test"}))
                .raw_output(serde_json::json!({"exit": 1, "stderr": "boom"}))
                .status(ToolCallStatus::Failed),
        ));
        let delta = tracker.observe(&event).expect("completed tool boundary");
        assert_eq!(
            delta.text,
            "**agent**:\n→ run tests(cargo test) ⇒ error · 1 line — boom"
        );
        assert!(!delta.text.contains("stderr"));
    }

    #[test]
    fn every_completed_step_is_submitted_without_a_workspace_change_gate() {
        let (handle, mut requests) = test_handle(7);
        handle.observe(
            7,
            Target::Thor,
            None,
            Checkpoint {
                step: 1,
                text: "planning only".to_string(),
                activities: vec!["plan update".to_string()],
            },
        );
        let Request::Review {
            target, checkpoint, ..
        } = requests.try_recv().expect("review request")
        else {
            panic!("expected review request");
        };
        assert_eq!(target, Target::Thor);
        assert_eq!(checkpoint.text, "planning only");
    }

    #[test]
    fn step_ordinals_are_session_wide_and_independent_per_god() {
        let (handle, mut requests) = test_handle(1);
        let checkpoint = || Checkpoint {
            step: 99,
            text: "step".into(),
            activities: vec!["test".into()],
        };
        handle.observe(1, Target::Thor, None, checkpoint());
        handle.observe(1, Target::Eitri, Some(1), checkpoint());
        handle.observe(1, Target::Thor, None, checkpoint());
        handle.observe(1, Target::Eitri, Some(2), checkpoint());
        let ordinals = (0..4)
            .map(|_| match requests.try_recv().expect("review") {
                Request::Review {
                    target, checkpoint, ..
                } => (target, checkpoint.step),
                _ => panic!("expected review"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ordinals,
            [
                (Target::Thor, 1),
                (Target::Eitri, 1),
                (Target::Thor, 2),
                (Target::Eitri, 2),
            ]
        );
    }

    #[test]
    fn guardian_review_is_reviewable_without_advancing_pull_cadence() {
        let (handle, mut requests) = test_handle(1);
        let checkpoint = |activity: &str| Checkpoint {
            step: 99,
            text: activity.to_string(),
            activities: vec![activity.to_string()],
        };

        for activity in [
            "Guardian Review",
            "Editing files",
            "Guardian Review",
            "cargo test",
            "Guardian Review",
            "Editing files",
            "Guardian Review",
            "cargo test",
            "Guardian Review",
            "git status",
        ] {
            handle.observe(1, Target::Eitri, Some(1), checkpoint(activity));
        }

        let activities = (0..10)
            .map(|_| match requests.try_recv().expect("review") {
                Request::Review { checkpoint, .. } => checkpoint.activities[0].clone(),
                _ => panic!("expected review"),
            })
            .collect::<Vec<_>>();
        assert_eq!(activities.len(), 10);
        assert_eq!(
            activities
                .iter()
                .filter(|activity| activity.as_str() == "Guardian Review")
                .count(),
            5
        );
        assert_eq!(handle.eitri_steps.load(Ordering::Relaxed), 11);
        assert_eq!(handle.eitri_cadence_steps.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn reviewed_spans_preserve_each_exact_step() {
        let batch = vec![
            ReviewItem {
                id: 1,
                target: Target::Eitri,
                invocation: Some(2),
                checkpoint: Checkpoint {
                    step: 4,
                    text: "a".into(),
                    activities: vec!["rg".into()],
                },
            },
            ReviewItem {
                id: 2,
                target: Target::Eitri,
                invocation: Some(2),
                checkpoint: Checkpoint {
                    step: 5,
                    text: "b".into(),
                    activities: vec!["rg".into(), "cargo test".into()],
                },
            },
        ];
        let spans = reviewed_spans(&batch);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].first_step, 4);
        assert_eq!(spans[0].last_step, 4);
        assert_eq!(spans[0].activities, ["rg"]);
        assert_eq!(spans[0].marker(), "Eitri #2 · step 4: rg");
        assert_eq!(spans[1].first_step, 5);
        assert_eq!(spans[1].last_step, 5);
        assert_eq!(spans[1].activities, ["rg", "cargo test"]);
    }

    #[test]
    fn prompt_done_flushes_the_final_message_as_a_checkpoint() {
        use agent_client_protocol::schema::v1::{
            ContentBlock, ContentChunk, StopReason, TextContent,
        };

        let mut tracker = BoundaryTracker::default();
        let message = UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("done: the fix is in place")),
        )));
        assert!(tracker.observe(&message).is_none());

        let done = UiEvent::PromptDone {
            stop_reason: StopReason::EndTurn,
            usage: None,
        };
        let checkpoint = tracker.observe(&done).expect("final response checkpoint");
        assert_eq!(checkpoint.text, "**agent**:\ndone: the fix is in place");
        assert_eq!(checkpoint.activities, ["final response"]);

        // Nothing pending afterwards: a second completion yields no checkpoint.
        assert!(
            tracker
                .observe(&UiEvent::PromptDone {
                    stop_reason: StopReason::EndTurn,
                    usage: None,
                })
                .is_none()
        );
    }

    #[test]
    fn cancelled_prompt_done_does_not_flush_a_final_checkpoint() {
        use agent_client_protocol::schema::v1::{
            ContentBlock, ContentChunk, StopReason, TextContent,
        };

        let mut tracker = BoundaryTracker::default();
        let message = UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("partial")),
        )));
        assert!(tracker.observe(&message).is_none());
        assert!(
            tracker
                .observe(&UiEvent::PromptDone {
                    stop_reason: StopReason::Cancelled,
                    usage: None,
                })
                .is_none()
        );
    }

    #[test]
    fn message_and_thought_transitions_wait_for_a_semantic_checkpoint() {
        use agent_client_protocol::schema::v1::{ContentBlock, ContentChunk, TextContent};

        let mut tracker = BoundaryTracker::default();
        let message = UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("checking")),
        )));
        let thought = UiEvent::SessionUpdate(SessionUpdate::AgentThoughtChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new(" next")),
        )));
        assert!(tracker.observe(&message).is_none());
        assert!(tracker.observe(&thought).is_none());

        let tool = UiEvent::SessionUpdate(SessionUpdate::ToolCall(
            ToolCall::new("tool", "cargo test").status(ToolCallStatus::Completed),
        ));
        let checkpoint = tracker.observe(&tool).expect("tool checkpoint");
        assert!(checkpoint.text.contains("**agent**:\nchecking"));
        assert!(checkpoint.text.contains("_thinking:_  next"));
        assert_eq!(checkpoint.step, 1);
    }

    #[test]
    fn successful_tool_projects_shape_without_raw_result_body() {
        let tool = ToolCall::new("tool", "search")
            .raw_input(serde_json::json!({
                "action": {"type": "mcpToolCall", "toolName": "explore_agent"},
                "query": "find config"
            }))
            .raw_output(serde_json::json!({"result": "large successful body\nsecond line"}))
            .status(ToolCallStatus::Completed);
        let projected = render_tool_delta(&tool);
        assert_eq!(projected, "→ explore_agent(find config) ⇒ ok · 2 lines");
        assert!(!projected.contains("large successful body"));
    }

    #[test]
    fn tool_projection_includes_omp_intent_and_argument_formatting() {
        let tool = ToolCall::new("tool", "grep")
            .raw_input(serde_json::json!({
                "pattern": "Council\\s+role",
                "paths": ["src/loki.rs", "src/council.rs"],
                "i": "Find   the role declaration"
            }))
            .status(ToolCallStatus::Completed);

        assert_eq!(
            render_tool_delta(&tool),
            "// Find the role declaration\n→ grep(Council\\s+role @ src/loki.rs, src/council.rs) ⇒ ok · 0 lines"
        );
    }

    #[test]
    fn edit_projection_renders_a_unified_hunk_instead_of_file_snapshots() {
        use agent_client_protocol::schema::v1::{Diff, ToolCallContent};

        let old = [
            "far-start",
            "one",
            "two",
            "three",
            "old value",
            "five",
            "six",
            "seven",
            "eight",
            "far-end",
        ]
        .join("\n");
        let new = old.replace("old value", "new value");
        let tool = ToolCall::new("tool", "edit")
            .raw_input(serde_json::json!({"path": "src/lib.rs"}))
            .content(vec![ToolCallContent::Diff(
                Diff::new("src/lib.rs", new).old_text(old),
            )])
            .status(ToolCallStatus::Completed);

        let projected = render_tool_delta(&tool);
        assert!(projected.starts_with("→ edit(src/lib.rs) ⇒ ok · 0 lines\n```diff\n"));
        assert!(projected.contains("--- a/src/lib.rs\n+++ b/src/lib.rs"));
        assert!(projected.contains("-old value\n+new value"));
        assert!(!projected.contains("far-start"));
        assert!(!projected.contains("far-end"));
        assert!(projected.ends_with("```"));

        let review_projected = render_review_tool_delta(&tool);
        assert_eq!(review_projected, "→ edit(src/lib.rs) ⇒ ok · 0 lines");

        let mut tracker = BoundaryTracker::default();
        tracker
            .observe(&UiEvent::SessionUpdate(SessionUpdate::ToolCall(tool)))
            .expect("edit checkpoint");
        assert!(tracker.trajectory().contains("```diff"));
        assert!(!tracker.review_trajectory().contains("```diff"));
        assert!(
            tracker
                .review_trajectory()
                .contains("→ edit(src/lib.rs) ⇒ ok")
        );
    }

    #[test]
    fn new_file_and_large_replacement_diffs_are_complete() {
        use agent_client_protocol::schema::v1::Diff;

        let created = unified_diff(&Diff::new("src/new.rs", "fn main() {}\n"));
        assert!(created.contains("--- /dev/null\n+++ b/src/new.rs"));
        assert!(created.contains("+fn main() {}"));

        let old = (0..2_000)
            .map(|line| format!("old line {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        let new = (0..2_000)
            .map(|line| format!("new line {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        let replacement = unified_diff(&Diff::new("large.txt", new).old_text(old));
        assert!(replacement.len() > 16 * 1024);
        assert!(replacement.contains("-old line 1999"));
        assert!(replacement.contains("+new line 1999"));
        assert!(!replacement.contains("item truncated"));
    }

    #[test]
    fn diff_fence_outlasts_backticks_in_edited_content() {
        let fenced = fence_diff("@@ -1 +1 @@\n-```old\n+```new\n");
        assert!(fenced.starts_with("````diff\n"));
        assert!(fenced.ends_with("````"));
    }

    #[test]
    fn review_prompt_and_trajectory_are_not_context_truncated() {
        let text = "x".repeat(100 * 1024);
        let batch = vec![ReviewItem {
            id: 1,
            target: Target::Thor,
            invocation: None,
            checkpoint: Checkpoint {
                step: 1,
                text: text.clone(),
                activities: vec!["message".to_string()],
            },
        }];
        let spans = reviewed_spans(&batch);
        let prompt = review_prompt(&batch, &spans, &[], false, &LedgerSnapshot::default());
        assert!(prompt.len() > 100 * 1024);
        assert!(prompt.contains(&text));
        assert!(!prompt.contains("earlier context omitted"));

        let mut tracker = BoundaryTracker {
            trajectory: text.clone(),
            ..BoundaryTracker::default()
        };
        tracker.trajectory.push_str("tail");
        assert!(tracker.trajectory().starts_with(&text));
        assert!(tracker.trajectory().ends_with("tail"));
    }

    #[test]
    fn terminal_backed_tool_emits_only_the_terminal_exit_checkpoint() {
        use crate::event::TerminalOutputSnapshot;
        use agent_client_protocol::schema::v1::{Terminal, TerminalExitStatus, ToolCallContent};

        let mut tracker = BoundaryTracker::default();
        let pending = UiEvent::SessionUpdate(SessionUpdate::ToolCall(
            ToolCall::new("tool", "printf")
                .content(vec![ToolCallContent::Terminal(Terminal::new("term"))])
                .status(ToolCallStatus::InProgress),
        ));
        assert!(tracker.observe(&pending).is_none());
        let terminal = UiEvent::TerminalOutput(TerminalOutputSnapshot {
            terminal_id: "term".into(),
            output: "alpha\nbeta\n".into(),
            truncated: false,
            exit_status: Some(TerminalExitStatus::new().exit_code(0)),
        });
        let checkpoint = tracker.observe(&terminal).expect("terminal checkpoint");
        assert_eq!(checkpoint.step, 1);
        assert_eq!(checkpoint.text, "**agent**:\n→ printf() ⇒ ok · 3 lines");

        let completed = UiEvent::SessionUpdate(SessionUpdate::ToolCall(
            ToolCall::new("tool", "printf")
                .content(vec![ToolCallContent::Terminal(Terminal::new("term"))])
                .status(ToolCallStatus::Completed),
        ));
        assert!(tracker.observe(&completed).is_none());
    }

    #[tokio::test]
    async fn accepted_advice_queues_and_notifies_the_posted_watch() {
        let state = SharedAdviceState::default();
        let (posted_tx, mut posted) = watch::channel(0_u64);
        let slot = AdviceSlot {
            active: Arc::default(),
            state: state.clone(),
            posted: Some(posted_tx),
            finished_reviews: None,
        };
        let span = ReviewedSpan {
            target: Target::Thor,
            invocation: None,
            first_step: 2,
            last_step: 2,
            activities: vec!["cargo test".into()],
        };
        slot.begin(8, 3, vec![span]).await;
        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: 2,
                },
                note: "the test is destructive".into(),
            }],
        })
        .await
        .unwrap();

        assert!(posted.has_changed().unwrap());
        assert_eq!(*posted.borrow_and_update(), 1);
        let guard = state.lock().unwrap();
        let deferred = &guard.thor;
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].note, "the test is destructive");
    }

    #[test]
    fn thor_drain_merges_advice_across_turns_with_provenance_labels() {
        let (handle, _requests) = test_handle(4);
        {
            let mut state = handle.advice_state.lock().unwrap();
            let advice = |epoch, note: &str| Advice {
                id: epoch,
                epoch,
                target: Target::Thor,
                note: note.to_string(),
                span: ReviewedSpan {
                    target: Target::Thor,
                    invocation: None,
                    first_step: 1,
                    last_step: 1,
                    activities: vec!["cargo test".into()],
                },
                updated: false,
            };
            state.thor.push_back(advice(2, "old turn advice"));
            state.eitri.push_back(advice(4, "current turn advice"));
        }

        let taken = drain_for(&mut handle.advice_state.lock().unwrap(), Consumer::Thor).advice;
        assert_eq!(taken.len(), 2);
        assert!(
            drain_for(&mut handle.advice_state.lock().unwrap(), Consumer::Thor)
                .advice
                .is_empty()
        );

        let formatted = format_deferred(&taken, 4);
        assert!(formatted.contains("[reviewed in an earlier turn: Thor · step 1: cargo test]"));
        assert!(formatted.contains("old turn advice"));
        assert!(formatted.contains("[reviewed Thor · step 1: cargo test]"));
        assert!(formatted.contains("current turn advice"));
    }

    #[test]
    fn begin_turn_preserves_queued_advice_from_earlier_turns() {
        let (handle, mut requests) = test_handle(1);
        {
            let mut state = handle.advice_state.lock().unwrap();
            state.thor.push_back(Advice {
                id: 1,
                epoch: 1,
                target: Target::Thor,
                note: "still relevant".to_string(),
                span: ReviewedSpan {
                    target: Target::Thor,
                    invocation: None,
                    first_step: 1,
                    last_step: 1,
                    activities: vec!["edit".into()],
                },
                updated: false,
            });
        }

        let epoch = handle.begin_turn("next task".to_string());
        assert_eq!(epoch, 2);
        assert!(matches!(
            requests.try_recv().expect("begin request"),
            Request::Begin { epoch: 2, .. }
        ));
        assert_eq!(
            drain_for(&mut handle.advice_state.lock().unwrap(), Consumer::Thor)
                .advice
                .len(),
            1
        );
    }

    #[test]
    fn eitri_drain_leaves_thor_advice_and_thor_drain_preserves_global_order() {
        let advice = |id, target| Advice {
            id,
            epoch: 1,
            target,
            note: format!("note {id}"),
            span: ReviewedSpan {
                target,
                invocation: None,
                first_step: id,
                last_step: id,
                activities: vec!["test".into()],
            },
            updated: false,
        };
        let mut state = AdviceState::default();
        state.thor.push_back(advice(2, Target::Thor));
        state.eitri.push_back(advice(1, Target::Eitri));
        state.eitri.push_back(advice(3, Target::Eitri));

        let eitri = drain_for(&mut state, Consumer::Eitri);
        assert_eq!(
            eitri.advice.iter().map(|note| note.id).collect::<Vec<_>>(),
            [1, 3]
        );
        assert_eq!(state.thor.len(), 1);

        state.eitri.push_back(advice(4, Target::Eitri));
        let thor = drain_for(&mut state, Consumer::Thor);
        assert_eq!(
            thor.advice.iter().map(|note| note.id).collect::<Vec<_>>(),
            [2, 4]
        );
    }

    #[test]
    fn bounded_queue_drops_oldest_and_reports_overflow() {
        let mut queue = VecDeque::new();
        let mut dropped = 0;
        for id in 1..=ADVICE_QUEUE_CAPACITY as u64 + 2 {
            push_bounded(
                &mut queue,
                &mut dropped,
                Advice {
                    id,
                    epoch: 1,
                    target: Target::Thor,
                    note: format!("note {id}"),
                    span: ReviewedSpan {
                        target: Target::Thor,
                        invocation: None,
                        first_step: id,
                        last_step: id,
                        activities: vec!["test".into()],
                    },
                    updated: false,
                },
            );
        }
        assert_eq!(queue.len(), ADVICE_QUEUE_CAPACITY);
        assert_eq!(queue.front().expect("oldest retained").id, 3);
        assert_eq!(dropped, 2);
    }

    #[test]
    fn pull_advice_tool_completion_is_not_a_semantic_step() {
        let mut tracker = BoundaryTracker::default();
        let pull = UiEvent::SessionUpdate(SessionUpdate::ToolCall(
            ToolCall::new("pull", "pull_advice").status(ToolCallStatus::Completed),
        ));
        assert!(tracker.observe(&pull).is_none());
    }

    #[test]
    fn compaction_signal_is_not_a_semantic_step() {
        let mut tracker = BoundaryTracker::default();

        assert!(tracker.observe(&UiEvent::ContextCompacted).is_none());
    }

    #[test]
    fn pull_server_registers_only_the_role_scoped_pull_tool() {
        let router = PullMcpHandler::tool_router();
        assert!(router.get("pull_advice").is_some());
        assert_eq!(router.list_all().len(), 1);
    }

    #[tokio::test]
    async fn pull_waits_for_the_snapshotted_review_to_finish_before_draining() {
        let (mut handle, _requests) = test_handle(1);
        let (finished_tx, finished_rx) = watch::channel(0_u64);
        handle.finished_reviews = finished_rx;
        let slot = AdviceSlot {
            active: handle.active.clone(),
            state: handle.advice_state.clone(),
            posted: None,
            finished_reviews: Some(finished_tx),
        };
        let span = ReviewedSpan {
            target: Target::Thor,
            invocation: None,
            first_step: 1,
            last_step: 1,
            activities: vec!["test".into()],
        };
        slot.begin(7, 1, vec![span]).await;
        let pulling = tokio::spawn({
            let handle = handle.clone();
            async move { handle.pull(Consumer::Thor).await }
        });
        tokio::task::yield_now().await;
        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: 1,
                },
                note: "material advice".into(),
            }],
        })
        .await
        .unwrap();
        assert!(
            !pulling.is_finished(),
            "accepting advice alone must not release the pull"
        );
        slot.finish(7).await;
        let outcome = tokio::time::timeout(Duration::from_secs(1), pulling)
            .await
            .expect("pull released")
            .expect("pull task");
        assert!(outcome.waited);
        assert_eq!(outcome.advice[0].note, "material advice");
    }

    #[tokio::test]
    async fn pull_waits_for_a_pending_review_blocked_on_loki_startup() {
        let (mut handle, _requests) = test_handle(1);
        let (started_tx, started_rx) = watch::channel(0_u64);
        let (finished_tx, finished_rx) = watch::channel(0_u64);
        handle.review_started = started_rx;
        handle.finished_reviews = finished_rx;
        handle.pending_thor.store(1, Ordering::Relaxed);
        let slot = AdviceSlot {
            active: handle.active.clone(),
            state: handle.advice_state.clone(),
            posted: None,
            finished_reviews: Some(finished_tx),
        };
        let pulling = tokio::spawn({
            let handle = handle.clone();
            async move { handle.pull(Consumer::Thor).await }
        });
        tokio::task::yield_now().await;
        assert!(!pulling.is_finished());

        slot.begin(
            8,
            1,
            vec![ReviewedSpan {
                target: Target::Thor,
                invocation: None,
                first_step: 1,
                last_step: 1,
                activities: vec!["test".into()],
            }],
        )
        .await;
        handle.pending_thor.store(0, Ordering::Relaxed);
        let _ = started_tx.send(8);
        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: 1,
                },
                note: "startup-delayed advice".into(),
            }],
        })
        .await
        .unwrap();
        slot.finish(8).await;
        let outcome = tokio::time::timeout(Duration::from_secs(1), pulling)
            .await
            .expect("pull released")
            .expect("pull task");
        assert!(outcome.waited);
        assert_eq!(outcome.advice[0].note, "startup-delayed advice");
    }

    #[tokio::test]
    async fn rendezvous_flushes_then_waits_for_an_in_flight_review_and_delivers_one_digest() {
        let (mut handle, mut requests) = test_handle(1);
        let (finished_tx, finished_rx) = watch::channel(0_u64);
        handle.finished_reviews = finished_rx;
        let slot = AdviceSlot {
            active: handle.active.clone(),
            state: handle.advice_state.clone(),
            posted: None,
            finished_reviews: Some(finished_tx),
        };
        let span = ReviewedSpan {
            target: Target::Thor,
            invocation: None,
            first_step: 1,
            last_step: 1,
            activities: vec!["test".into()],
        };
        // A review is already active (e.g. dispatched by an earlier
        // worker wake-up) before Thor reaches the injection point.
        slot.begin(7, 1, vec![span]).await;

        let rendezvousing = tokio::spawn({
            let handle = handle.clone();
            async move { handle.rendezvous(Consumer::Thor).await }
        });
        tokio::task::yield_now().await;
        // The rendezvous unconditionally sends a flush barrier before it
        // waits, even though there is nothing new to classify here. Ack it
        // immediately, as the real worker would once it drains its request
        // channel up to this point.
        match requests.try_recv() {
            Ok(Request::Flush { epoch: 1, ack }) => {
                let _ = ack.send(());
            }
            _ => panic!("expected Request::Flush {{ epoch: 1, .. }}"),
        }
        // Let the rendezvous task actually run past the ack and reach its
        // `finished_reviews` wait before this (single-threaded) test task
        // moves on to `slot.finish` below -- otherwise `finish` (which
        // clears `active`) could run first and the rendezvous would only
        // ever observe the review as already gone, never as active.
        tokio::task::yield_now().await;

        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: 1,
                },
                note: "material advice".into(),
            }],
        })
        .await
        .unwrap();
        assert!(
            !rendezvousing.is_finished(),
            "accepting advice alone must not release the rendezvous"
        );
        slot.finish(7).await;

        let outcome = tokio::time::timeout(Duration::from_secs(1), rendezvousing)
            .await
            .expect("rendezvous released")
            .expect("rendezvous task");
        assert!(outcome.waited);
        assert_eq!(outcome.advice.len(), 1);
        assert_eq!(outcome.advice[0].note, "material advice");
    }

    /// Regression-shaped test (adapted from the grouped-review design this
    /// barrier is ported from) for the specific failure mode the ack guards
    /// against: `rendezvous_bounded` must not conclude "nothing pending" by
    /// reading `has_pending_review`/`active_review_relevant` before the
    /// worker has drained its request channel up to the flush. Modeled here
    /// without a real worker: nothing is pending or active by the pure
    /// counter/watch state at the moment `rendezvous_bounded` is called
    /// (exactly what a not-yet-processed flush looks like from the caller's
    /// side), and the implementation must still not return until the
    /// flush's `ack` resolves -- only then may it conclude there was
    /// nothing to wait for, or (as simulated here) go on to observe the
    /// dispatch the flush's caught-up worker performs and deliver its
    /// advice.
    #[tokio::test]
    async fn rendezvous_waits_for_the_flush_ack_before_concluding_nothing_is_pending() {
        let (mut handle, mut requests) = test_handle(1);
        let (started_tx, started_rx) = watch::channel(0_u64);
        let (finished_tx, finished_rx) = watch::channel(0_u64);
        handle.review_started = started_rx;
        handle.finished_reviews = finished_rx;
        // Nothing is pending or active by the counters/watches a
        // not-yet-processed flush would leave untouched: this is exactly
        // the state that made the buggy implementation return immediately.
        assert_eq!(handle.pending_thor.load(Ordering::Relaxed), 0);
        assert_eq!(handle.pending_eitri.load(Ordering::Relaxed), 0);

        let slot = AdviceSlot {
            active: handle.active.clone(),
            state: handle.advice_state.clone(),
            posted: None,
            finished_reviews: Some(finished_tx),
        };

        let rendezvousing = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle
                    .rendezvous_bounded(Consumer::Thor, Duration::from_millis(500))
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(
            !rendezvousing.is_finished(),
            "must not conclude 'nothing pending' before the flush's ack resolves, \
             even though has_pending_review/active_review_relevant read empty right now"
        );

        let ack = match requests.try_recv() {
            Ok(Request::Flush { epoch: 1, ack }) => ack,
            _ => panic!("expected Request::Flush {{ epoch: 1, .. }}"),
        };
        // Hold the ack a bit longer to prove the wait is genuinely blocked
        // on it, not on some other timer.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !rendezvousing.is_finished(),
            "must still be waiting on the ack while the worker hasn't caught up yet"
        );

        // The worker "catches up": it drains its channel (releasing the
        // ack), then dispatches the review it picked up.
        let _ = ack.send(());
        let span = ReviewedSpan {
            target: Target::Thor,
            invocation: None,
            first_step: 1,
            last_step: 1,
            activities: vec!["test".into()],
        };
        slot.begin(9, 1, vec![span]).await;
        let _ = started_tx.send(9);
        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: 1,
                },
                note: "flushed group advice".into(),
            }],
        })
        .await
        .unwrap();
        slot.finish(9).await;

        let outcome = tokio::time::timeout(Duration::from_secs(1), rendezvousing)
            .await
            .expect("rendezvous released")
            .expect("rendezvous task");
        assert!(
            !outcome.advice.is_empty(),
            "must deliver the flushed review's advice"
        );
        assert_eq!(outcome.advice[0].note, "flushed group advice");
    }

    #[tokio::test]
    async fn rendezvous_timeout_delivers_already_queued_advice_without_hanging() {
        let (mut handle, _requests) = test_handle(1);
        let (_started_tx, started_rx) = watch::channel(0_u64);
        let (_finished_tx, finished_rx) = watch::channel(0_u64);
        handle.review_started = started_rx;
        handle.finished_reviews = finished_rx;
        // Advice already sitting in the queue from an earlier, already
        // finished review round.
        {
            let mut state = handle.advice_state.lock().unwrap();
            state.thor.push_back(Advice {
                id: 1,
                epoch: 1,
                target: Target::Thor,
                note: "already queued advice".into(),
                span: ReviewedSpan {
                    target: Target::Thor,
                    invocation: None,
                    first_step: 1,
                    last_step: 1,
                    activities: vec!["test".into()],
                },
                updated: false,
            });
        }
        // A second checkpoint is still pending and never starts reviewing
        // within the bound: `started_tx` is never sent.
        handle.pending_thor.store(1, Ordering::Relaxed);

        let start = tokio::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            handle.rendezvous_bounded(Consumer::Thor, Duration::from_millis(30)),
        )
        .await
        .expect("rendezvous must return by its own bound, not hang the caller");
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "rendezvous must not overrun its own timeout by more than test slack"
        );
        assert_eq!(outcome.advice.len(), 1);
        assert_eq!(outcome.advice[0].note, "already queued advice");
    }

    #[tokio::test]
    async fn handoff_cutoff_discards_late_thor_advice_but_keeps_eitri_advice() {
        let state = SharedAdviceState::default();
        state.lock().unwrap().thor_cutoff = 3;
        let slot = AdviceSlot {
            active: Arc::default(),
            state: state.clone(),
            posted: None,
            finished_reviews: None,
        };
        let span = |target| ReviewedSpan {
            target,
            invocation: None,
            first_step: 3,
            last_step: 3,
            activities: vec!["test".into()],
        };
        slot.begin(9, 1, vec![span(Target::Thor), span(Target::Eitri)])
            .await;
        slot.accept(AdviseArgs {
            advice: vec![
                AdviseItem {
                    step: StepRef {
                        god: Target::Thor,
                        ordinal: 3,
                    },
                    note: "stale Thor note".into(),
                },
                AdviseItem {
                    step: StepRef {
                        god: Target::Eitri,
                        ordinal: 3,
                    },
                    note: "current Eitri note".into(),
                },
            ],
        })
        .await
        .unwrap();
        let state = state.lock().unwrap();
        assert!(state.thor.is_empty());
        assert_eq!(state.eitri[0].note, "current Eitri note");
    }

    #[tokio::test]
    async fn ledger_renders_delivered_queued_and_rejected_categories() {
        let state = SharedAdviceState::default();
        let slot = AdviceSlot {
            active: Arc::default(),
            state: state.clone(),
            posted: None,
            finished_reviews: None,
        };

        slot.begin(
            1,
            1,
            vec![ReviewedSpan {
                target: Target::Thor,
                invocation: None,
                first_step: 1,
                last_step: 1,
                activities: vec!["edit".into()],
            }],
        )
        .await;

        // Rejected: references a step outside this review batch.
        let rejection = slot
            .accept(AdviseArgs {
                advice: vec![AdviseItem {
                    step: StepRef {
                        god: Target::Thor,
                        ordinal: 99,
                    },
                    note: "out of batch note".to_string(),
                }],
            })
            .await;
        assert!(rejection.is_err());

        // Queued: valid submission, never drained.
        slot.accept(AdviseArgs {
            advice: vec![AdviseItem {
                step: StepRef {
                    god: Target::Thor,
                    ordinal: 1,
                },
                note: "queued advice note".to_string(),
            }],
        })
        .await
        .unwrap();
        slot.finish(1).await;

        // Delivered: a note placed directly in Eitri's queue, then drained.
        {
            let mut guard = state.lock().unwrap();
            guard.eitri.push_back(Advice {
                id: 500,
                epoch: 1,
                target: Target::Eitri,
                note: "delivered advice note".to_string(),
                span: ReviewedSpan {
                    target: Target::Eitri,
                    invocation: None,
                    first_step: 2,
                    last_step: 2,
                    activities: vec!["edit".into()],
                },
                updated: false,
            });
            drain_for(&mut guard, Consumer::Eitri);
        }

        let ledger = {
            let mut guard = state.lock().unwrap();
            take_ledger_snapshot(&mut guard)
        };
        let rendered = render_ledger(&ledger);
        assert!(rendered.starts_with("### Your advice ledger"));
        assert!(rendered.contains("Delivered"));
        assert!(rendered.contains("#500 → Eitri: delivered advice note"));
        assert!(rendered.contains("Queued"));
        assert!(rendered.contains("→ Thor: queued advice note"));
        assert!(rendered.contains("Rejected"));
        assert!(rendered.contains("advice referenced a step outside this review batch"));
        assert!(rendered.contains("out of batch note"));
    }

    #[test]
    fn empty_ledger_omits_the_advice_ledger_section() {
        let ledger = LedgerSnapshot::default();
        assert!(render_ledger(&ledger).is_empty());
        let prompt = review_prompt(&[], &[], &[], false, &ledger);
        assert!(!prompt.contains("Your advice ledger"));
    }

    #[test]
    fn ledger_gives_full_outstanding_queue_after_compaction_not_just_deltas() {
        let mut state = AdviceState::default();
        state.thor.push_back(Advice {
            id: 1,
            epoch: 1,
            target: Target::Thor,
            note: "still-queued before compaction".to_string(),
            span: ReviewedSpan {
                target: Target::Thor,
                invocation: None,
                first_step: 1,
                last_step: 1,
                activities: vec!["edit".into()],
            },
            updated: false,
        });

        // A review prompt is built (the normal per-review snapshot). The
        // still-undelivered note is read, not drained: `queued` is always
        // the live queue, never a consumed delta.
        let first = take_ledger_snapshot(&mut state);
        assert_eq!(first.queued.len(), 1);

        // Loki's session compacts here. `/compact` only touches Loki's
        // *model* context; the ledger lives in `AdviceState` (mj-side, in
        // the worker), so it is untouched by compaction. The very next
        // review must still report the still-undelivered note in full, not
        // as an empty delta just because nothing new happened since the
        // prior snapshot.
        let second = take_ledger_snapshot(&mut state);
        assert_eq!(second.queued.len(), 1);
        assert_eq!(second.queued[0].note, "still-queued before compaction");
        assert!(render_ledger(&second).contains("still-queued before compaction"));
    }

    #[test]
    fn loki_priming_requires_a_successful_contract_prompt() {
        let mut state = LokiReviewState::default();
        assert_eq!(
            state.resume_session(),
            None,
            "initial connection starts new"
        );
        state
            .accept_session(Some(("first", false)))
            .expect("capture the initial ACP identity");
        assert_eq!(state.resume_session().as_deref(), Some("first"));
        assert!(state.include_contract());

        // Setup loss before the first prompt must leave the retry unprimed.
        assert!(state.include_contract());
        state
            .accept_session(Some(("first", true)))
            .expect("exact resume after setup loss");
        assert!(state.include_contract());

        state.contract_succeeded(state.include_contract());
        assert!(
            !state.include_contract(),
            "successful contract prompt primes Loki"
        );

        // A later prompt failure and its reconnect preserve the logical state.
        assert!(!state.include_contract());
        state
            .accept_session(Some(("first", true)))
            .expect("exact resume after prompt failure");
        assert!(!state.include_contract());
        state.contract_succeeded(state.include_contract());
        assert!(
            !state.include_contract(),
            "non-contract success stays primed"
        );

        let retained_id = state.resume_session();
        let prior_primed = state.primed;
        assert!(
            state.accept_session(Some(("first", false))).is_err(),
            "a replacement that starts new is rejected"
        );
        assert_eq!(state.resume_session(), retained_id);
        assert_eq!(state.primed, prior_primed);
        assert!(
            state.accept_session(Some(("other", true))).is_err(),
            "a different resumed ID is rejected"
        );
        assert_eq!(state.resume_session(), retained_id);
        assert_eq!(state.primed, prior_primed);

        // Begin only replaces pending per-turn context.
        let mut pending_context = HashMap::new();
        pending_context.insert((Target::Thor, Some(1)), "old per-turn context".to_string());
        begin_request(&mut pending_context, "new request".to_string());
        assert_eq!(pending_context.len(), 1);
        assert_eq!(state.resume_session(), retained_id);
        assert_eq!(state.primed, prior_primed);
        assert!(
            review_prompt(&[], &[], &[], true, &LedgerSnapshot::default())
                .contains("persistent read-only advisor"),
            "an initial session receives Loki's contract"
        );
        assert!(
            !review_prompt(&[], &[], &[], false, &LedgerSnapshot::default())
                .contains("persistent read-only advisor"),
            "a primed session does not repeat Loki's contract"
        );
    }

    #[test]
    fn contract_text_tells_loki_not_to_resubmit_queued_advice() {
        let contract = review_prompt(&[], &[], &[], true, &LedgerSnapshot::default());
        assert!(contract.contains("advice ledger"));
        assert!(contract.contains("do not resubmit"));
        assert!(contract.contains("REJECTED"));
        assert!(contract.contains("QUEUED"));
    }

    #[test]
    fn contract_text_flags_the_three_verification_laundering_patterns() {
        let contract = review_prompt(&[], &[], &[], true, &LedgerSnapshot::default());
        assert!(contract.contains(
            "Regenerating golden files or snapshots (for example `-update` or snapshot-update flags) to match new output instead of investigating the mismatch is self-referential, not verification."
        ));
        assert!(contract.contains(
            "Declaring a multi-part spec complete when the new feature's own tests were never executed is not verification, even when build, lint, or pre-existing suites passed."
        ));
        assert!(contract.contains(
            "Narrowing the test-run scope after a failure (for example switching to `-run` filters that exclude previously failing or hanging tests) and then claiming completeness without disclosing the exclusion is also material."
        ));
    }

    #[test]
    fn compact_threshold_coalesces_and_rearms_below_32k() {
        let mut threshold = CompactThreshold::default();

        assert!(!threshold.observe(31_999));
        assert!(threshold.observe(32_000));
        assert!(!threshold.observe(40_000));
        assert!(!threshold.observe(16_000));
        assert!(threshold.observe(32_001));
    }

    #[test]
    fn compact_threshold_retry_after_failure_is_capped_at_one_per_epoch() {
        let mut threshold = CompactThreshold::default();

        // Initial automatic fire.
        assert!(threshold.observe(40_000));
        // Compact failed; a retry is granted once for this epoch...
        assert!(threshold.rearm_for_retry(1));
        assert!(threshold.observe(40_000));
        // ...but a second failure in the same epoch does not get another.
        assert!(!threshold.rearm_for_retry(1));
        assert!(!threshold.observe(40_000));

        // A new epoch (the next Loki turn) earns a fresh retry.
        assert!(threshold.rearm_for_retry(2));
        assert!(threshold.observe(40_000));
    }

    #[test]
    fn compact_threshold_retry_is_superseded_by_a_genuine_usage_dip() {
        let mut threshold = CompactThreshold::default();

        assert!(threshold.observe(40_000));
        // Usage actually falling below the threshold re-arms independently
        // of the retry-cap bookkeeping.
        assert!(!threshold.observe(16_000));
        assert!(threshold.observe(40_000));
    }

    #[test]
    fn manual_compaction_has_priority_when_triggers_coalesce() {
        let requests = vec![
            (CompactTrigger::Loki128k, None),
            (CompactTrigger::ThorCompacted, None),
            (CompactTrigger::Manual, None),
        ];

        assert_eq!(
            preferred_compact_trigger(&requests),
            Some(CompactTrigger::Manual)
        );
    }

    #[test]
    fn automatic_compaction_is_enqueued_on_lokis_single_worker_lane() {
        let (handle, mut requests) = test_handle(1);

        handle.request_compact(CompactTrigger::ThorCompacted);

        assert!(matches!(
            requests.try_recv(),
            Ok(Request::Compact {
                trigger: CompactTrigger::ThorCompacted,
                responder: None,
            })
        ));
    }
}
