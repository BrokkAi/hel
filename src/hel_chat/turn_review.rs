//! The chat's turn-review view: the split pane a completed turn is reviewed in.
//!
//! This is the sibling of `second_opinion.rs`. Both put a reviewer's
//! conversation beside the primary's and replace the composer with a row of
//! actions; they differ in what starts them and what the actions mean. Plan
//! review starts at a plan-approval decision mid-turn and ends by transferring
//! a critique. Turn review starts when a turn *finishes* and ends by forwarding
//! validated findings, dismissing them, or cancelling.
//!
//! The view owns the screen while it is up, which is the whole point: review is
//! synchronous, so findings can never land out of the blue in the middle of the
//! next conversation. The rules of the review itself live in
//! `crate::hel_review::driver`; this module is the keyboard, the pane, and the
//! action bar.

use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::hel_review::driver::{
    ReviewRequest, Resolution, RoleState, TurnReviewDriver, TurnReviewPhase,
};
use crate::hel_second_opinion::{ReviewerSetup, SetupRequest};
use crate::hel_review::verdict::ReviewVerdict;

use super::second_opinion::ReviewerPane;

/// Which of the review's actions the keyboard is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReviewAction {
    Forward,
    Dismiss,
    Cancel,
}

impl ReviewAction {
    const ORDER: [Self; 3] = [Self::Forward, Self::Dismiss, Self::Cancel];

    fn label(self) -> &'static str {
        match self {
            Self::Forward => "Forward findings",
            Self::Dismiss => "Dismiss",
            Self::Cancel => "Cancel",
        }
    }

    fn next(self, delta: isize) -> Self {
        let position = Self::ORDER
            .iter()
            .position(|action| *action == self)
            .unwrap_or(0);
        let length = Self::ORDER.len();
        let moved = if delta.is_negative() {
            position.checked_sub(1).unwrap_or(length - 1)
        } else {
            (position + 1) % length
        };
        Self::ORDER[moved]
    }
}

/// One turn review on screen.
pub(super) struct TurnReview {
    pub(super) driver: TurnReviewDriver,
    pub(super) reviewer: ReviewerPane,
    pub(super) action: ReviewAction,
    /// The reviewer waterfall, while the user is choosing which harness
    /// reviews. It is skipped whenever the workspace already remembers one.
    pub(super) setup: Option<Box<ReviewerSetup>>,
    /// The role whose launch is waiting for that choice.
    pub(super) pending_role: Option<String>,
    /// A failure to report in place, rather than in a dialog that would take
    /// the review off screen with it.
    pub(super) failure: Option<String>,
}

impl std::fmt::Debug for TurnReview {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnReview")
            .field("phase", self.driver.phase())
            .field("action", &self.action)
            .field("choosing_reviewer", &self.setup.is_some())
            .finish()
    }
}

impl TurnReview {
    #[must_use]
    pub(super) fn new(driver: TurnReviewDriver) -> Self {
        Self {
            driver,
            reviewer: ReviewerPane::default(),
            action: ReviewAction::Forward,
            setup: None,
            pending_role: None,
            failure: None,
        }
    }

    /// What the pane's status line says right now.
    #[must_use]
    pub(super) fn status(&self) -> String {
        if let Some(failure) = &self.failure {
            return failure.clone();
        }
        self.driver.status().to_string()
    }

    pub(super) fn report_failure(&mut self, message: impl Into<String>) {
        let message = message.into();
        match self.setup.as_mut() {
            Some(setup) => setup.probe_failed_current(message),
            None => self.failure = Some(message),
        }
    }
}

/// What the turn-review view asked the session to do.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnReviewIntent {
    /// Reviewer waterfall steps, in order.
    Setup(Vec<SetupRequest>),
    /// The user chose a reviewer; start the waiting role under it.
    Confirmed {
        profile_id: String,
        model: Option<String>,
        effort: Option<String>,
    },
    /// Work the review state machine asked for, in order.
    Requests(Vec<ReviewRequest>),
    /// The view closed with nothing further to do.
    Closed,
}

impl super::ChatState {
    /// Whether the turn-review view owns the screen. While it does, the
    /// composer is not accepting prompts for the primary agent.
    pub(super) fn turn_review_active(&self) -> bool {
        self.turn_review.is_some()
    }

    /// Whether the split is up, which is when the transcript shares the frame.
    pub(super) fn turn_review_split(&self) -> bool {
        self.turn_review
            .as_ref()
            .is_some_and(|review| review.setup.is_none())
    }

    pub(super) fn turn_review(&self) -> Option<&TurnReview> {
        self.turn_review.as_deref()
    }

    pub(super) fn turn_review_mut(&mut self) -> Option<&mut TurnReview> {
        self.turn_review.as_deref_mut()
    }

    /// Why a review cannot start right now, or `None` when it can.
    ///
    /// Every gate here keeps review synchronous and unsurprising. A review only
    /// starts from an idle session with an empty prompt queue, so it can hold
    /// the composer without stranding work the user has already sent, and never
    /// while another review owns the screen.
    pub(super) fn turn_review_blocker(&self) -> Option<&'static str> {
        if self.phase != crate::hel_worker::WorkerPhase::Idle {
            return Some("A review runs between turns; this one is still working");
        }
        if self.turn_review_active() {
            return Some("A review is already open");
        }
        if self.second_opinion_active() {
            return Some("A second opinion is already open");
        }
        if self.has_queued_prompts() {
            // Reviewing now would hold prompts the user has already sent. The
            // review after the queue drains covers the whole batch instead.
            return Some("Prompts are queued; the review waits for them");
        }
        None
    }

    pub(super) fn open_turn_review(&mut self, driver: TurnReviewDriver) {
        self.turn_review = Some(Box::new(TurnReview::new(driver)));
    }

    pub(super) fn close_turn_review(&mut self) {
        self.turn_review = None;
        self.turn_review_action_areas.clear();
    }

    /// Drives the turn-review view from one key press.
    pub(super) fn handle_turn_review_key(
        &mut self,
        code: crossterm::event::KeyCode,
        modifiers: crossterm::event::KeyModifiers,
    ) -> super::ChatAction {
        use crossterm::event::{KeyCode, KeyModifiers};

        let Some(review) = self.turn_review.as_mut() else {
            return super::ChatAction::None;
        };
        if let Some(setup) = review.setup.as_mut() {
            let outcome = match code {
                KeyCode::Up => {
                    setup.move_selection(-1);
                    return super::ChatAction::None;
                }
                KeyCode::Down => {
                    setup.move_selection(1);
                    return super::ChatAction::None;
                }
                KeyCode::Enter => setup.confirm(),
                KeyCode::Char('r') if setup.failure().is_some() => setup.retry(),
                KeyCode::Left | KeyCode::Backspace => setup.back(),
                KeyCode::Esc => setup.cancel(),
                KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => setup.cancel(),
                _ => return super::ChatAction::None,
            };
            return self.apply_turn_review_setup_outcome(outcome);
        }
        match code {
            KeyCode::Tab | KeyCode::Right => {
                review.action = review.action.next(1);
                super::ChatAction::None
            }
            KeyCode::BackTab | KeyCode::Left => {
                review.action = review.action.next(-1);
                super::ChatAction::None
            }
            KeyCode::PageUp => {
                let page = self.last_viewport_height.max(1);
                review.reviewer.scroll_by(-(page as isize), page);
                super::ChatAction::None
            }
            KeyCode::PageDown => {
                let page = self.last_viewport_height.max(1);
                review.reviewer.scroll_by(page as isize, page);
                super::ChatAction::None
            }
            KeyCode::Enter => self.activate_turn_review_action(),
            // Escape cancels at every stage before the review resolves, which
            // is what keeps the composer one keypress away.
            KeyCode::Esc => self.cancel_turn_review(),
            KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => {
                self.cancel_turn_review()
            }
            _ => super::ChatAction::None,
        }
    }

    fn apply_turn_review_setup_outcome(
        &mut self,
        outcome: crate::hel_second_opinion::SetupOutcome,
    ) -> super::ChatAction {
        use crate::hel_second_opinion::SetupOutcome;

        match outcome {
            SetupOutcome::None => super::ChatAction::None,
            SetupOutcome::Requests(requests) => {
                super::ChatAction::TurnReview(TurnReviewIntent::Setup(requests))
            }
            SetupOutcome::Confirmed { selection } => {
                if let Some(review) = self.turn_review.as_mut() {
                    review.setup = None;
                }
                super::ChatAction::TurnReview(TurnReviewIntent::Confirmed {
                    profile_id: selection.profile_id,
                    model: selection.model,
                    effort: selection.effort,
                })
            }
            // Abandoning the reviewer choice abandons the review, which leaves
            // the baseline alone: the next review covers this turn too.
            SetupOutcome::Cancelled { requests } => {
                let mut steps = self
                    .turn_review
                    .as_mut()
                    .map(|review| review.driver.cancel())
                    .unwrap_or_default();
                self.close_turn_review();
                for request in requests {
                    if let SetupRequest::CancelProbe { .. } = request {
                        steps.push(ReviewRequest::PauseReviewer);
                    }
                }
                super::ChatAction::TurnReview(TurnReviewIntent::Requests(steps))
            }
        }
    }

    fn activate_turn_review_action(&mut self) -> super::ChatAction {
        let Some(review) = self.turn_review.as_mut() else {
            return super::ChatAction::None;
        };
        let action = review.action;
        if action == ReviewAction::Forward && !review.driver.can_forward() {
            // Forwarding stays unavailable until a findings verdict exists;
            // pressing it early does nothing.
            return super::ChatAction::None;
        }
        if action != ReviewAction::Cancel && review.driver.verdict().is_none() {
            return super::ChatAction::None;
        }
        let requests = match action {
            ReviewAction::Forward => review.driver.forward(),
            ReviewAction::Dismiss => review.driver.dismiss(),
            ReviewAction::Cancel => review.driver.cancel(),
        };
        if requests.is_empty() {
            return super::ChatAction::None;
        }
        super::ChatAction::TurnReview(TurnReviewIntent::Requests(requests))
    }

    fn cancel_turn_review(&mut self) -> super::ChatAction {
        let Some(review) = self.turn_review.as_mut() else {
            return super::ChatAction::None;
        };
        let requests = review.driver.cancel();
        super::ChatAction::TurnReview(TurnReviewIntent::Requests(requests))
    }

    /// Activates the review action under the pointer, if any.
    pub(super) fn click_turn_review_action(&mut self, column: u16, row: u16) -> super::ChatAction {
        let Some(action) = self
            .turn_review_action_areas
            .iter()
            .find(|(_, area)| area.contains(ratatui::layout::Position::new(column, row)))
            .map(|(action, _)| *action)
        else {
            return super::ChatAction::None;
        };
        let Some(review) = self.turn_review.as_mut() else {
            return super::ChatAction::None;
        };
        review.action = action;
        self.activate_turn_review_action()
    }

    /// Scrolls the reviewer pane of an open turn review.
    pub(super) fn scroll_turn_review(&mut self, rows: isize) -> bool {
        let height = self.last_viewport_height.max(1);
        let Some(review) = self.turn_review.as_mut() else {
            return false;
        };
        review.reviewer.scroll_by(rows, height);
        true
    }
}

/// Draws the review's action bar and reports where each button landed, so a
/// click picks the same action the keyboard would.
pub(super) fn render_turn_review_actions(
    frame: &mut ratatui::Frame,
    area: Rect,
    review: &TurnReview,
    status: &str,
) -> Vec<(ReviewAction, Rect)> {
    let mut spans = Vec::new();
    let mut buttons = Vec::new();
    let mut column = area.x;
    let has_verdict = review.driver.verdict().is_some();
    for candidate in ReviewAction::ORDER {
        let available = match candidate {
            ReviewAction::Forward => review.driver.can_forward(),
            ReviewAction::Dismiss => has_verdict,
            ReviewAction::Cancel => true,
        };
        let mut style = Style::default();
        if !available {
            style = style.fg(Color::DarkGray);
        }
        if candidate == review.action {
            style = style.add_modifier(Modifier::REVERSED);
        }
        let label = format!(" {} ", candidate.label());
        let width = u16::try_from(label.chars().count()).unwrap_or(u16::MAX);
        if column < area.right() {
            buttons.push((
                candidate,
                Rect::new(column, area.y, width.min(area.right() - column), 1),
            ));
        }
        column = column.saturating_add(width).saturating_add(2);
        spans.push(Span::styled(label, style));
        spans.push(Span::raw("  "));
    }
    spans.push(Span::styled(
        status.to_owned(),
        Style::default().fg(Color::DarkGray),
    ));
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
    buttons
}

/// The one-line strip above the reviewer transcript: which reviewing agents
/// this review is running and where each has got to.
#[must_use]
pub(super) fn role_strip(review: &TurnReview) -> Option<Line<'static>> {
    let roles = review.driver.roles();
    if roles.is_empty() {
        return None;
    }
    let mut spans = Vec::new();
    for (index, role) in roles.iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw("  "));
        }
        let color = match role.state {
            RoleState::Pending => Color::DarkGray,
            RoleState::Running => Color::Yellow,
            RoleState::Clean => Color::Green,
            RoleState::Findings => Color::LightMagenta,
            RoleState::Failed => Color::Red,
        };
        spans.push(Span::styled(
            format!("{} {}", role.label, role.state.label()),
            Style::default().fg(color),
        ));
    }
    Some(Line::from(spans))
}

/// A one-line note for the primary transcript when a review closes itself.
#[must_use]
pub fn resolution_notice(phase: &TurnReviewPhase) -> Option<String> {
    let TurnReviewPhase::Resolved(resolution) = phase else {
        return None;
    };
    Some(match resolution {
        Resolution::Forwarded => "Review findings sent to the agent".to_string(),
        Resolution::Dismissed => "Review dismissed".to_string(),
        Resolution::Cancelled => "Review cancelled".to_string(),
        Resolution::NothingToReview => "Nothing to review: the turn changed no files".to_string(),
    })
}

/// What the reviewer pane's title says while a verdict is up.
#[must_use]
pub(super) fn verdict_title(verdict: Option<&ReviewVerdict>) -> &'static str {
    match verdict {
        Some(ReviewVerdict::Clean) => " Turn review · clean ",
        Some(ReviewVerdict::Findings { .. }) => " Turn review · findings ",
        Some(ReviewVerdict::Failed { .. }) => " Turn review · failed ",
        None => " Turn review ",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_chat::ChatAction;
    use crate::hel_chat::test_support::{key, queued, snapshot};
    use crate::hel_review::driver::{TurnReviewDriver, TurnReviewSeed};
    use crate::hel_review::lanes::{ReviewTier, UserMessage};
    use crate::hel_worker::WorkerPhase;
    use crossterm::event::KeyCode;

    fn chat() -> super::super::ChatState {
        super::super::ChatState::new(&snapshot(), &[])
    }

    fn driver() -> TurnReviewDriver {
        TurnReviewDriver::start(TurnReviewSeed {
            tier: ReviewTier::Quick,
            task: "add a retry".to_string(),
            user_messages: vec![UserMessage::prompt("add a retry")],
            initial_result: "done".to_string(),
            trajectory: String::new(),
            baselines: Default::default(),
            through_ordinal: 7,
            prior_review: None,
        })
        .0
    }

    #[test]
    fn turn_completion_with_queued_prompts_does_not_trigger_review() {
        let mut chat = chat();
        assert_eq!(
            chat.turn_review_blocker(),
            None,
            "an idle session with an empty queue is reviewable"
        );
        chat.queued_prompts.push_back(queued("queued-1", "next"));
        assert_eq!(
            chat.turn_review_blocker(),
            Some("Prompts are queued; the review waits for them"),
            "holding the composer would strand a prompt the user already sent"
        );
    }

    #[test]
    fn a_busy_session_and_an_open_review_both_refuse_a_second_review() {
        let mut chat = chat();
        chat.phase = WorkerPhase::Running;
        assert_eq!(
            chat.turn_review_blocker(),
            Some("A review runs between turns; this one is still working")
        );
        chat.phase = WorkerPhase::Idle;
        chat.open_turn_review(driver());
        assert_eq!(chat.turn_review_blocker(), Some("A review is already open"));
    }

    #[test]
    fn submission_refused_while_review_unresolved() {
        let mut chat = chat();
        chat.open_turn_review(driver());
        // Typed input never reaches the composer: the review owns the keyboard
        // while it is up.
        assert_eq!(chat.handle_key(key(KeyCode::Char('h'))), ChatAction::None);
        assert!(chat.input.is_empty(), "the composer takes no input");

        // Even a prompt that arrives another way (a queued prompt peeled back,
        // a remote submit) is refused while the review is unresolved.
        chat.input = "next".to_string();
        chat.input_cursor = 4;
        assert_eq!(chat.submit_input(), ChatAction::None);
        assert!(
            chat.notice()
                .is_some_and(|notice| notice.contains("review of the last turn is open")),
            "the refusal says why: {:?}",
            chat.notice()
        );
    }

    #[test]
    fn escape_cancels_the_review_and_gives_the_composer_back() {
        let mut chat = chat();
        chat.open_turn_review(driver());
        let action = chat.handle_key(key(KeyCode::Esc));
        let ChatAction::TurnReview(TurnReviewIntent::Requests(requests)) = action else {
            panic!("escape cancels the review, got {action:?}");
        };
        assert_eq!(
            requests,
            vec![ReviewRequest::PauseReviewer, ReviewRequest::Close],
            "cancelling stops the reviewer and never advances the baseline"
        );
    }

    #[test]
    fn the_action_bar_offers_nothing_until_a_verdict_arrives() {
        let mut chat = chat();
        chat.open_turn_review(driver());
        // Tab cycles the highlight, but neither Forward nor Dismiss does
        // anything while the reviewer is still working.
        assert_eq!(chat.handle_key(key(KeyCode::Enter)), ChatAction::None);
        assert_eq!(chat.handle_key(key(KeyCode::Tab)), ChatAction::None);
        assert_eq!(chat.handle_key(key(KeyCode::Enter)), ChatAction::None);
        assert!(chat.turn_review_active(), "the review is still up");
    }

    #[test]
    fn a_resolution_notice_names_what_happened() {
        assert_eq!(
            resolution_notice(&TurnReviewPhase::Resolved(Resolution::Forwarded)).as_deref(),
            Some("Review findings sent to the agent")
        );
        assert_eq!(
            resolution_notice(&TurnReviewPhase::Resolved(Resolution::NothingToReview)).as_deref(),
            Some("Nothing to review: the turn changed no files")
        );
        assert_eq!(resolution_notice(&TurnReviewPhase::CapturingDelta), None);
    }
}
