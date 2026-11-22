#![deny(clippy::wildcard_enum_match_arm)]

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use crate::fixture_wire::Reply;

#[derive(Clone, Copy)]
pub enum ResponsePhase {
    Inspect,
    Observe,
    Streaming,
    Finished,
}

impl StateTag for ResponsePhase {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Inspect => "inspect",
            Self::Observe => "observe",
            Self::Streaming => "streaming",
            Self::Finished => "finished",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::Finished)
    }
}

#[derive(Clone, Copy)]
pub enum ReplyKind {
    SetupFailure,
    InspectResult,
    ObservationItem,
    ObservationEnd,
}

impl EventTag for ReplyKind {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::SetupFailure => "setupFailure",
            Self::InspectResult => "inspectResult",
            Self::ObservationItem => "observationItem",
            Self::ObservationEnd => "observationEnd",
        }
    }
}

impl From<&Reply> for ReplyKind {
    fn from(reply: &Reply) -> Self {
        match reply {
            Reply::Refused { .. } | Reply::SourceError { .. } => Self::SetupFailure,
            Reply::InspectResult { .. } => Self::InspectResult,
            Reply::ObservationItem { .. } => Self::ObservationItem,
            Reply::ObservationEnd { .. } => Self::ObservationEnd,
        }
    }
}

pub enum NoDomainRefusal {}

impl RefusalTag for NoDomainRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

pub struct CallLifecycle;

impl StateMachine for CallLifecycle {
    type State = ResponsePhase;
    type Event = ReplyKind;
    type Effect = ();
    type Error = NoDomainRefusal;

    fn transition(phase: &Self::State, kind: Self::Event) -> StateMachineResult<Self> {
        use ReplyKind::{InspectResult, ObservationEnd, ObservationItem, SetupFailure};
        use ResponsePhase::{Finished, Inspect, Observe, Streaming};
        match (*phase, kind) {
            (Inspect, InspectResult | SetupFailure)
            | (Observe, SetupFailure | ObservationEnd)
            | (Streaming, ObservationEnd) => Ok((Finished, vec![])),
            (Observe | Streaming, ObservationItem) => Ok((Streaming, vec![])),
            (Inspect, ObservationItem | ObservationEnd)
            | (Observe, InspectResult)
            | (Streaming, SetupFailure | InspectResult)
            | (Finished, SetupFailure | InspectResult | ObservationItem | ObservationEnd) => {
                Err(TransitionError::NotAllowed {
                    state: phase.state_tag(),
                    event: kind.event_tag(),
                })
            }
        }
    }

    fn transitions() -> &'static [Transition] {
        const EDGES: &[Transition] = &[
            edge("inspect", "setupFailure", "finished", true),
            edge("inspect", "inspectResult", "finished", true),
            edge("observe", "setupFailure", "finished", true),
            edge("observe", "observationEnd", "finished", true),
            edge("observe", "observationItem", "streaming", false),
            edge("streaming", "observationItem", "streaming", false),
            edge("streaming", "observationEnd", "finished", true),
        ];
        EDGES
    }
}

const fn edge(
    from: &'static str,
    event: &'static str,
    to: &'static str,
    terminal: bool,
) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal,
    }
}

pub fn next_response(
    phase: ResponsePhase,
    reply: &Reply,
) -> Result<ResponsePhase, TransitionError<NoDomainRefusal>> {
    CallLifecycle::transition(&phase, ReplyKind::from(reply)).map(|(next, _)| next)
}

#[cfg(test)]
#[path = "call_lifecycle_test.rs"]
mod tests;
