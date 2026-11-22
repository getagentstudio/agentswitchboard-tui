#![deny(clippy::wildcard_enum_match_arm)]

use agentstudio_gateway_contract::observation_records::{
    Cursor, OrderedRecord, SessionObservation, TranscriptEvent,
};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

#[derive(Clone, Copy)]
pub(super) enum ObservationPhase {
    History,
    Live { history_items: usize },
    Terminal,
}

impl StateTag for ObservationPhase {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::History => "history",
            Self::Live { .. } => "live",
            Self::Terminal => "terminal",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal)
    }
}

#[derive(Clone, Copy)]
pub(super) struct SourceArrival<'stream> {
    pub(super) observation: &'stream SessionObservation,
    pub(super) position: Option<Cursor>,
    pub(super) emitted: &'stream [OrderedRecord],
}

impl EventTag for SourceArrival<'_> {
    fn event_tag(&self) -> &'static str {
        match self.observation {
            SessionObservation::History { .. } => "history",
            SessionObservation::SnapshotEnd(_) => "cut",
            SessionObservation::Live { .. } => "live",
            SessionObservation::Gap { .. } => "gap",
            SessionObservation::Ended(_) => "ended",
            SessionObservation::Result { .. } => "result",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SourceEffect {
    Forward,
    DiscardReplay,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum SourceRefusal {
    InvalidRecord,
    WrongEpoch,
    InvalidPosition,
    DivergentReplay,
}

impl RefusalTag for SourceRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::InvalidRecord => "invalidRecord",
            Self::WrongEpoch => "wrongEpoch",
            Self::InvalidPosition => "invalidPosition",
            Self::DivergentReplay => "divergentReplay",
        }
    }
}

pub(super) struct ObservationOrder<'stream>(std::marker::PhantomData<&'stream ()>);

impl<'stream> StateMachine for ObservationOrder<'stream> {
    type State = ObservationPhase;
    type Event = SourceArrival<'stream>;
    type Effect = SourceEffect;
    type Error = SourceRefusal;

    fn transition(phase: &Self::State, arrival: Self::Event) -> StateMachineResult<Self> {
        use ObservationPhase::{History, Live, Terminal};
        use SessionObservation as Frame;
        let forward = |next| Ok((next, vec![SourceEffect::Forward]));
        match (*phase, arrival.observation) {
            (History, Frame::History { cursor, record }) => {
                let effect = arrival.check_item(*cursor, record, 0)?;
                Ok((History, vec![effect]))
            }
            (Live { history_items }, Frame::Live { cursor, record }) => {
                let effect = arrival.check_item(*cursor, record, history_items)?;
                Ok((*phase, vec![effect]))
            }
            (History, Frame::SnapshotEnd(cursor)) => {
                if !arrival
                    .position
                    .map_or(cursor.sequence == 0, |previous| previous == *cursor)
                {
                    return Err(TransitionError::Domain(SourceRefusal::InvalidPosition));
                }
                forward(Live {
                    history_items: arrival.emitted.len(),
                })
            }
            (History | Live { .. }, Frame::Result { epoch, records, .. }) => {
                arrival.check_result(*epoch, records, matches!(phase, Live { .. }))?;
                forward(Terminal)
            }
            (History | Live { .. }, Frame::Gap { .. } | Frame::Ended(_)) => forward(Terminal),
            (History, Frame::Live { .. })
            | (Live { .. }, Frame::History { .. } | Frame::SnapshotEnd(_))
            | (
                Terminal,
                Frame::History { .. }
                | Frame::Live { .. }
                | Frame::SnapshotEnd(_)
                | Frame::Gap { .. }
                | Frame::Ended(_)
                | Frame::Result { .. },
            ) => Err(TransitionError::NotAllowed {
                state: phase.state_tag(),
                event: arrival.event_tag(),
            }),
        }
    }

    fn transitions() -> &'static [Transition] {
        const EDGES: &[Transition] = &[
            transition("history", "history", "history", false),
            transition("history", "cut", "live", false),
            transition("live", "live", "live", false),
            transition("history", "gap", "terminal", true),
            transition("history", "ended", "terminal", true),
            transition("history", "result", "terminal", true),
            transition("live", "gap", "terminal", true),
            transition("live", "ended", "terminal", true),
            transition("live", "result", "terminal", true),
        ];
        EDGES
    }
}

impl<'stream> SourceArrival<'stream> {
    fn check_item(
        self,
        cursor: Cursor,
        record: &OrderedRecord,
        phase_start: usize,
    ) -> Result<SourceEffect, TransitionError<SourceRefusal>> {
        if cursor.sequence != record.sequence || is_resync(record) {
            return Err(TransitionError::Domain(SourceRefusal::InvalidRecord));
        }
        let Some(previous) = self.position else {
            return Ok(SourceEffect::Forward);
        };
        if previous.epoch != cursor.epoch {
            return Err(TransitionError::Domain(SourceRefusal::WrongEpoch));
        }
        if previous.sequence.checked_add(1) == Some(cursor.sequence) {
            return Ok(SourceEffect::Forward);
        }
        if let Some((index, prior)) = self.find_record(record.sequence) {
            if index >= phase_start && prior == record {
                return Ok(SourceEffect::DiscardReplay);
            }
            return Err(TransitionError::Domain(SourceRefusal::DivergentReplay));
        }
        Err(TransitionError::Domain(SourceRefusal::InvalidPosition))
    }

    fn check_result(
        self,
        epoch: u64,
        records: &[OrderedRecord],
        after_cut: bool,
    ) -> Result<(), TransitionError<SourceRefusal>> {
        if self
            .position
            .is_some_and(|position| position.epoch != epoch)
        {
            return Err(TransitionError::Domain(SourceRefusal::WrongEpoch));
        }
        let mut preceding: Option<&OrderedRecord> = None;
        let mut through = self.position.map(|position| position.sequence);
        for record in records {
            if is_resync(record) {
                return Err(TransitionError::Domain(SourceRefusal::InvalidRecord));
            }
            if let Some(prior) = preceding {
                if record.sequence == prior.sequence && record == prior {
                    continue;
                }
                if record.sequence <= prior.sequence {
                    return Err(TransitionError::Domain(SourceRefusal::DivergentReplay));
                }
            }
            preceding = Some(record);
            if let Some((_, prior)) = self.find_record(record.sequence) {
                if record != prior {
                    return Err(TransitionError::Domain(SourceRefusal::DivergentReplay));
                }
                continue;
            }
            if through.is_some_and(|position| record.sequence <= position)
                || (after_cut
                    && through.and_then(|position| position.checked_add(1))
                        != Some(record.sequence))
            {
                return Err(TransitionError::Domain(SourceRefusal::InvalidPosition));
            }
            through = Some(record.sequence);
        }
        Ok(())
    }

    fn find_record(self, sequence: u64) -> Option<(usize, &'stream OrderedRecord)> {
        let index = self
            .emitted
            .binary_search_by_key(&sequence, |record| record.sequence)
            .ok()?;
        Some((index, &self.emitted[index]))
    }
}

const fn transition(
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

fn is_resync(record: &OrderedRecord) -> bool {
    matches!(record.event, TranscriptEvent::ResyncRequired { .. })
}

pub(super) fn check_initial(observation: &SessionObservation) -> bool {
    ObservationOrder::transition(
        &ObservationPhase::History,
        SourceArrival {
            observation,
            position: None,
            emitted: &[],
        },
    )
    .is_ok()
}

#[cfg(test)]
#[path = "observation_order_test.rs"]
mod tests;
