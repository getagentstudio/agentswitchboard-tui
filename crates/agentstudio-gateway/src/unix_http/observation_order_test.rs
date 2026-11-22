#![allow(clippy::unwrap_used)]

use super::*;
use agentstudio_gateway_contract::observation_records::{EndReason, GapReason, ResultReason};
use state_machine_core::check_state_machine_transitions;

fn record(sequence: u64) -> OrderedRecord {
    OrderedRecord {
        sequence,
        event: TranscriptEvent::ItemCompleted {
            item_id: format!("msg_{sequence}"),
        },
    }
}

#[test]
fn phase_table_and_terminal_rejection_cover_each_kind() {
    let zero = Cursor {
        epoch: 1,
        sequence: 0,
    };
    let one = Cursor {
        epoch: 1,
        sequence: 1,
    };
    let observations = [
        SessionObservation::History {
            cursor: one,
            record: record(1),
        },
        SessionObservation::Live {
            cursor: one,
            record: record(1),
        },
        SessionObservation::SnapshotEnd(zero),
        SessionObservation::Gap {
            cursor: zero,
            reason: GapReason::Lag,
        },
        SessionObservation::Ended(EndReason::Completed),
        SessionObservation::Result {
            epoch: 1,
            reason: ResultReason::DeadlineReached,
            records: Vec::new(),
        },
    ];
    let phases = [
        ObservationPhase::History,
        ObservationPhase::Live { history_items: 0 },
        ObservationPhase::Terminal,
    ];
    let cases = phases.into_iter().flat_map(|phase| {
        observations.iter().map(move |observation| {
            let position = matches!(phase, ObservationPhase::Live { .. }).then_some(zero);
            (
                phase,
                SourceArrival {
                    observation,
                    position,
                    emitted: &[],
                },
            )
        })
    });
    assert_eq!(
        check_state_machine_transitions::<ObservationOrder<'_>>(cases),
        Ok(())
    );
    for observation in &observations {
        assert!(matches!(
            ObservationOrder::transition(
                &ObservationPhase::Terminal,
                SourceArrival {
                    observation,
                    position: Some(zero),
                    emitted: &[],
                }
            ),
            Err(TransitionError::NotAllowed { .. })
        ));
    }
}

#[test]
fn replay_and_result_payloads_are_checked_before_the_encoder() {
    let last = Cursor {
        epoch: 2,
        sequence: 8,
    };
    let emitted = [record(7), record(8)];
    let phase = ObservationPhase::Live { history_items: 1 };
    let validate = |observation| {
        ObservationOrder::transition(
            &phase,
            SourceArrival {
                observation,
                position: Some(last),
                emitted: &emitted,
            },
        )
    };
    let repeat = SessionObservation::Live {
        cursor: last,
        record: record(8),
    };
    assert_eq!(validate(&repeat).unwrap().1, [SourceEffect::DiscardReplay]);
    let accepted = SessionObservation::Result {
        epoch: 2,
        reason: ResultReason::DeadlineReached,
        records: vec![record(7), record(8), record(9)],
    };
    let (next, effects) = validate(&accepted).unwrap();
    assert!(next.is_terminal());
    assert_eq!(effects, [SourceEffect::Forward]);

    let wrong_epoch = SessionObservation::Result {
        epoch: 3,
        reason: ResultReason::DeadlineReached,
        records: vec![record(8)],
    };
    let skipped = SessionObservation::Result {
        epoch: 2,
        reason: ResultReason::DeadlineReached,
        records: vec![record(10)],
    };
    let altered = SessionObservation::Result {
        epoch: 2,
        reason: ResultReason::DeadlineReached,
        records: vec![OrderedRecord {
            sequence: 8,
            event: TranscriptEvent::ItemCompleted {
                item_id: "different".into(),
            },
        }],
    };
    assert!(matches!(
        validate(&wrong_epoch),
        Err(TransitionError::Domain(SourceRefusal::WrongEpoch))
    ));
    assert!(matches!(
        validate(&skipped),
        Err(TransitionError::Domain(SourceRefusal::InvalidPosition))
    ));
    assert!(matches!(
        validate(&altered),
        Err(TransitionError::Domain(SourceRefusal::DivergentReplay))
    ));
}
