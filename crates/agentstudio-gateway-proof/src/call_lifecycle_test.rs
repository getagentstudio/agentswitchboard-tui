use super::*;
use state_machine_core::check_state_machine_transitions;

#[test]
fn unary_and_observation_modes_obey_the_declared_reply_table() {
    let phases = [
        ResponsePhase::Inspect,
        ResponsePhase::Observe,
        ResponsePhase::Streaming,
        ResponsePhase::Finished,
    ];
    let kinds = [
        ReplyKind::SetupFailure,
        ReplyKind::InspectResult,
        ReplyKind::ObservationItem,
        ReplyKind::ObservationEnd,
    ];
    let cases = phases
        .into_iter()
        .flat_map(|phase| kinds.into_iter().map(move |kind| (phase, kind)));
    assert_eq!(
        check_state_machine_transitions::<CallLifecycle>(cases),
        Ok(())
    );
}
