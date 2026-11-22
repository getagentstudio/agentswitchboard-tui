use serde_json::json;
use session_event_model::{
    InputId, SessionEvent, SessionItem, SessionItemKind, StopReason, TurnOutcome,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event =
        |sequence: u64, payload: SessionEvent| json!({"sequence": sequence, "event": payload});
    let item = |item_id: &str, kind: SessionItemKind, text: Option<&str>| SessionItem {
        item_id: item_id.into(),
        kind,
        text: text.map(str::to_owned),
    };
    let events = [
        event(
            9_007_199_254_740_991,
            SessionEvent::ItemStarted {
                item: item(
                    "msg_one",
                    SessionItemKind::UserMessage,
                    Some("First question"),
                ),
            },
        ),
        event(
            9_007_199_254_740_992,
            SessionEvent::TurnStarted {
                turn_id: "turn-1".into(),
                input_id: InputId::new("input-1")?,
            },
        ),
        event(
            9_007_199_254_740_993,
            SessionEvent::ItemStarted {
                item: item("msg_two", SessionItemKind::AgentMessage, Some("Starting")),
            },
        ),
        event(
            9_007_199_254_740_994,
            SessionEvent::ItemUpdated {
                item: item("msg_two", SessionItemKind::AgentMessage, Some("Finished")),
            },
        ),
        event(
            9_007_199_254_740_995,
            SessionEvent::ItemCompleted {
                item_id: "msg_two".into(),
            },
        ),
        event(
            9_007_199_254_740_996,
            SessionEvent::TurnEnded {
                turn_id: "turn-1".into(),
                outcome: TurnOutcome::Ended {
                    stop_reason: StopReason::EndTurn,
                    local_cause: None,
                },
            },
        ),
        event(
            9_007_199_254_740_997,
            SessionEvent::ItemStarted {
                item: item("msg_three", SessionItemKind::Notice, None),
            },
        ),
        event(
            u64::MAX,
            SessionEvent::ResyncRequired {
                replay_epoch: u64::MAX,
            },
        ),
    ];
    println!("{}", serde_json::to_string_pretty(&events)?);
    Ok(())
}
