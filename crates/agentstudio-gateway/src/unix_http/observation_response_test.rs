#![allow(clippy::unwrap_used)]

use super::*;
use crate::fixture::fixture_session_source::fixture_target;
use agentstudio_gateway_contract::observation_records::{
    TranscriptEvent, TranscriptItem, TranscriptItemKind,
};

fn large_record(sequence: u64, fill: &str) -> OrderedRecord {
    OrderedRecord {
        sequence,
        event: TranscriptEvent::ItemStarted {
            item: TranscriptItem {
                item_id: format!("msg_{sequence}"),
                kind: TranscriptItemKind::AgentMessage,
                text: Some(fill.repeat(614_400)),
            },
        },
    }
}

#[test]
fn oversized_full_result_falls_back_only_when_its_items_were_emitted() {
    let emitted_record = large_record(10, "a");
    let cursor = Cursor {
        epoch: 1,
        sequence: 10,
    };
    let item = ItemFrame {
        phase: ItemPhase::Live,
        cursor: cursor.into(),
        record: emitted_record.clone(),
    };
    assert!(encode_frame("item", &item).is_some());
    let emitted = EmittedRecords {
        items: vec![emitted_record.clone()],
        size: 614_400,
        position: Some(cursor),
    };
    let id = RequestId::Text("test".into());
    let target = fixture_target();

    let covered = emitted
        .terminal_result(
            &id,
            &target,
            cursor.epoch,
            ResultReason::DeadlineReached,
            vec![emitted_record.clone()],
        )
        .unwrap();
    let uncovered = emitted
        .terminal_result(
            &id,
            &target,
            cursor.epoch,
            ResultReason::DeadlineReached,
            vec![large_record(11, "b")],
        )
        .unwrap();

    let different_epoch = emitted
        .terminal_result(
            &id,
            &target,
            cursor.epoch + 1,
            ResultReason::DeadlineReached,
            vec![emitted_record],
        )
        .unwrap();

    assert_eq!(
        std::str::from_utf8(&covered).unwrap(),
        "event: ended\ndata: {\"reason\":\"completed\"}\n\n"
    );
    assert_eq!(
        std::str::from_utf8(&uncovered).unwrap(),
        "event: gap\ndata: {\"cursor\":{\"epoch\":\"1\",\"sequence\":\"10\"},\"reason\":\"eventTooLarge\"}\n\n"
    );
    assert_eq!(
        different_epoch, uncovered,
        "The same bytes in a new epoch do not establish coverage"
    );
}
