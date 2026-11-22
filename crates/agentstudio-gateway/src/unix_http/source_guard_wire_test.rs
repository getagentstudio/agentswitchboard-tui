#![allow(clippy::unwrap_used)]

use std::{collections::VecDeque, sync::Mutex};

use agentstudio_gateway_contract::{
    observation_records::{
        Cursor, EndReason, OrderedRecord, ResultReason, SessionObservation, TranscriptEvent,
        TranscriptItem, TranscriptItemKind,
    },
    session_contract::InspectResult,
};
use futures_util::stream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::{
    fixture::fixture_session_source::fixture_target, session::session_source::ObservationStream,
};

struct ScriptedSource {
    records: Mutex<Option<VecDeque<SessionObservation>>>,
    dropped: CancellationToken,
}

struct DropNotice(CancellationToken);

impl Drop for DropNotice {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl SessionSource for ScriptedSource {
    async fn inspect(&self, _: InspectArguments) -> Result<InspectResult, SessionSourceError> {
        Err(SessionSourceError::Unavailable)
    }

    async fn observe(
        &self,
        _: ObserveArguments,
        _: CancellationToken,
    ) -> Result<ObservationStream, SessionSourceError> {
        let records = self.records.lock().unwrap().take().unwrap();
        let lease = DropNotice(self.dropped.clone());
        Ok(Box::pin(stream::unfold(
            (records, lease),
            |(mut records, lease)| async move {
                records.pop_front().map(|record| (record, (records, lease)))
            },
        )))
    }
}

fn record(sequence: u64, content: String) -> OrderedRecord {
    OrderedRecord {
        sequence,
        event: TranscriptEvent::ItemStarted {
            item: TranscriptItem {
                item_id: format!("msg_{sequence}"),
                kind: TranscriptItemKind::Notice,
                text: Some(content),
            },
        },
    }
}

async fn run_source(records: Vec<SessionObservation>) -> String {
    let work = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(work.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = work.path().join("g.sock");
    let dropped = CancellationToken::new();
    let source = ScriptedSource {
        records: Mutex::new(Some(records.into())),
        dropped: dropped.clone(),
    };
    let target = fixture_target();
    let server = GatewayServer::bind(&socket, source, target.clone())
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let serving = tokio::spawn(server.serve(stop.clone()));
    let body = serde_json::json!({
        "jsonrpc":"2.0", "id":"source-order", "method":"tools/call",
        "params":{"name":"events_observe", "arguments":{ "target":target,"timeoutSeconds":5,"maxEvents":99,"maxBytes":1048576 }}
    }).to_string();
    let request = format!(
        "POST /mcp HTTP/1.1\r\nHost: gateway\r\nAgentStudio-Wire-Version: 1\r\nAccept: text/event-stream\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let mut peer = tokio::net::UnixStream::connect(socket).await.unwrap();
    peer.write_all(request.as_bytes()).await.unwrap();
    let mut result = String::new();
    time::timeout(Duration::from_secs(2), peer.read_to_string(&mut result))
        .await
        .unwrap()
        .unwrap();
    time::timeout(Duration::from_secs(2), dropped.cancelled())
        .await
        .unwrap();
    stop.cancel();
    serving.await.unwrap().unwrap();
    result
}

#[tokio::test]
async fn unix_guard_preserves_prior_bytes_then_refuses_wrong_epoch_before_oversize_fallback() {
    let cursor = Cursor {
        epoch: 4,
        sequence: 7,
    };
    let response = run_source(vec![
        SessionObservation::History {
            cursor,
            record: record(7, "approved-first-record".into()),
        },
        SessionObservation::SnapshotEnd(cursor),
        SessionObservation::Result {
            epoch: 5,
            reason: ResultReason::DeadlineReached,
            records: vec![record(8, "x".repeat(614400))],
        },
        SessionObservation::Ended(EndReason::Completed),
    ])
    .await;
    assert!(response.contains("approved-first-record"));
    assert!(response.contains("event: snapshotEnd"));
    assert!(!response.contains("event: gap"), "{response}");
    assert!(!response.contains("event: ended"), "{response}");
    assert!(!response.contains("event: result"), "{response}");
}

#[tokio::test]
async fn unix_guard_refuses_live_before_cut_without_polling_later_source_items() {
    let response = run_source(vec![
        SessionObservation::Live {
            cursor: Cursor {
                epoch: 1,
                sequence: 1,
            },
            record: record(1, "refused-live-record".into()),
        },
        SessionObservation::Ended(EndReason::Completed),
    ])
    .await;
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(!response.contains("event: "), "{response}");
}

#[tokio::test]
async fn unix_guard_preserves_valid_replay_and_consecutive_result_suffix() {
    let cursor = Cursor {
        epoch: 4,
        sequence: 7,
    };
    let first = record(7, "validated-history".into());
    let response = run_source(vec![
        SessionObservation::History {
            cursor,
            record: first.clone(),
        },
        SessionObservation::SnapshotEnd(cursor),
        SessionObservation::Result {
            epoch: 4,
            reason: ResultReason::DeadlineReached,
            records: vec![first, record(8, "validated-suffix".into())],
        },
        SessionObservation::Ended(EndReason::Completed),
    ])
    .await;
    assert!(response.contains("event: snapshotEnd"));
    assert!(response.contains("event: result"));
    assert!(response.contains("validated-suffix"));
    assert!(!response.contains("event: ended"));
}

#[tokio::test]
async fn unix_guard_rejects_divergent_reversed_and_skipped_results_as_whole_records() {
    let cursor = Cursor {
        epoch: 4,
        sequence: 7,
    };
    let prior = record(7, "approved-history".into());
    let suffix = record(8, "valid-prefix-of-rejected-result".into());
    for invalid in [
        vec![record(7, "divergent-replay".into())],
        vec![suffix.clone(), prior.clone()],
        vec![suffix, record(10, "skipped-position".into())],
    ] {
        let response = run_source(vec![
            SessionObservation::History {
                cursor,
                record: prior.clone(),
            },
            SessionObservation::SnapshotEnd(cursor),
            SessionObservation::Result {
                epoch: 4,
                reason: ResultReason::DeadlineReached,
                records: invalid,
            },
            SessionObservation::Ended(EndReason::Completed),
        ])
        .await;
        assert!(response.contains("approved-history"));
        assert!(response.contains("event: snapshotEnd"));
        assert!(!response.contains("event: result"));
        assert!(!response.contains("event: ended"));
        assert!(!response.contains("event: gap"));
        assert!(!response.contains("valid-prefix-of-rejected-result"));
    }
}
