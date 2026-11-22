#![allow(clippy::unwrap_used)]

use super::*;
use crate::fixture::fixture_session_source::{FixtureSessionSource, fixture_target};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn real_socket_refuses_wrong_version_without_waiting_for_body() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = directory.path().join("gateway.sock");
    let stop = CancellationToken::new();
    let fixture = Arc::new(FixtureSessionSource::default());
    let server = GatewayServer::bind(&socket, fixture, fixture_target())
        .await
        .unwrap();
    let serving = tokio::spawn(server.serve(stop.clone()));
    let mut peer = tokio::net::UnixStream::connect(&socket).await.unwrap();

    peer.write_all(b"POST /mcp HTTP/1.1\r\nHost: gateway\r\nAgentStudio-Wire-Version: 99\r\nContent-Length: 999\r\n\r\n").await.unwrap();
    let mut raw = String::new();
    time::timeout(Duration::from_secs(1), peer.read_to_string(&mut raw))
        .await
        .unwrap()
        .unwrap();

    assert!(raw.starts_with("HTTP/1.1 426 Upgrade Required\r\n"));
    assert!(raw.contains("\"code\":\"incompatibleVersion\""));
    stop.cancel();
    serving.await.unwrap().unwrap();
    assert!(!socket.exists());
}

#[tokio::test]
async fn real_socket_refuses_oversized_chunked_body_and_wrong_scope() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = directory.path().join("gateway.sock");
    let stop = CancellationToken::new();
    let fixture = Arc::new(FixtureSessionSource::default());
    let server = GatewayServer::bind(&socket, fixture, fixture_target())
        .await
        .unwrap();
    let serving = tokio::spawn(server.serve(stop.clone()));
    let oversized = format!(
        "POST /mcp HTTP/1.1\r\nHost: gateway\r\nAgentStudio-Wire-Version: 1\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
        REQUEST_BYTES + 1,
        "!".repeat(REQUEST_BYTES + 1),
    );
    let forbidden = r#"{"jsonrpc":"2.0","id":"scope","method":"tools/call","params":{"name":"session_create","arguments":{}}}"#;
    let malformed = "{";

    let response = exchange(&socket, oversized.as_bytes()).await;
    assert!(response.starts_with("HTTP/1.1 413 Payload Too Large"));
    assert!(response.contains("\"code\":\"frameTooLarge\""));
    let response = exchange(&socket, &unary_request(forbidden)).await;
    assert!(response.starts_with("HTTP/1.1 403 Forbidden"));
    assert!(response.contains("\"code\":\"scopeDenied\""));
    let response = exchange(&socket, &unary_request(malformed)).await;
    assert!(response.starts_with("HTTP/1.1 400 Bad Request"));
    assert!(response.contains("\"code\":\"invalidFrame\""));

    stop.cancel();
    serving.await.unwrap().unwrap();
}

fn unary_request(body: &str) -> Vec<u8> {
    format!(
        "POST /mcp HTTP/1.1\r\nHost: gateway\r\nAgentStudio-Wire-Version: 1\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    ).into_bytes()
}

async fn exchange(socket: &Path, request: &[u8]) -> String {
    let mut peer = tokio::net::UnixStream::connect(socket).await.unwrap();
    let mut raw = String::new();
    time::timeout(Duration::from_secs(2), async {
        let _ = peer.write_all(request).await;
        let _ = peer.read_to_string(&mut raw).await;
    })
    .await
    .unwrap();
    raw
}

struct ResultBeforePosition {
    item_text_bytes: usize,
    emit_as_history: bool,
}

impl SessionSource for ResultBeforePosition {
    async fn inspect(
        &self,
        request: InspectArguments,
    ) -> Result<agentstudio_gateway_contract::session_contract::InspectResult, SessionSourceError>
    {
        Arc::new(FixtureSessionSource::default())
            .inspect(request)
            .await
    }

    async fn observe(
        &self,
        _: ObserveArguments,
        _: CancellationToken,
    ) -> Result<crate::session::session_source::ObservationStream, SessionSourceError> {
        use agentstudio_gateway_contract::observation_records::{
            OrderedRecord, ResultReason, SessionObservation, TranscriptEvent, TranscriptItem,
            TranscriptItemKind,
        };
        let record = OrderedRecord {
            sequence: 9_007_199_254_740_993,
            event: TranscriptEvent::ItemStarted {
                item: TranscriptItem {
                    item_id: "msg_result_only".into(),
                    kind: TranscriptItemKind::Notice,
                    text: Some("x".repeat(self.item_text_bytes)),
                },
            },
        };
        let first = if self.emit_as_history {
            SessionObservation::History {
                cursor: agentstudio_gateway_contract::observation_records::Cursor {
                    epoch: 1,
                    sequence: record.sequence,
                },
                record,
            }
        } else {
            SessionObservation::Result {
                epoch: 1,
                reason: ResultReason::DeadlineReached,
                records: vec![record],
            }
        };
        Ok(Box::pin(futures_util::stream::iter([first])))
    }
}

#[tokio::test]
async fn real_socket_does_not_claim_clean_completion_for_unseen_oversize_result_without_cursor() {
    let response = observe_before_position(614_400, false).await;
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(!response.contains("event: ended"), "{response}");
    assert!(response.contains("event: gap"), "{response}");
    assert!(response.contains("data: {\"cursor\":null,\"reason\":\"eventTooLarge\"}"));
    assert_eq!(response.matches("event: ").count(), 1);
}

#[tokio::test]
async fn real_socket_preserves_fitting_result_before_any_cursor_without_manufacturing_cut() {
    let response = observe_before_position(8, false).await;
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("event: result"), "{response}");
    assert!(!response.contains("event: ended"));
    assert!(!response.contains("event: snapshotEnd"));
    assert!(response.contains("\"sequence\":9007199254740993"));
}

#[tokio::test]
async fn real_socket_does_not_invent_a_cursor_for_oversized_first_item() {
    let response = observe_before_position(SSE_DATA_BYTES + 1, true).await;
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("event: gap\ndata: {\"cursor\":null,\"reason\":\"eventTooLarge\"}"));
    assert_eq!(response.matches("event: ").count(), 1);
}

async fn observe_before_position(item_text_bytes: usize, emit_as_history: bool) -> String {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = directory.path().join("gateway.sock");
    let stop = CancellationToken::new();
    let target = fixture_target();
    let server = GatewayServer::bind(
        &socket,
        ResultBeforePosition {
            item_text_bytes,
            emit_as_history,
        },
        target.clone(),
    )
    .await
    .unwrap();
    let serving = tokio::spawn(server.serve(stop.clone()));
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": "result-before-cursor", "method": "tools/call",
        "params": {
            "name": "events_observe", "arguments": { "target": target, "timeoutSeconds": 5, "maxEvents": 10, "maxBytes": 1048576 }
        }
    }).to_string();
    let request = format!(
        "POST /mcp HTTP/1.1\r\nHost: gateway\r\nAgentStudio-Wire-Version: 1\r\nAccept: text/event-stream\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let response = exchange(&socket, request.as_bytes()).await;
    stop.cancel();
    serving.await.unwrap().unwrap();
    response
}

#[tokio::test]
async fn real_gateway_and_effect_attachment_report_pre_cut_gap_without_committing_history() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = directory.path().join("gateway.sock");
    let stop = CancellationToken::new();
    let server = GatewayServer::bind(
        &socket,
        ResultBeforePosition {
            item_text_bytes: 614_400,
            emit_as_history: false,
        },
        fixture_target(),
    )
    .await
    .unwrap();
    let serving = tokio::spawn(server.serve(stop.clone()));

    let result = tokio::task::spawn_blocking(move || {
        std::process::Command::new("bun")
            .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .env("PHASE2A_CHECK_SOCKET", socket)
            .arg("apps/agentswitchboard-tui/tests/integration/pre-cut-gateway.ts")
            .output()
    })
    .await
    .unwrap()
    .unwrap();

    stop.cancel();
    serving.await.unwrap().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&result.stdout).trim(),
        "pre-cut gap: null cursor, zero committed snapshots"
    );
}
