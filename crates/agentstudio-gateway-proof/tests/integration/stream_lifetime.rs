use agentstudio_gateway::{
    fixture::fixture_session_source::fixture_target,
    session::session_source::{ObservationStream, SessionSource, SessionSourceError},
    unix_http::unix_gateway_server::GatewayServer,
};
use agentstudio_gateway_contract::{
    observation_records::{
        Cursor, OrderedRecord, SessionObservation, TranscriptEvent, TranscriptItem,
        TranscriptItemKind,
    },
    session_contract::{InspectArguments, InspectResult, ObserveArguments},
};
use agentstudio_gateway_proof::{
    endpoint_config::{SERVER_NAME, proof_client, proof_server},
    framed_stream::Receiving,
    peer_transport::{PeerBounds, PeerControls, PeerTransport},
    quinn_session_source::QuinnSessionSource,
    witness_fixture::{WitnessFixture, WitnessHold},
};
use futures_util::{StreamExt, stream};
use quinn::{Connection, Endpoint, rustls::pki_types::CertificateDer};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
    time,
};
use tokio_util::sync::CancellationToken;

use super::remote_admission::{envelope, open, terminal};

#[derive(Debug, Eq, PartialEq)]
enum LeaseNotice {
    Opened(usize),
    Retired(usize),
}

struct LeaseDrop(usize, mpsc::UnboundedSender<LeaseNotice>);
impl Drop for LeaseDrop {
    fn drop(&mut self) {
        let _ = self.1.send(LeaseNotice::Retired(self.0));
    }
}

struct LeasedSource {
    fixture: WitnessFixture,
    lifecycle: mpsc::UnboundedSender<LeaseNotice>,
}

impl SessionSource for LeasedSource {
    async fn inspect(
        &self,
        request: InspectArguments,
    ) -> Result<InspectResult, SessionSourceError> {
        self.fixture.inspect(request).await
    }

    async fn observe(
        &self,
        request: ObserveArguments,
        cancel: CancellationToken,
    ) -> Result<ObservationStream, SessionSourceError> {
        let case = request.max_events;
        let stream = if case == 101 {
            let record = OrderedRecord {
                sequence: 1,
                event: TranscriptEvent::ItemStarted {
                    item: TranscriptItem {
                        item_id: "msg_credit_control".into(),
                        kind: TranscriptItemKind::Notice,
                        text: Some("x".repeat(96 * 1024)),
                    },
                },
            };
            Box::pin(stream::iter([SessionObservation::History {
                cursor: Cursor {
                    epoch: 1,
                    sequence: 1,
                },
                record,
            }]))
        } else {
            self.fixture.observe(request, cancel).await?
        };
        let lease = LeaseDrop(case, self.lifecycle.clone());
        let _ = self.lifecycle.send(LeaseNotice::Opened(case));
        Ok(Box::pin(stream.map(move |frame| {
            let _ = &lease;
            frame
        })))
    }
}

struct Scenario {
    peer_address: std::net::SocketAddr,
    certificate: CertificateDer<'static>,
    fixture: WitnessFixture,
    controls: PeerControls,
    notices: mpsc::UnboundedReceiver<LeaseNotice>,
    stop: CancellationToken,
    peer: tokio::task::JoinHandle<()>,
}
impl Scenario {
    fn start(hold: WitnessHold, bounds: PeerBounds) -> Self {
        let (endpoint, certificate) = proof_server("127.0.0.1:0".parse().unwrap()).unwrap();
        let peer_address = endpoint.local_addr().unwrap();
        let fixture = WitnessFixture::new(
            fixture_target(),
            ["history one".into(), "history two".into()],
            hold,
        );
        let (lifecycle, notices) = mpsc::unbounded_channel();
        let stop = CancellationToken::new();
        let controls = PeerControls::default();
        let transport = PeerTransport {
            endpoint,
            source: LeasedSource {
                fixture: fixture.clone(),
                lifecycle,
            },
            allowed_target: fixture_target(),
            bounds,
            controls: controls.clone(),
        };
        let peer = tokio::spawn(transport.serve(stop.clone()));
        Self {
            peer_address,
            certificate,
            fixture,
            controls,
            notices,
            stop,
            peer,
        }
    }
    fn client(&self) -> Endpoint {
        proof_client("127.0.0.1:0".parse().unwrap(), self.certificate.clone()).unwrap()
    }
    async fn connect(&self, client: &Endpoint) -> Connection {
        time::timeout(
            Duration::from_secs(2),
            client.connect(self.peer_address, SERVER_NAME).unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
    }
    async fn notice(&mut self, want: LeaseNotice) {
        assert_eq!(
            time::timeout(Duration::from_secs(2), self.notices.recv())
                .await
                .unwrap(),
            Some(want)
        );
    }
    async fn finish(self) {
        self.stop.cancel();
        self.peer.await.unwrap();
    }
}

fn args(max_events: usize) -> ObserveArguments {
    ObserveArguments {
        target: fixture_target(),
        timeout_seconds: 33,
        max_events,
        max_bytes: 1_048_576,
        after_sequence: None,
        epoch: None,
    }
}
async fn observe(connection: &Connection, max_events: usize) -> quinn::RecvStream {
    let body = envelope(
        1,
        "observe",
        &serde_json::to_value(args(max_events)).unwrap(),
    );
    let (mut sent, received) = open(connection, &body).await;
    sent.finish().unwrap();
    received
}
async fn read_item(receive: &mut Receiving) -> Value {
    let data = time::timeout(Duration::from_secs(2), receive.read_frame(120000))
        .await
        .unwrap()
        .unwrap();
    serde_json::from_slice(&data).unwrap()
}

#[tokio::test]
async fn unix_disconnect_during_first_frame_wait_retires_remote_source() {
    let mut test = Scenario::start(WitnessHold::BeforeFirst, PeerBounds::default());
    let source = QuinnSessionSource::new(test.client(), test.peer_address).unwrap();
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = directory.path().join("g.sock");
    let server = GatewayServer::bind(&socket, source, fixture_target())
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(server.serve(stop.clone()));
    let mut client = tokio::net::UnixStream::connect(&socket).await.unwrap();
    let body=json!({"jsonrpc":"2.0","id":"preadmission-detach","method":"tools/call","params":{"name":"events_observe","arguments":args(77)}}).to_string();
    let wire = format!(
        "POST /mcp HTTP/1.1\r\nHost: local\r\nAgentStudio-Wire-Version: 1\r\nAccept: text/event-stream\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    client.write_all(wire.as_bytes()).await.unwrap();
    test.notice(LeaseNotice::Opened(77)).await;

    drop(client);
    test.notice(LeaseNotice::Retired(77)).await;

    stop.cancel();
    task.await.unwrap().unwrap();
    test.finish().await;
}

#[tokio::test]
async fn quiet_admitted_detach_retires_only_its_remote_lease() {
    let mut test = Scenario::start(WitnessHold::Released, PeerBounds::default());
    let source = QuinnSessionSource::new(test.client(), test.peer_address).unwrap();
    let mut first = source
        .observe(args(77), CancellationToken::new())
        .await
        .unwrap();
    test.notice(LeaseNotice::Opened(77)).await;
    let mut sibling = source
        .observe(args(78), CancellationToken::new())
        .await
        .unwrap();
    test.notice(LeaseNotice::Opened(78)).await;

    for stream in [&mut first, &mut sibling] {
        assert!(matches!(
            stream.next().await,
            Some(SessionObservation::History { .. })
        ));
        assert!(matches!(
            stream.next().await,
            Some(SessionObservation::History { .. })
        ));
        assert!(matches!(
            stream.next().await,
            Some(SessionObservation::SnapshotEnd(_))
        ));
    }

    drop(first);
    test.notice(LeaseNotice::Retired(77)).await;
    assert!(test.notices.try_recv().is_err());
    test.fixture
        .publish("only sibling sees this".into())
        .unwrap();
    assert!(matches!(
        time::timeout(Duration::from_secs(2), sibling.next())
            .await
            .unwrap(),
        Some(SessionObservation::Live { .. })
    ));
    assert_eq!(
        source
            .inspect(InspectArguments {
                target: fixture_target()
            })
            .await
            .unwrap()
            .target,
        fixture_target()
    );

    drop(sibling);
    test.notice(LeaseNotice::Retired(78)).await;
    drop(source);
    test.finish().await;
}

#[tokio::test]
async fn credit_blocked_write_leaves_same_connection_siblings_live_and_is_cancellable() {
    let mut test = Scenario::start(WitnessHold::Released, PeerBounds::default());
    let client = test.client();
    let connection = test.connect(&client).await;
    let mut credit = test.controls.credits();
    let mut stalled = observe(&connection, 101).await;
    test.notice(LeaseNotice::Opened(101)).await;
    let blocked = time::timeout(Duration::from_secs(2), credit.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(blocked.stream, stalled.id());
    assert!(blocked.frame_bytes > 96 * 1024);

    let mut sibling = Receiving::new(observe(&connection, 78).await);
    test.notice(LeaseNotice::Opened(78)).await;
    for expectation in ["history", "history", "snapshotEnd"] {
        assert_eq!(read_item(&mut sibling).await["item"]["kind"], expectation);
    }
    test.fixture
        .publish("sibling progresses while A is blocked".into())
        .unwrap();
    assert_eq!(read_item(&mut sibling).await["item"]["kind"], "live");
    let (mut inspect, received) = open(
        &connection,
        &envelope(1, "inspect", &json!({"target":fixture_target()})),
    )
    .await;
    inspect.finish().unwrap();
    assert_eq!(terminal(received).await["kind"], "inspectResult");
    assert!(test.notices.try_recv().is_err());

    stalled.stop(5_u8.into()).unwrap();
    test.notice(LeaseNotice::Retired(101)).await;
    drop(sibling);
    test.notice(LeaseNotice::Retired(78)).await;
    client.close(0_u8.into(), b"done");
    test.finish().await;
}

#[tokio::test]
async fn credit_blocked_write_retires_source_at_its_deadline() {
    let mut test = Scenario::start(
        WitnessHold::Released,
        PeerBounds {
            write: Duration::from_millis(300),
            ..PeerBounds::default()
        },
    );
    let client = test.client();
    let connection = test.connect(&client).await;
    let mut credit = test.controls.credits();
    let mut stalled = observe(&connection, 101).await;
    test.notice(LeaseNotice::Opened(101)).await;
    let blocked = time::timeout(Duration::from_secs(2), credit.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(blocked.stream, stalled.id());

    test.notice(LeaseNotice::Retired(101)).await;

    let closed = time::timeout(Duration::from_secs(2), async {
        let mut buffered = [0; 4096];
        loop {
            match stalled.read(&mut buffered).await {
                Ok(Some(_)) => {}
                terminal => return terminal,
            }
        }
    })
    .await
    .unwrap();
    assert!(matches!(closed, Err(quinn::ReadError::Reset(_))));
    client.close(0_u8.into(), b"done");
    test.finish().await;
}

#[tokio::test]
async fn remote_loss_before_any_valid_frame_returns_a_correlated_local_tool_error() {
    let mut test = Scenario::start(WitnessHold::BeforeFirst, PeerBounds::default());
    let source = QuinnSessionSource::new(test.client(), test.peer_address).unwrap();
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = directory.path().join("g.sock");
    let server = GatewayServer::bind(&socket, source, fixture_target())
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(server.serve(stop.clone()));
    let mut client = tokio::net::UnixStream::connect(&socket).await.unwrap();
    let body = json!({"jsonrpc":"2.0","id":"pre-first-loss","method":"tools/call","params":{"name":"events_observe","arguments":args(77)}}).to_string();
    let request = format!(
        "POST /mcp HTTP/1.1\r\nHost: local\r\nAgentStudio-Wire-Version: 1\r\nAccept: text/event-stream\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    client.write_all(request.as_bytes()).await.unwrap();
    test.notice(LeaseNotice::Opened(77)).await;
    assert!(test.controls.abort_stream(0));

    let mut response = String::new();
    time::timeout(Duration::from_secs(2), client.read_to_string(&mut response))
        .await
        .unwrap()
        .unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(headers.contains("content-type: application/json"));
    let decoded: Value = serde_json::from_str(body).unwrap();
    assert_eq!(decoded["id"], "pre-first-loss");
    assert_eq!(decoded["result"]["isError"], true);
    assert_eq!(decoded["result"]["structuredContent"]["mcpResult"], "error");
    assert_eq!(
        decoded["result"]["structuredContent"]["kind"],
        "unavailable"
    );
    test.notice(LeaseNotice::Retired(77)).await;

    stop.cancel();
    task.await.unwrap().unwrap();
    test.finish().await;
}
