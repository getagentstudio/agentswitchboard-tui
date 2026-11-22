use std::time::Duration;

use agentstudio_gateway::{
    fixture::fixture_session_source::fixture_target,
    session::session_source::{ObservationStream, SessionSource, SessionSourceError},
};
use agentstudio_gateway_contract::{
    observation_records::{Cursor, EndReason, SessionObservation},
    session_contract::{
        FixtureCapabilities, HistoryAvailability, InspectArguments, InspectResult,
        ObserveArguments, ProviderState,
    },
};
use agentstudio_gateway_proof::{
    endpoint_config::{SERVER_NAME, proof_client, proof_server},
    framed_stream::Receiving,
    peer_transport::{PeerBounds, PeerControls, PeerTransport},
};
use futures_util::stream;
use quinn::{Connection, RecvStream, SendStream};
use serde_json::{Value, json};
use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;

struct RecordingSource {
    dispatched: mpsc::UnboundedSender<ObserveArguments>,
}

impl SessionSource for RecordingSource {
    async fn inspect(
        &self,
        request: InspectArguments,
    ) -> Result<InspectResult, SessionSourceError> {
        Ok(InspectResult {
            target: request.target,
            state: ProviderState::Idle,
            capabilities: FixtureCapabilities::default(),
            history: HistoryAvailability::Available,
        })
    }

    async fn observe(
        &self,
        request: ObserveArguments,
        _: CancellationToken,
    ) -> Result<ObservationStream, SessionSourceError> {
        let timeout = request.timeout_seconds;
        self.dispatched.send(request).unwrap();
        if timeout == 29 {
            return Err(SessionSourceError::Denied);
        }
        Ok(Box::pin(stream::iter([
            SessionObservation::SnapshotEnd(Cursor {
                epoch: 9_007_199_254_740_993,
                sequence: 0,
            }),
            SessionObservation::Ended(EndReason::Completed),
        ])))
    }
}

#[tokio::test]
async fn real_quinn_checks_frames_versions_limits_targets_and_fin_before_source_dispatch() {
    let (endpoint, certificate) = proof_server("127.0.0.1:0".parse().unwrap()).unwrap();
    let peer_address = endpoint.local_addr().unwrap();
    let client = proof_client("127.0.0.1:0".parse().unwrap(), certificate).unwrap();
    let stop = CancellationToken::new();
    let (events, mut received) = mpsc::unbounded_channel();
    let peer = PeerTransport {
        endpoint,
        allowed_target: fixture_target(),
        source: RecordingSource { dispatched: events },
        bounds: PeerBounds {
            intake: Duration::from_millis(600),
            ..PeerBounds::default()
        },
        controls: PeerControls::default(),
    };
    let serving = tokio::spawn(peer.serve(stop.clone()));
    let connection = time::timeout(
        Duration::from_secs(2),
        client.connect(peer_address, SERVER_NAME).unwrap(),
    )
    .await
    .unwrap()
    .unwrap();

    let tuple = json!({
        "target":{"endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"claude-local"},"sessionId":"ses_fixture"},
        "timeoutSeconds":19, "maxEvents":31, "maxBytes":12401, "afterSequence":null,"epoch":null
    });
    let wrong_scope = json!({
        "target":{"endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"claude-local"},"sessionId":"foreign"},
        "timeoutSeconds":19, "maxEvents":31, "maxBytes":12401
    });
    let mut malformed_cases = vec![(Vec::new(),"invalidFrame"),
        (envelope(79,"observe",&json!({"maxEvents":"invalid"})), "incompatibleVersion"),
        (envelope(1,"unknown",&tuple), "unknownRoute"),
        (envelope(1,"observe",&wrong_scope), "scopeDenied"),
        (envelope(1,"observe",&json!({"maxEvents":"wrong"})), "invalidArguments"),
        (b"{\"wireVersion\":1,\"wireVersion\":1,\"service\":\"sessions\",\"operation\":\"observe\",\"payload\":{}}".to_vec(),"invalidFrame"),
        (b"{not-json".to_vec(),"invalidFrame"),
        (b"\xff".to_vec(),"invalidFrame"),
    ];
    for (field, value) in [
        ("timeoutSeconds", json!(0)),
        ("timeoutSeconds", json!(u64::MAX)),
        ("maxEvents", json!(0)),
        ("maxEvents", json!(4097)),
        ("maxBytes", json!(0)),
        ("maxBytes", json!(1_048_577)),
        ("afterSequence", json!(1)),
        ("epoch", json!(1)),
        ("unrecognized", json!(true)),
    ] {
        let mut wrong = tuple.clone();
        wrong[field] = value;
        malformed_cases.push((envelope(1, "observe", &wrong), "invalidArguments"));
    }
    for (bytes, reason) in malformed_cases {
        let (mut send, receive) = open(&connection, &bytes).await;
        send.finish().unwrap();
        assert_eq!(
            terminal(receive).await,
            json!({"kind":"refused","code":reason})
        );
        assert!(received.try_recv().is_err());
    }
    let (mut oversize, receive) = connection.open_bi().await.unwrap();
    oversize.write_all(&262145_u32.to_be_bytes()).await.unwrap();
    oversize.finish().unwrap();
    assert_eq!(
        terminal(receive).await,
        json!({"kind":"refused","code":"frameTooLarge"})
    );
    let (mut extra, receive) = open(&connection, &envelope(1, "observe", &tuple)).await;
    extra.write_all(&[1]).await.unwrap();
    extra.finish().unwrap();
    assert_eq!(
        terminal(receive).await,
        json!({"kind":"refused","code":"invalidFrame"})
    );
    assert!(received.try_recv().is_err());

    let (mut withheld, withheld_receive) = open(&connection, &envelope(1, "observe", &tuple)).await;
    let (mut inspect, inspect_receive) = open(
        &connection,
        &envelope(1, "inspect", &json!({"target":tuple["target"]})),
    )
    .await;
    inspect.finish().unwrap();
    assert_eq!(terminal(inspect_receive).await["kind"], "inspectResult");
    assert_eq!(
        terminal(withheld_receive).await,
        json!({"kind":"refused","code":"intakeTimeout"})
    );
    let _ = withheld.reset(1_u8.into());
    assert!(received.try_recv().is_err());

    let (mut good, receive) = open(&connection, &envelope(1, "observe", &tuple)).await;
    good.finish().unwrap();
    let mut reader = Receiving::new(receive);
    let cut: Value = serde_json::from_slice(&reader.read_frame(1024).await.unwrap()).unwrap();
    let end: Value = serde_json::from_slice(&reader.read_frame(1024).await.unwrap()).unwrap();
    reader.confirm_fin().await.unwrap();
    assert_eq!(
        cut,
        json!({"kind":"observationItem","item":{"kind":"snapshotEnd","cursor":{"epoch":9_007_199_254_740_993_u64,"sequence":0}}})
    );
    assert_eq!(
        end,
        json!({"kind":"observationEnd","end":{"kind":"ended","reason":"completed"}})
    );
    let inspected = received.try_recv().unwrap();
    assert_eq!(
        (
            inspected.timeout_seconds,
            inspected.max_events,
            inspected.max_bytes
        ),
        (19, 31, 12401)
    );
    assert_eq!(inspected.target, fixture_target());
    assert!(inspected.epoch.is_none() && inspected.after_sequence.is_none());

    let mut denied = tuple;
    denied["timeoutSeconds"] = json!(29);
    let (mut source_error, receive) = open(&connection, &envelope(1, "observe", &denied)).await;
    source_error.finish().unwrap();
    assert_eq!(
        terminal(receive).await,
        json!({"kind":"sourceError","code":"denied"})
    );
    assert_eq!(received.try_recv().unwrap().timeout_seconds, 29);
    stop.cancel();
    client.close(0_u8.into(), b"done");
    serving.await.unwrap();
}

pub(super) fn envelope(version: u64, operation: &str, payload: &Value) -> Vec<u8> {
    format!(r#"{{"wireVersion":{version},"service":"sessions","operation":"{operation}","payload":{payload}}}"#).into_bytes()
}

pub(super) async fn open(connection: &Connection, body: &[u8]) -> (SendStream, RecvStream) {
    let (mut send, receive) = connection.open_bi().await.unwrap();
    send.write_all(&(body.len() as u32).to_be_bytes())
        .await
        .unwrap();
    send.write_all(body).await.unwrap();
    (send, receive)
}

pub(super) async fn terminal(receive: RecvStream) -> Value {
    let mut reader = Receiving::new(receive);
    let response = time::timeout(Duration::from_secs(3), async {
        let response = reader.read_frame(2048).await.unwrap();
        reader.confirm_fin().await.unwrap();
        response
    })
    .await
    .unwrap();
    serde_json::from_slice(&response).unwrap()
}
