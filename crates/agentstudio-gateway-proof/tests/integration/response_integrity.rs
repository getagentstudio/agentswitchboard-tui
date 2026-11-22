use agentstudio_gateway::{
    fixture::fixture_session_source::fixture_target,
    session::session_source::{SessionSource, SessionSourceError},
};
use agentstudio_gateway_contract::session_contract::ObserveArguments;
use agentstudio_gateway_proof::{
    endpoint_config::{proof_client, proof_server},
    framed_stream::Receiving,
    quinn_session_source::QuinnSessionSource,
};
use futures_util::StreamExt;
use std::time::Duration;
use tokio::{sync::oneshot, time};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum TerminalFault {
    Valid,
    Trailing,
    Withheld,
    Reset,
    NestedUnknown,
    WrongMode,
    BadFirstRecord,
    OversizedReply,
}

const END: &str = r#"{"kind":"observationEnd","end":{"kind":"ended","reason":"completed"}}"#;
const BAD_NESTED: &str =
    r#"{"kind":"observationEnd","end":{"kind":"ended","reason":"completed","hidden":true}}"#;
const WRONG_MODE: &str = r#"{"kind":"inspectResult","result":{"target":{"endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"claude-local"},"sessionId":"ses_fixture"},"state":"idle","capabilities":{"load":false,"resume":false,"close":false,"list":false,"steer":false,"modes":false,"configOptions":false,"elicitation":false,"usage":false,"promptContent":{"image":false,"audio":false,"embeddedContext":false},"authStatus":{"kind":"notReported"}},"history":"available"}}"#;
const BAD_FIRST: &str = r#"{"kind":"observationItem","item":{"kind":"history","cursor":{"epoch":1,"sequence":2},"record":{"sequence":1,"event":{"kind":"itemCompleted","itemId":"one"}}}}"#;

async fn independently_written_response(fault: TerminalFault) {
    let (server, cert) = proof_server("127.0.0.1:0".parse().unwrap()).unwrap();
    let peer = server.local_addr().unwrap();
    let endpoint = proof_client("127.0.0.1:0".parse().unwrap(), cert).unwrap();
    let source = QuinnSessionSource::new(endpoint, peer).unwrap();
    let mut fin = source.subscribe_fin_waits();
    let (release, released) = oneshot::channel::<()>();
    let (server_done, finished) = oneshot::channel::<()>();
    let (sent_id, receive_id) = oneshot::channel::<quinn::StreamId>();
    let serving = tokio::spawn(async move {
        let conn = server.accept().await.unwrap().await.unwrap();
        let (mut send, receive) = conn.accept_bi().await.unwrap();
        let mut reader = Receiving::new(receive);
        reader.read_frame(262144).await.unwrap();
        reader.confirm_fin().await.unwrap();
        let wire = match fault {
            TerminalFault::NestedUnknown => BAD_NESTED,
            TerminalFault::WrongMode => WRONG_MODE,
            TerminalFault::BadFirstRecord => BAD_FIRST,
            TerminalFault::OversizedReply => "",
            TerminalFault::Valid
            | TerminalFault::Trailing
            | TerminalFault::Withheld
            | TerminalFault::Reset => END,
        };
        let declared_length = match fault {
            TerminalFault::OversizedReply => 4_194_305,
            _ => wire.len() as u32,
        };
        send.write_all(&declared_length.to_be_bytes())
            .await
            .unwrap();
        if !wire.is_empty() {
            send.write_all(wire.as_bytes()).await.unwrap();
        }
        sent_id.send(send.id()).unwrap();
        match fault {
            TerminalFault::Trailing => {
                send.write_all(b"!").await.unwrap();
                send.finish().unwrap();
            }
            TerminalFault::Reset => {
                released.await.unwrap();
                send.reset(13_u8.into()).unwrap();
            }
            TerminalFault::Withheld => {
                let _ = released.await;
                let _ = send.reset(13_u8.into());
            }
            TerminalFault::Valid
            | TerminalFault::NestedUnknown
            | TerminalFault::WrongMode
            | TerminalFault::BadFirstRecord
            | TerminalFault::OversizedReply => {
                send.finish().unwrap();
            }
        }
        let _ = server_done.send(());
        let _ = time::timeout(Duration::from_secs(3), conn.closed()).await;
    });

    let (completed, completion) = oneshot::channel();
    let client = tokio::spawn(async move {
        let result = source
            .observe(
                ObserveArguments {
                    target: fixture_target(),
                    timeout_seconds: 10,
                    max_events: 51,
                    max_bytes: 8192,
                    after_sequence: None,
                    epoch: None,
                },
                CancellationToken::new(),
            )
            .await;
        match (fault, result) {
            (TerminalFault::Valid, Ok(mut frames)) => {
                assert!(matches!(frames.next().await,Some(agentstudio_gateway_contract::observation_records::SessionObservation::Ended(_))));
                assert!(frames.next().await.is_none());
            }
            (
                TerminalFault::Trailing
                | TerminalFault::Withheld
                | TerminalFault::Reset
                | TerminalFault::NestedUnknown
                | TerminalFault::WrongMode
                | TerminalFault::BadFirstRecord
                | TerminalFault::OversizedReply,
                Err(SessionSourceError::Unavailable),
            ) => {}
            (_, _) => unreachable!("terminal was admitted/refused unexpectedly"),
        }
        let _ = completed.send(());
    });
    let announced = time::timeout(Duration::from_secs(2), receive_id)
        .await
        .unwrap()
        .unwrap();
    if matches!(fault, TerminalFault::Reset | TerminalFault::Withheld) {
        let observed = time::timeout(Duration::from_secs(2), fin.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            observed, announced,
            "inject only after observing terminal decode"
        );
        assert!(!client.is_finished());
        if matches!(fault, TerminalFault::Reset) {
            release.send(()).unwrap();
        } else {
            time::timeout(Duration::from_secs(3), completion)
                .await
                .unwrap()
                .unwrap();
            let _ = release.send(());
        }
    }
    time::timeout(Duration::from_secs(5), client)
        .await
        .unwrap()
        .unwrap();
    time::timeout(Duration::from_secs(2), finished)
        .await
        .unwrap()
        .unwrap();
    time::timeout(Duration::from_secs(4), serving)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn terminal_requires_clean_fin_and_closed_nested_shapes_before_first_admission() {
    for fault in [
        TerminalFault::Valid,
        TerminalFault::Trailing,
        TerminalFault::Withheld,
        TerminalFault::Reset,
        TerminalFault::NestedUnknown,
        TerminalFault::WrongMode,
        TerminalFault::BadFirstRecord,
        TerminalFault::OversizedReply,
    ] {
        independently_written_response(fault).await;
    }
}
