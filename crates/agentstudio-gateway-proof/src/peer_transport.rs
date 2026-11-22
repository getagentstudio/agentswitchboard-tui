use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use agentstudio_gateway::session::session_source::{SessionSource, SessionSourceError};
use agentstudio_gateway_contract::session_contract::SessionRef;
use futures_util::StreamExt;
use quinn::{Connection, Endpoint, ReadExactError, WriteError};
use state_machine_core::StateTag;
use tokio::{sync::broadcast, task::JoinSet, time};
use tokio_util::sync::CancellationToken;

use crate::{
    call_lifecycle::{ResponsePhase, next_response},
    fixture_wire::{REPLY_LIMIT, REQUEST_LIMIT, RefusalCode, Reply, SessionCall, SourceErrorCode},
    framed_stream::{CreditPending, FrameError, Receiving, Sending},
};

#[derive(Clone, Copy)]
pub struct PeerBounds {
    pub intake: Duration,
    pub write: Duration,
    pub finish_ack: Duration,
}

impl Default for PeerBounds {
    fn default() -> Self {
        Self {
            intake: Duration::from_secs(2),
            write: Duration::from_secs(4),
            finish_ack: Duration::from_secs(2),
        }
    }
}

#[derive(Clone)]
pub struct PeerControls {
    streams: Arc<Mutex<HashMap<u64, CancellationToken>>>,
    credits: broadcast::Sender<CreditPending>,
}

impl Default for PeerControls {
    fn default() -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
            credits: broadcast::channel(32).0,
        }
    }
}

impl PeerControls {
    pub fn abort_stream(&self, stream: u64) -> bool {
        let Ok(streams) = self.streams.lock() else {
            return false;
        };
        let Some(token) = streams.get(&stream) else {
            return false;
        };
        token.cancel();
        true
    }

    pub fn credits(&self) -> broadcast::Receiver<CreditPending> {
        self.credits.subscribe()
    }

    fn register(&self, stream: u64) -> Option<StreamAbort> {
        let mut streams = self.streams.lock().ok()?;
        if streams.contains_key(&stream) {
            return None;
        }
        let cancel = CancellationToken::new();
        streams.insert(stream, cancel.clone());
        Some(StreamAbort {
            stream,
            cancel,
            streams: self.streams.clone(),
        })
    }
}

struct StreamAbort {
    stream: u64,
    cancel: CancellationToken,
    streams: Arc<Mutex<HashMap<u64, CancellationToken>>>,
}

impl Drop for StreamAbort {
    fn drop(&mut self) {
        if let Ok(mut streams) = self.streams.lock() {
            streams.remove(&self.stream);
        }
    }
}

pub struct PeerTransport<Source> {
    pub endpoint: Endpoint,
    pub source: Source,
    pub allowed_target: SessionRef,
    pub bounds: PeerBounds,
    pub controls: PeerControls,
}

impl<Source: SessionSource + 'static> PeerTransport<Source> {
    pub async fn serve(self, stop: CancellationToken) {
        let endpoint = self.endpoint.clone();
        loop {
            let incoming = tokio::select! {
                () = stop.cancelled() => break,
                incoming = endpoint.accept() => match incoming {
                    Some(incoming) => incoming,
                    None => break,
                }
            };
            let handshake = tokio::select! {
                () = stop.cancelled() => break,
                result = time::timeout(self.bounds.intake, incoming) => result,
            };
            match handshake {
                Ok(Ok(connection)) => {
                    eprintln!(
                        "quinn-peer-connection local={:?} remote={} stable={}",
                        endpoint.local_addr(),
                        connection.remote_address(),
                        connection.stable_id()
                    );
                    self.serve_connection(connection, &stop).await;
                    break;
                }
                Ok(Err(error)) => eprintln!("quinn-peer-handshake-failed cause={error}"),
                Err(_) => eprintln!("quinn-peer-handshake-failed cause=deadline"),
            }
        }
        endpoint.close(0_u8.into(), b"proof shutdown");
        endpoint.wait_idle().await;
    }

    async fn serve_connection(self, connection: Connection, stop: &CancellationToken) {
        let source = Arc::new(self.source);
        let mut calls = JoinSet::new();
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                _ = calls.join_next(), if !calls.is_empty() => {}
                opened = connection.accept_bi() => {
                    let Ok((send, recv)) = opened else { break; };
                    let stream = send.id().index();
                    let Some(abort) = self.controls.register(stream) else { continue; };
                    let call = ActiveCall {
                        stream,
                        connection: connection.clone(),
                        source: source.clone(),
                        target: self.allowed_target.clone(),
                        bounds: self.bounds,
                        stop: stop.child_token(),
                        abort,
                        send: Sending::new(send).with_credit_receipts(self.controls.credits.clone()),
                        receive: Receiving::new(recv),
                    };
                    calls.spawn(call.run());
                }
            }
        }
        while calls.join_next().await.is_some() {}
    }
}

struct ActiveCall<Source> {
    stream: u64,
    connection: Connection,
    source: Arc<Source>,
    target: SessionRef,
    bounds: PeerBounds,
    stop: CancellationToken,
    abort: StreamAbort,
    send: Sending,
    receive: Receiving,
}

#[derive(Debug, thiserror::Error)]
enum PumpError {
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("source EOF before terminal reply")]
    SourceEof,
    #[error("peer attempted an invalid response transition")]
    InvalidTransition,
}

impl<Source: SessionSource> ActiveCall<Source> {
    async fn run(mut self) {
        let stopped = self.send.stopped();
        let source_cancellation = CancellationToken::new();
        let stream = self.stream;
        let abort = self.abort.cancel.clone();
        let stop = self.stop.clone();
        let connection = self.connection.clone();
        let pumped = tokio::select! {
            biased;
            outcome = stopped => {
                match outcome {
                    Ok(Some(code)) => eprintln!("quinn-peer-stopped stream={stream} code={code}"),
                    Ok(None) => eprintln!("quinn-peer-finish-ack stream={stream}"),
                    Err(error) => eprintln!("quinn-peer-stop-failed stream={stream} cause={error}"),
                };
                None
            }
            () = abort.cancelled() => { eprintln!("quinn-peer-injected-abort stream={stream}"); None }
            () = stop.cancelled() => { eprintln!("quinn-peer-shutdown stream={stream}"); None }
            error = connection.closed() => { eprintln!("quinn-peer-connection-lost stream={stream} cause={error}"); None }
            result = self.pump(source_cancellation.clone()) => Some(result),
        };
        source_cancellation.cancel();
        match pumped {
            Some(Ok(())) => {
                if matches!(
                    time::timeout(self.bounds.finish_ack, self.send.stopped()).await,
                    Ok(Ok(None))
                ) {
                    eprintln!("quinn-peer-finish-ack stream={stream}");
                }
            }
            Some(Err(PumpError::Frame(FrameError::Write(WriteError::Stopped(code))))) => {
                eprintln!("quinn-peer-write-stopped stream={stream} code={code}");
            }
            Some(Err(error)) => eprintln!("quinn-peer-pump-failed stream={stream} cause={error}"),
            None => {}
        }
    }

    async fn pump(&mut self, cancellation: CancellationToken) -> Result<(), PumpError> {
        let request = time::timeout(self.bounds.intake, async {
            let bytes = self.receive.read_frame(REQUEST_LIMIT).await?;
            self.receive.confirm_fin().await?;
            Ok::<_, FrameError>(bytes)
        })
        .await;
        let decoded = match request {
            Ok(Ok(bytes)) => SessionCall::decode(&bytes, &self.target),
            Ok(Err(FrameError::TooLarge(_))) => Err(RefusalCode::FrameTooLarge),
            Ok(Err(
                FrameError::Empty
                | FrameError::Trailing
                | FrameError::Prefix(ReadExactError::FinishedEarly(_))
                | FrameError::Body(ReadExactError::FinishedEarly(_)),
            )) => Err(RefusalCode::InvalidFrame),
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => Err(RefusalCode::IntakeTimeout),
        };
        let call = match decoded {
            Ok(call) => call,
            Err(code) => {
                eprintln!("quinn-peer-refused stream={} code={code:?}", self.stream);
                self.send
                    .send_json(&Reply::Refused { code }, REPLY_LIMIT, self.bounds.write)
                    .await?;
                self.send.finish()?;
                return Ok(());
            }
        };

        match call {
            SessionCall::Inspect(arguments) => {
                eprintln!(
                    "quinn-peer-dispatch stream={} operation=inspect",
                    self.stream
                );
                let reply = match self.source.inspect(arguments).await {
                    Ok(result) => Reply::InspectResult { result },
                    Err(error) => self.source_error(error),
                };
                self.send_response(ResponsePhase::Inspect, &reply).await?;
            }
            SessionCall::Observe(arguments) => {
                eprintln!(
                    "quinn-peer-dispatch stream={} operation=observe timeout={} max_events={} max_bytes={}",
                    self.stream,
                    arguments.timeout_seconds,
                    arguments.max_events,
                    arguments.max_bytes
                );
                let source = self.source.observe(arguments, cancellation).await;
                match source {
                    Ok(mut source) => {
                        let mut phase = ResponsePhase::Observe;
                        loop {
                            let observation = source.next().await.ok_or(PumpError::SourceEof)?;
                            let reply = Reply::from(observation);
                            phase = self.send_response(phase, &reply).await?;
                            if phase.is_terminal() {
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        let reply = self.source_error(error);
                        self.send_response(ResponsePhase::Observe, &reply).await?;
                    }
                }
            }
        }
        self.send.finish()?;
        Ok(())
    }

    fn source_error(&self, error: SessionSourceError) -> Reply {
        let code = match error {
            SessionSourceError::Denied => SourceErrorCode::Denied,
            SessionSourceError::Unavailable => SourceErrorCode::Unavailable,
        };
        eprintln!(
            "quinn-peer-source-error stream={} code={code:?}",
            self.stream
        );
        Reply::SourceError { code }
    }

    async fn send_response(
        &mut self,
        phase: ResponsePhase,
        reply: &Reply,
    ) -> Result<ResponsePhase, PumpError> {
        let next = next_response(phase, reply).map_err(|_| PumpError::InvalidTransition)?;
        self.send
            .send_json(reply, REPLY_LIMIT, self.bounds.write)
            .await?;
        let kind = reply_label(reply);
        eprintln!("quinn-peer-frame-sent stream={} kind={kind}", self.stream);
        Ok(next)
    }
}

fn reply_label(reply: &Reply) -> &'static str {
    use crate::fixture_wire::ObservationItem;
    match reply {
        Reply::Refused { .. } => "refused",
        Reply::SourceError { .. } => "sourceError",
        Reply::InspectResult { .. } => "inspectResult",
        Reply::ObservationItem {
            item: ObservationItem::History { .. },
        } => "history",
        Reply::ObservationItem {
            item: ObservationItem::Live { .. },
        } => "live",
        Reply::ObservationItem {
            item: ObservationItem::SnapshotEnd { .. },
        } => "snapshotEnd",
        Reply::ObservationEnd { .. } => "observationEnd",
    }
}
