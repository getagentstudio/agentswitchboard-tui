use std::{net::SocketAddr, time::Duration};

use agentstudio_gateway::session::session_source::{
    ObservationStream, SessionSource, SessionSourceError,
};
use agentstudio_gateway::unix_http::accepts_initial_observation;
use agentstudio_gateway_contract::{
    observation_records::SessionObservation,
    session_contract::{InspectArguments, InspectResult, ObserveArguments},
};
use futures_util::stream;
use quinn::{Connection, Endpoint, StreamId};
use state_machine_core::StateTag;
use tokio::{
    sync::{Mutex, broadcast},
    time,
};
use tokio_util::sync::CancellationToken;

use crate::{
    call_lifecycle::{ResponsePhase, next_response},
    endpoint_config::{EndpointError, SERVER_NAME, require_loopback},
    fixture_wire::{REPLY_LIMIT, REQUEST_LIMIT, RefusalCode, Reply, SessionCall, SourceErrorCode},
    framed_stream::{FrameError, Receiving, Sending},
};

pub const CONNECTION_DEADLINE: Duration = Duration::from_secs(2);
pub const FIRST_FRAME_DEADLINE: Duration = Duration::from_secs(4);
pub const RESPONSE_FIN_DEADLINE: Duration = Duration::from_secs(2);

pub struct QuinnSessionSource {
    endpoint: Endpoint,
    peer: SocketAddr,
    connection: Mutex<ConnectionState>,
    fin_waits: broadcast::Sender<StreamId>,
}

enum ConnectionState {
    Unattempted,
    Connected(Connection),
    Failed,
}

#[derive(Debug, thiserror::Error)]
enum CallFailure {
    #[error("peer route: {0}")]
    Route(String),
    #[error("call deadline elapsed")]
    Deadline,
    #[error("call cancelled")]
    Cancelled,
    #[error("malformed response in {0} phase")]
    InvalidMode(&'static str),
    #[error("inspection target did not match")]
    Target,
    #[error("observation first frame failed admission")]
    InitialPayload,
    #[error("peer refused: {0:?}")]
    Refused(RefusalCode),
    #[error("source setup failed: {0:?}")]
    Source(SourceErrorCode),
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("invalid peer JSON: {0}")]
    Json(#[from] serde_json::Error),
}

impl QuinnSessionSource {
    pub fn new(endpoint: Endpoint, peer: SocketAddr) -> Result<Self, EndpointError> {
        require_loopback(endpoint.local_addr()?)?;
        require_loopback(peer)?;
        Ok(Self {
            endpoint,
            peer,
            connection: Mutex::new(ConnectionState::Unattempted),
            fin_waits: broadcast::channel(16).0,
        })
    }

    pub fn subscribe_fin_waits(&self) -> broadcast::Receiver<StreamId> {
        self.fin_waits.subscribe()
    }

    async fn connection(&self) -> Result<Connection, CallFailure> {
        let mut state = self.connection.lock().await;
        match &*state {
            ConnectionState::Connected(connection) => return Ok(connection.clone()),
            ConnectionState::Failed => {
                return Err(CallFailure::Route(
                    "the run's peer connection failed".into(),
                ));
            }
            ConnectionState::Unattempted => {}
        }
        let attempt = self
            .endpoint
            .connect(self.peer, SERVER_NAME)
            .map_err(|error| CallFailure::Route(error.to_string()))?;
        let connected = time::timeout(CONNECTION_DEADLINE, attempt).await;
        let connection = match connected {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => {
                *state = ConnectionState::Failed;
                return Err(CallFailure::Route(error.to_string()));
            }
            Err(_) => {
                *state = ConnectionState::Failed;
                return Err(CallFailure::Deadline);
            }
        };
        eprintln!(
            "quinn-gateway-connection local={:?} remote={} stable={}",
            self.endpoint.local_addr(),
            connection.remote_address(),
            connection.stable_id()
        );
        *state = ConnectionState::Connected(connection.clone());
        Ok(connection)
    }

    async fn request(&self, call: SessionCall) -> Result<Receiving, CallFailure> {
        let connection = self.connection().await?;
        let (send, receive) = time::timeout(CONNECTION_DEADLINE, connection.open_bi())
            .await
            .map_err(|_| CallFailure::Deadline)?
            .map_err(|error| CallFailure::Route(error.to_string()))?;
        let mut write = Sending::new(send);
        let reader = Receiving::new(receive);
        let mode = match &call {
            SessionCall::Inspect(_) => "inspect",
            SessionCall::Observe(_) => "observe",
        };
        eprintln!(
            "quinn-gateway-call operation={mode} stream={} stable={}",
            reader.stream_id().index(),
            connection.stable_id()
        );
        write
            .send_bytes(&call.encode()?, REQUEST_LIMIT, CONNECTION_DEADLINE)
            .await?;
        write.finish()?;
        Ok(reader)
    }
}

impl SessionSource for QuinnSessionSource {
    async fn inspect(
        &self,
        request: InspectArguments,
    ) -> Result<InspectResult, SessionSourceError> {
        let target = request.target.clone();
        let consume = async {
            let mut reader = self.request(SessionCall::Inspect(request)).await?;
            let (reply, _) = response(&mut reader, ResponsePhase::Inspect, &self.fin_waits).await?;
            match reply {
                Reply::InspectResult { result } if result.target == target => Ok(result),
                Reply::InspectResult { .. } => Err(CallFailure::Target),
                Reply::Refused { code } => Err(CallFailure::Refused(code)),
                Reply::SourceError { code } => Err(CallFailure::Source(code)),
                Reply::ObservationItem { .. } | Reply::ObservationEnd { .. } => {
                    Err(CallFailure::InvalidMode("inspect"))
                }
            }
        };
        time::timeout(FIRST_FRAME_DEADLINE, consume)
            .await
            .map_err(|_| CallFailure::Deadline)
            .and_then(|result| result)
            .map_err(|error| local_error("inspect", error))
    }

    async fn observe(
        &self,
        request: ObserveArguments,
        cancel: CancellationToken,
    ) -> Result<ObservationStream, SessionSourceError> {
        let handshake = async {
            let mut reader = self.request(SessionCall::Observe(request)).await?;
            let (reply, phase) =
                response(&mut reader, ResponsePhase::Observe, &self.fin_waits).await?;
            let initial = observation_reply(reply)?;
            if !accepts_initial_observation(&initial) {
                return Err(CallFailure::InitialPayload);
            }
            Ok((reader, phase, initial))
        };
        let (reader, phase, first) = tokio::select! {
            () = cancel.cancelled() => Err(CallFailure::Cancelled),
            admitted = time::timeout(FIRST_FRAME_DEADLINE, handshake) => {
                admitted.map_err(|_|CallFailure::Deadline).and_then(|value|value)
            }
        }
        .map_err(|error| local_error("observe-before-admission", error))?;

        eprintln!(
            "quinn-gateway-observation-admitted stream={}",
            reader.stream_id().index()
        );
        let observation = stream::unfold(
            Some((reader, phase, Some(first), cancel, self.fin_waits.clone())),
            |state| async move {
                let (mut reader, phase, first, cancel, fin_waits) = state?;
                if let Some(first) = first {
                    let remaining =
                        (!phase.is_terminal()).then_some((reader, phase, None, cancel, fin_waits));
                    return Some((first, remaining));
                }
                let decoded = tokio::select! {
                    () = cancel.cancelled() => return None,
                    result = response(&mut reader, phase, &fin_waits) => result,
                };
                match decoded
                    .and_then(|(reply, next)| observation_reply(reply).map(|value| (value, next)))
                {
                    Ok((frame, next)) => {
                        let remaining = (!next.is_terminal())
                            .then_some((reader, next, None, cancel, fin_waits));
                        Some((frame, remaining))
                    }
                    Err(error) => {
                        eprintln!(
                            "quinn-gateway-observation-lost stream={} cause={error}",
                            reader.stream_id().index()
                        );
                        None
                    }
                }
            },
        );
        Ok(Box::pin(observation))
    }
}

async fn response(
    reader: &mut Receiving,
    phase: ResponsePhase,
    fin_waits: &broadcast::Sender<StreamId>,
) -> Result<(Reply, ResponsePhase), CallFailure> {
    let raw = reader.read_frame(REPLY_LIMIT).await?;
    let reply = serde_json::from_slice(&raw)?;
    let next =
        next_response(phase, &reply).map_err(|_| CallFailure::InvalidMode(phase.state_tag()))?;
    if next.is_terminal() {
        let _ = fin_waits.send(reader.stream_id());
        time::timeout(RESPONSE_FIN_DEADLINE, reader.confirm_fin())
            .await
            .map_err(|_| CallFailure::Deadline)??;
    }
    Ok((reply, next))
}

fn observation_reply(reply: Reply) -> Result<SessionObservation, CallFailure> {
    match reply {
        Reply::ObservationItem { item } => Ok(item.into()),
        Reply::ObservationEnd { end } => Ok(end.into()),
        Reply::Refused { code } => Err(CallFailure::Refused(code)),
        Reply::SourceError { code } => Err(CallFailure::Source(code)),
        Reply::InspectResult { .. } => Err(CallFailure::InvalidMode("observe")),
    }
}

fn local_error(operation: &str, error: CallFailure) -> SessionSourceError {
    eprintln!("quinn-gateway-call-failed operation={operation} cause={error}");
    match error {
        CallFailure::Refused(RefusalCode::ScopeDenied)
        | CallFailure::Source(SourceErrorCode::Denied) => SessionSourceError::Denied,
        CallFailure::Refused(_)
        | CallFailure::Source(SourceErrorCode::Unavailable)
        | CallFailure::Route(_)
        | CallFailure::Deadline
        | CallFailure::Cancelled
        | CallFailure::InvalidMode(_)
        | CallFailure::Target
        | CallFailure::InitialPayload
        | CallFailure::Frame(_)
        | CallFailure::Json(_) => SessionSourceError::Unavailable,
    }
}
