use std::{
    convert::Infallible,
    io,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    Method, Request, Response, StatusCode, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde_json::value::RawValue;
use tokio::{net::UnixListener, task::JoinSet, time};
use tokio_util::sync::CancellationToken;

use agentstudio_gateway_contract::{
    local_gateway_wire::{
        EnvelopeCode, EnvelopeError, EnvelopeRefusal, REQUEST_BYTES, RESPONSE_BYTES, RequestId,
        SSE_DATA_BYTES, TextContent, ToolRequest, ToolResponse, ToolResult, VERSION_HEADER,
        WIRE_VERSION,
    },
    session_contract::{InspectArguments, ObserveArguments, SessionRef},
};

use super::{
    GatewayResponse,
    observation_response::{ObservationLimits, encode_observation},
};
use crate::session::session_source::{SessionSource, SessionSourceError};

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("Gateway requires an owner-private socket directory")]
    PrivateDirectoryRequired,
    #[error("Gateway socket I/O: {0}")]
    SocketIo(#[from] io::Error),
}

pub struct GatewayServer<Source> {
    listener: UnixListener,
    socket: OwnedSocket,
    owner_uid: u32,
    source: Arc<Source>,
    allowed_target: SessionRef,
}

impl<Source: SessionSource + 'static> GatewayServer<Source> {
    pub async fn bind(
        socket_path: &Path,
        source: Source,
        allowed_target: SessionRef,
    ) -> Result<Self, GatewayError> {
        let directory = socket_path
            .parent()
            .ok_or(GatewayError::PrivateDirectoryRequired)?;
        let metadata = tokio::fs::symlink_metadata(directory).await?;
        if !socket_path.is_absolute()
            || !metadata.is_dir()
            || metadata.mode() & 0o777 != 0o700
            || metadata.uid() != rustix::process::geteuid().as_raw()
        {
            return Err(GatewayError::PrivateDirectoryRequired);
        }
        let listener = UnixListener::bind(socket_path)?;
        let metadata_socket = tokio::fs::symlink_metadata(socket_path).await?;
        let socket = OwnedSocket {
            path: socket_path.into(),
            device: metadata_socket.dev(),
            inode: metadata_socket.ino(),
        };
        tokio::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600)).await?;
        Ok(Self {
            listener,
            socket,
            owner_uid: metadata.uid(),
            source: Arc::new(source),
            allowed_target,
        })
    }

    pub async fn serve(self, stop: CancellationToken) -> Result<(), GatewayError> {
        let mut connections = JoinSet::new();
        eprintln!("gateway-ready socket={}", self.socket.path.display());
        let result = loop {
            tokio::select! {
                () = stop.cancelled() => break Ok(()),
                _ = connections.join_next(), if !connections.is_empty() => {}
                accepted = self.listener.accept() => {
                    let (socket, _) = match accepted {
                        Ok(connection) => connection,
                        Err(error) => break Err(error.into()),
                    };
                    let allowed_peer = socket.peer_cred().is_ok_and(|peer| peer.uid() == self.owner_uid);
                    let source = self.source.clone();
                    let allowed_target = self.allowed_target.clone();
                    let connection_stop = stop.child_token();
                    connections.spawn(async move {
                        let service_stop = connection_stop.clone();
                        let server = http1::Builder::new().keep_alive(false).serve_connection(
                            TokioIo::new(socket),
                            service_fn(move |request| {
                                let source = source.clone();
                                let target = allowed_target.clone();
                                let request_stop = service_stop.child_token();
                                async move {
                                    Ok::<_, Infallible>(dispatch(request, &*source, &target, allowed_peer, request_stop).await)
                                }
                            }),
                        );
                        tokio::select! {
                            () = connection_stop.cancelled() => {}
                            _ = server => {}
                        }
                        connection_stop.cancel();
                    });
                }
            }
        };
        stop.cancel();
        while connections.join_next().await.is_some() {}
        result
    }
}

struct OwnedSocket {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Drop for OwnedSocket {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.file_type().is_socket()
                && metadata.dev() == self.device
                && metadata.ino() == self.inode
        }) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

async fn dispatch(
    request: Request<Incoming>,
    source: &impl SessionSource,
    allowed_target: &SessionRef,
    allowed_peer: bool,
    cancellation: CancellationToken,
) -> GatewayResponse {
    let (parts, body) = request.into_parts();
    if parts
        .headers
        .get(VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some(WIRE_VERSION)
    {
        return refusal(EnvelopeCode::IncompatibleVersion);
    }
    if !allowed_peer {
        return refusal(EnvelopeCode::ScopeDenied);
    }
    if parts.method != Method::POST || parts.uri.path() != "/mcp" {
        return refusal(EnvelopeCode::InvalidFrame);
    }
    if parts
        .headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > REQUEST_BYTES as u64)
    {
        return refusal(EnvelopeCode::FrameTooLarge);
    }
    let collected = match time::timeout(
        Duration::from_secs(5),
        Limited::new(body, REQUEST_BYTES).collect(),
    )
    .await
    {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(error)) if error.is::<http_body_util::LengthLimitError>() => {
            return refusal(EnvelopeCode::FrameTooLarge);
        }
        _ => return refusal(EnvelopeCode::InvalidFrame),
    };
    let Ok(request) = serde_json::from_slice::<ToolRequest<Box<RawValue>>>(&collected) else {
        return refusal(EnvelopeCode::InvalidFrame);
    };
    if request.jsonrpc != "2.0" || request.method != "tools/call" {
        return refusal(EnvelopeCode::InvalidFrame);
    }

    let ToolRequest { id, params, .. } = request;
    match params.name.as_str() {
        "provider_session_inspect" => {
            let Ok(arguments) = serde_json::from_str::<InspectArguments>(params.arguments.get())
            else {
                return refusal(EnvelopeCode::InvalidFrame);
            };
            if !arguments.target.is_valid() || &arguments.target != allowed_target {
                return refusal(EnvelopeCode::ScopeDenied);
            }
            eprintln!("gateway-dispatch tool=provider_session_inspect request={id:?}");
            match source.inspect(arguments).await {
                Ok(result) => encode_tool_response(id, result, false),
                Err(error) => encode_source_error(id, error),
            }
        }
        "events_observe" => {
            let Ok(arguments) = serde_json::from_str::<ObserveArguments>(params.arguments.get())
            else {
                return refusal(EnvelopeCode::InvalidFrame);
            };
            if !arguments.target.is_valid() || &arguments.target != allowed_target {
                return refusal(EnvelopeCode::ScopeDenied);
            }
            if arguments.timeout_seconds == 0
                || !(1..=4096).contains(&arguments.max_events)
                || !(1..=SSE_DATA_BYTES).contains(&arguments.max_bytes)
                || arguments.after_sequence.is_some()
                || arguments.epoch.is_some()
                || !parts
                    .headers
                    .get(hyper::header::ACCEPT)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| {
                        value
                            .split(',')
                            .any(|item| item.trim() == "text/event-stream")
                    })
            {
                return refusal(EnvelopeCode::InvalidFrame);
            }
            let limits = ObservationLimits {
                max_events: arguments.max_events,
                max_bytes: arguments.max_bytes,
            };
            let target = arguments.target.clone();
            eprintln!("gateway-dispatch tool=events_observe request={id:?}");
            match source.observe(arguments, cancellation.clone()).await {
                Ok(observation) => {
                    encode_observation(observation, cancellation, id, target, limits)
                }
                Err(error) => encode_source_error(id, error),
            }
        }
        _ => refusal(EnvelopeCode::ScopeDenied),
    }
}

fn encode_source_error(id: RequestId, error: SessionSourceError) -> GatewayResponse {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ToolFailure<'message> {
        mcp_result: &'static str,
        kind: &'static str,
        message: &'message str,
    }
    let kind = match error {
        SessionSourceError::Denied => "notFound",
        SessionSourceError::Unavailable => "unavailable",
    };
    encode_tool_response(
        id,
        ToolFailure {
            mcp_result: "error",
            kind,
            message: &error.to_string(),
        },
        true,
    )
}

fn encode_tool_response<T: Serialize>(id: RequestId, value: T, is_error: bool) -> GatewayResponse {
    let Ok(text) = serde_json::to_string(&value) else {
        return refusal(EnvelopeCode::Unavailable);
    };
    let response = ToolResponse {
        jsonrpc: "2.0",
        id,
        result: ToolResult {
            content: [TextContent { kind: "text", text }],
            structured_content: value,
            is_error,
        },
    };
    match serde_json::to_vec(&response) {
        Ok(body) if body.len() <= RESPONSE_BYTES => json_body(StatusCode::OK, body),
        Ok(_) => refusal(EnvelopeCode::FrameTooLarge),
        Err(_) => refusal(EnvelopeCode::Unavailable),
    }
}

fn refusal(code: EnvelopeCode) -> GatewayResponse {
    let (status, message) = match code {
        EnvelopeCode::IncompatibleVersion => (
            StatusCode::UPGRADE_REQUIRED,
            "Unsupported AgentStudio-Wire-Version",
        ),
        EnvelopeCode::InvalidFrame => (StatusCode::BAD_REQUEST, "Invalid Gateway request frame"),
        EnvelopeCode::ScopeDenied => (
            StatusCode::FORBIDDEN,
            "Gateway request is outside attachment scope",
        ),
        EnvelopeCode::FrameTooLarge => (
            StatusCode::PAYLOAD_TOO_LARGE,
            "Gateway frame exceeds its byte limit",
        ),
        EnvelopeCode::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "Gateway is unavailable"),
    };
    let supported_versions =
        matches!(code, EnvelopeCode::IncompatibleVersion).then_some([WIRE_VERSION]);
    let content = EnvelopeRefusal {
        envelope_error: EnvelopeError {
            code,
            message,
            supported_versions,
        },
    };
    json_body(status, serde_json::to_vec(&content).unwrap_or_default())
}

fn json_body(status: StatusCode, body: Vec<u8>) -> GatewayResponse {
    let mut response = Response::new(Full::new(Bytes::from(body)).boxed_unsync());
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response
}

#[cfg(test)]
#[path = "unix_gateway_server_test.rs"]
mod tests;
