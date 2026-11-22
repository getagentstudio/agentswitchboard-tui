use std::time::Duration;

use agentstudio_gateway_contract::{
    observation_records::{
        Cursor, EndReason, GapReason, OrderedRecord, ResultReason, SessionObservation,
    },
    session_contract::{InspectArguments, InspectResult, ObserveArguments, SessionRef},
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::time::Instant;

pub const WIRE_VERSION: u64 = 1;
pub const REQUEST_LIMIT: usize = 256 * 1024;
pub const REPLY_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Envelope<Payload> {
    wire_version: u64,
    service: String,
    operation: String,
    payload: Payload,
}

#[derive(Debug)]
pub enum SessionCall {
    Inspect(InspectArguments),
    Observe(ObserveArguments),
}

impl SessionCall {
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        match self {
            Self::Inspect(arguments) => encode_request("inspect", arguments),
            Self::Observe(arguments) => encode_request("observe", arguments),
        }
    }

    pub fn decode(bytes: &[u8], allowed_target: &SessionRef) -> Result<Self, RefusalCode> {
        let wire: Envelope<&RawValue> =
            serde_json::from_slice(bytes).map_err(|_| RefusalCode::InvalidFrame)?;
        if wire.wire_version != WIRE_VERSION {
            return Err(RefusalCode::IncompatibleVersion);
        }
        let call = match (wire.service.as_str(), wire.operation.as_str()) {
            ("sessions", "inspect") => {
                let arguments: InspectArguments = serde_json::from_str(wire.payload.get())
                    .map_err(|_| RefusalCode::InvalidArguments)?;
                authorize_target(&arguments.target, allowed_target)?;
                Self::Inspect(arguments)
            }
            ("sessions", "observe") => {
                let arguments: ObserveArguments = serde_json::from_str(wire.payload.get())
                    .map_err(|_| RefusalCode::InvalidArguments)?;
                authorize_target(&arguments.target, allowed_target)?;
                if arguments.timeout_seconds == 0
                    || Instant::now()
                        .checked_add(Duration::from_secs(arguments.timeout_seconds))
                        .is_none()
                    || !(1..=4096).contains(&arguments.max_events)
                    || !(1..=1048576).contains(&arguments.max_bytes)
                    || arguments.after_sequence.is_some()
                    || arguments.epoch.is_some()
                {
                    return Err(RefusalCode::InvalidArguments);
                }
                Self::Observe(arguments)
            }
            _ => return Err(RefusalCode::UnknownRoute),
        };
        Ok(call)
    }
}

fn authorize_target(target: &SessionRef, allowed_target: &SessionRef) -> Result<(), RefusalCode> {
    if target.is_valid() && target == allowed_target {
        Ok(())
    } else {
        Err(RefusalCode::ScopeDenied)
    }
}

fn encode_request<Payload: Serialize>(
    operation: &str,
    payload: Payload,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&Envelope {
        wire_version: WIRE_VERSION,
        service: "sessions".into(),
        operation: operation.into(),
        payload,
    })
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RefusalCode {
    IncompatibleVersion,
    UnknownRoute,
    InvalidFrame,
    InvalidArguments,
    ScopeDenied,
    FrameTooLarge,
    IntakeTimeout,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SourceErrorCode {
    Denied,
    Unavailable,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Reply {
    Refused { code: RefusalCode },
    SourceError { code: SourceErrorCode },
    InspectResult { result: InspectResult },
    ObservationItem { item: ObservationItem },
    ObservationEnd { end: ObservationEnd },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ObservationItem {
    History {
        cursor: Cursor,
        record: OrderedRecord,
    },
    Live {
        cursor: Cursor,
        record: OrderedRecord,
    },
    SnapshotEnd {
        cursor: Cursor,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ObservationEnd {
    Gap {
        cursor: Cursor,
        reason: GapReason,
    },
    Ended {
        reason: EndReason,
    },
    Result {
        epoch: u64,
        reason: ResultReason,
        records: Vec<OrderedRecord>,
    },
}

impl From<SessionObservation> for Reply {
    fn from(frame: SessionObservation) -> Self {
        match frame {
            SessionObservation::History { cursor, record } => Self::ObservationItem {
                item: ObservationItem::History { cursor, record },
            },
            SessionObservation::Live { cursor, record } => Self::ObservationItem {
                item: ObservationItem::Live { cursor, record },
            },
            SessionObservation::SnapshotEnd(cursor) => Self::ObservationItem {
                item: ObservationItem::SnapshotEnd { cursor },
            },
            SessionObservation::Gap { cursor, reason } => Self::ObservationEnd {
                end: ObservationEnd::Gap { cursor, reason },
            },
            SessionObservation::Ended(reason) => Self::ObservationEnd {
                end: ObservationEnd::Ended { reason },
            },
            SessionObservation::Result {
                epoch,
                reason,
                records,
            } => Self::ObservationEnd {
                end: ObservationEnd::Result {
                    epoch,
                    reason,
                    records,
                },
            },
        }
    }
}

impl From<ObservationItem> for SessionObservation {
    fn from(item: ObservationItem) -> Self {
        match item {
            ObservationItem::History { cursor, record } => Self::History { cursor, record },
            ObservationItem::Live { cursor, record } => Self::Live { cursor, record },
            ObservationItem::SnapshotEnd { cursor } => Self::SnapshotEnd(cursor),
        }
    }
}

impl From<ObservationEnd> for SessionObservation {
    fn from(end: ObservationEnd) -> Self {
        match end {
            ObservationEnd::Gap { cursor, reason } => Self::Gap { cursor, reason },
            ObservationEnd::Ended { reason } => Self::Ended(reason),
            ObservationEnd::Result {
                epoch,
                reason,
                records,
            } => Self::Result {
                epoch,
                reason,
                records,
            },
        }
    }
}
