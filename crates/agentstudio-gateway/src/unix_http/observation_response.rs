use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures_util::{StreamExt, stream};
use http_body_util::{BodyExt, StreamBody};
use hyper::{Response, body::Frame};
use serde::Serialize;
use tokio::time;
use tokio_util::sync::CancellationToken;

use super::GatewayResponse;
use crate::session::session_source::ObservationStream;
use agentstudio_gateway_contract::{
    local_gateway_wire::{
        EndedFrame, GapFrame, HeartbeatFrame, ItemFrame, ItemPhase, RequestId, SSE_DATA_BYTES,
        SnapshotEndFrame, TextContent, ToolResponse, ToolResult,
    },
    observation_records::{
        Cursor, EndReason, GapReason, OrderedRecord, ResultReason, SessionObservation,
    },
    session_contract::SessionRef,
};

pub(crate) struct ObservationLimits {
    pub(crate) max_events: usize,
    pub(crate) max_bytes: usize,
}

struct EmittedRecords {
    items: Vec<OrderedRecord>,
    size: usize,
    position: Option<Cursor>,
}

impl EmittedRecords {
    fn append(&mut self, cursor: Cursor, record: OrderedRecord, limit: &ObservationLimits) -> bool {
        let Ok(bytes) = serde_json::to_vec(&record) else {
            return false;
        };
        if self.items.len() >= limit.max_events || self.size + bytes.len() > limit.max_bytes {
            return false;
        }
        self.items.push(record);
        self.size += bytes.len();
        self.position = Some(cursor);
        true
    }

    fn terminal_result(
        &self,
        id: &RequestId,
        target: &SessionRef,
        source_epoch: u64,
        reason: ResultReason,
        mut records: Vec<OrderedRecord>,
    ) -> Option<Bytes> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Generation {
            service_epoch: &'static str,
            generation: u64,
        }
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct BoundedResult<'record> {
            target: &'record SessionRef,
            generation: Generation,
            attached: bool,
            events: &'record [OrderedRecord],
            end_reason: ResultReason,
            continuation_gap: bool,
            epoch: u64,
        }
        if records.is_empty() {
            records.clone_from(&self.items);
        }
        let result = BoundedResult {
            target,
            generation: Generation {
                service_epoch: "00000000-0000-4000-8000-000000000002",
                generation: 1,
            },
            attached: true,
            events: &records,
            end_reason: reason,
            continuation_gap: true,
            epoch: source_epoch,
        };
        let response = ToolResponse {
            jsonrpc: "2.0",
            id: id.clone(),
            result: ToolResult {
                content: [TextContent {
                    kind: "text",
                    text: serde_json::to_string(&result).ok()?,
                }],
                structured_content: result,
                is_error: false,
            },
        };
        encode_frame("result", &response).or_else(|| {
            let covered = self
                .position
                .is_some_and(|cursor| cursor.epoch == source_epoch)
                && records.iter().all(|record| self.items.contains(record));
            if covered {
                encode_frame(
                    "ended",
                    &EndedFrame {
                        reason: EndReason::Completed,
                    },
                )
            } else {
                gap_frame(self.position, GapReason::EventTooLarge)
            }
        })
    }
}

pub(crate) fn encode_observation(
    observation: ObservationStream,
    cancellation: CancellationToken,
    id: RequestId,
    target: SessionRef,
    limits: ObservationLimits,
) -> GatewayResponse {
    let mut heartbeat = time::interval_at(
        time::Instant::now() + Duration::from_secs(15),
        Duration::from_secs(15),
    );
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    eprintln!("gateway-observation-open request={id:?}");
    let lease = ObservationLease {
        id: id.clone(),
        cancellation,
    };
    let emitted = EmittedRecords {
        items: Vec::new(),
        size: 0,
        position: None,
    };
    let settings = Arc::new((id, target, limits));
    let body = stream::unfold(
        (observation, heartbeat, lease, false, emitted, settings),
        |(mut source, mut heartbeat, lease, terminal, mut emitted, settings)| async move {
            if terminal {
                return None;
            }
            let (id, target, limits) = &*settings;
            let next = tokio::select! {
                item = source.next() => item,
                _ = heartbeat.tick() => {
                    let millis = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |time| time.as_millis());
                    let bytes = encode_frame("heartbeat", &HeartbeatFrame { server_ts_ms: u64::try_from(millis).unwrap_or_default() })?;
                    return Some((Ok(Frame::data(bytes)), (source, heartbeat, lease, false, emitted, settings)));
                }
            };
            let (bytes, terminal) = match next? {
                next @ (SessionObservation::History { .. } | SessionObservation::Live { .. }) => {
                    let (phase, cursor, record) = match next {
                        SessionObservation::History { cursor, record } => {
                            (ItemPhase::History, cursor, record)
                        }
                        SessionObservation::Live { cursor, record } => {
                            (ItemPhase::Live, cursor, record)
                        }
                        _ => return None,
                    };
                    let frame = ItemFrame {
                        phase,
                        cursor: cursor.into(),
                        record,
                    };
                    match encode_frame("item", &frame) {
                        Some(bytes) if emitted.append(cursor, frame.record, limits) => {
                            (bytes, false)
                        }
                        Some(_) => (
                            emitted.terminal_result(
                                id,
                                target,
                                cursor.epoch,
                                ResultReason::ResultLimitReached,
                                Vec::new(),
                            )?,
                            true,
                        ),
                        None => (gap_frame(emitted.position, GapReason::EventTooLarge)?, true),
                    }
                }
                SessionObservation::SnapshotEnd(cursor) => {
                    emitted.position = Some(cursor);
                    (
                        encode_frame(
                            "snapshotEnd",
                            &SnapshotEndFrame {
                                cursor: cursor.into(),
                            },
                        )?,
                        false,
                    )
                }
                SessionObservation::Gap { cursor, reason } => {
                    (gap_frame(Some(cursor), reason)?, true)
                }
                SessionObservation::Ended(reason) => {
                    (encode_frame("ended", &EndedFrame { reason })?, true)
                }
                SessionObservation::Result {
                    epoch,
                    reason,
                    records,
                } => (
                    emitted.terminal_result(id, target, epoch, reason, records)?,
                    true,
                ),
            };
            Some((
                Ok(Frame::data(bytes)),
                (source, heartbeat, lease, terminal, emitted, settings),
            ))
        },
    );
    let mut response = Response::new(StreamBody::new(body).boxed_unsync());
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    response.headers_mut().insert(
        hyper::header::CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    response
}

struct ObservationLease {
    id: RequestId,
    cancellation: CancellationToken,
}

impl Drop for ObservationLease {
    fn drop(&mut self) {
        self.cancellation.cancel();
        eprintln!("gateway-observation-close request={:?}", self.id);
    }
}

fn gap_frame(cursor: Option<Cursor>, reason: GapReason) -> Option<Bytes> {
    encode_frame(
        "gap",
        &GapFrame {
            cursor: cursor.into(),
            reason,
        },
    )
}

fn encode_frame<T: Serialize>(event: &str, value: &T) -> Option<Bytes> {
    let string = serde_json::to_string(value).ok()?;
    if string.len() > SSE_DATA_BYTES {
        return None;
    }
    Some(Bytes::from(format!("event: {event}\ndata: {string}\n\n")))
}

#[cfg(test)]
#[path = "observation_response_test.rs"]
mod tests;
