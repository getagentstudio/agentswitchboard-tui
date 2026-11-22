use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::stream;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::session::session_source::{ObservationStream, SessionSource, SessionSourceError};
use agentstudio_gateway_contract::{
    observation_records::{
        Cursor, EndReason, GapReason, OrderedRecord, ResultReason, SessionObservation,
        TranscriptEvent, TranscriptItem, TranscriptItemKind,
    },
    session_contract::{
        EndpointRef, FixtureCapabilities, HistoryAvailability, InspectArguments, InspectResult,
        ObserveArguments, ProviderState, SessionRef,
    },
};

pub const FIXTURE_SESSION_ID: &str = "ses_fixture";
pub const FIXTURE_SERVICE_ID: &str = "00000000-0000-4000-8000-000000000001";
pub const FIXTURE_ENDPOINT_ID: &str = "claude-local";

pub struct FixtureSessionSource {
    state: Mutex<FixtureState>,
    live: broadcast::Sender<FixtureUpdate>,
    next_subscription: AtomicU64,
}

struct FixtureState {
    cursor: Cursor,
    retained: Vec<OrderedRecord>,
}

#[derive(Clone)]
enum FixtureUpdate {
    Item {
        cursor: Cursor,
        record: OrderedRecord,
    },
    EpochReset,
}

impl Default for FixtureSessionSource {
    fn default() -> Self {
        let first_sequence = 9_007_199_254_740_991;
        let retained = vec![
            record(
                first_sequence,
                "msg_fixture_initial",
                TranscriptItemKind::UserMessage,
                "Fixture history before attachment",
            ),
            record(
                first_sequence + 1,
                "msg_fixture_history",
                TranscriptItemKind::Notice,
                "Fixture history second item",
            ),
        ];
        Self {
            state: Mutex::new(FixtureState {
                cursor: Cursor {
                    epoch: 1,
                    sequence: first_sequence + 1,
                },
                retained,
            }),
            live: broadcast::channel(32).0,
            next_subscription: AtomicU64::new(1),
        }
    }
}

impl FixtureSessionSource {
    pub fn append_notice(&self, text: String) -> Result<(), SessionSourceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SessionSourceError::Unavailable)?;
        state.cursor.sequence = state
            .cursor
            .sequence
            .checked_add(1)
            .ok_or(SessionSourceError::Unavailable)?;
        let cursor = state.cursor;
        let record = record(
            cursor.sequence,
            &format!("msg_fixture_{}_{}", cursor.epoch, cursor.sequence),
            TranscriptItemKind::Notice,
            &text,
        );
        state.retained.push(record.clone());
        let _ = self.live.send(FixtureUpdate::Item { cursor, record });
        eprintln!(
            "fixture-published epoch={} sequence={}",
            cursor.epoch, cursor.sequence
        );
        Ok(())
    }

    pub fn reset_epoch(&self) -> Result<(), SessionSourceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SessionSourceError::Unavailable)?;
        let epoch = state
            .cursor
            .epoch
            .checked_add(1)
            .ok_or(SessionSourceError::Unavailable)?;
        state.cursor = Cursor { epoch, sequence: 1 };
        state.retained = vec![record(
            1,
            &format!("msg_fixture_replacement_{epoch}"),
            TranscriptItemKind::UserMessage,
            "Fixture history from replacement snapshot",
        )];
        let _ = self.live.send(FixtureUpdate::EpochReset);
        Ok(())
    }
}

impl SessionSource for Arc<FixtureSessionSource> {
    async fn inspect(
        &self,
        request: InspectArguments,
    ) -> Result<InspectResult, SessionSourceError> {
        require_fixture_target(&request.target)?;
        eprintln!("fixture-inspect session={FIXTURE_SESSION_ID}");
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
        cancellation: CancellationToken,
    ) -> Result<ObservationStream, SessionSourceError> {
        require_fixture_target(&request.target)?;
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(request.timeout_seconds))
            .ok_or(SessionSourceError::Unavailable)?;
        let (cursor, retained, receiver) = {
            let state = self
                .state
                .lock()
                .map_err(|_| SessionSourceError::Unavailable)?;
            (state.cursor, state.retained.clone(), self.live.subscribe())
        };
        let id = self.next_subscription.fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "fixture-observe-open id={id} epoch={} sequence={}",
            cursor.epoch, cursor.sequence
        );
        let subscription = FixtureSubscription { id, cancellation };
        let stream = stream::unfold(
            (
                Some(subscription),
                receiver,
                retained.into_iter(),
                false,
                cursor,
            ),
            move |(subscription, mut receiver, mut retained, marker_sent, mut cursor)| async move {
                let subscription = subscription?;
                if subscription.cancellation.is_cancelled() {
                    return Some((
                        SessionObservation::Ended(EndReason::Cancelled),
                        (None, receiver, retained, marker_sent, cursor),
                    ));
                }
                if let Some(record) = retained.next() {
                    let position = Cursor {
                        sequence: record.sequence,
                        ..cursor
                    };
                    return Some((
                        SessionObservation::History {
                            cursor: position,
                            record,
                        },
                        (Some(subscription), receiver, retained, false, cursor),
                    ));
                }
                if !marker_sent {
                    return Some((
                        SessionObservation::SnapshotEnd(cursor),
                        (Some(subscription), receiver, retained, true, cursor),
                    ));
                }
                let update = tokio::select! {
                    () = subscription.cancellation.cancelled() => SessionObservation::Ended(EndReason::Cancelled),
                    () = tokio::time::sleep_until(deadline) => SessionObservation::Result { epoch: cursor.epoch, reason: ResultReason::DeadlineReached, records: Vec::new() },
                    received = receiver.recv() => match received {
                        Ok(FixtureUpdate::Item { cursor: next, record }) => {
                            cursor = next;
                            SessionObservation::Live { cursor: next, record }
                        }
                        Ok(FixtureUpdate::EpochReset) => SessionObservation::Gap { cursor, reason: GapReason::Epoch },
                        Err(broadcast::error::RecvError::Lagged(_)) => SessionObservation::Gap { cursor, reason: GapReason::Lag },
                        Err(broadcast::error::RecvError::Closed) => SessionObservation::Ended(EndReason::Closed),
                    }
                };
                let remaining =
                    matches!(update, SessionObservation::Live { .. }).then_some(subscription);
                Some((update, (remaining, receiver, retained, true, cursor)))
            },
        );
        Ok(Box::pin(stream))
    }
}

struct FixtureSubscription {
    id: u64,
    cancellation: CancellationToken,
}

impl Drop for FixtureSubscription {
    fn drop(&mut self) {
        eprintln!("fixture-observe-close id={}", self.id);
    }
}

pub fn fixture_target() -> SessionRef {
    SessionRef {
        endpoint: EndpointRef {
            service_id: FIXTURE_SERVICE_ID.into(),
            endpoint_id: FIXTURE_ENDPOINT_ID.into(),
        },
        session_id: FIXTURE_SESSION_ID.into(),
    }
}

fn require_fixture_target(target: &SessionRef) -> Result<(), SessionSourceError> {
    if *target == fixture_target() {
        Ok(())
    } else {
        Err(SessionSourceError::Denied)
    }
}

fn record(sequence: u64, item_id: &str, kind: TranscriptItemKind, text: &str) -> OrderedRecord {
    OrderedRecord {
        sequence,
        event: TranscriptEvent::ItemStarted {
            item: TranscriptItem {
                item_id: item_id.into(),
                kind,
                text: Some(text.into()),
            },
        },
    }
}

#[cfg(test)]
#[path = "fixture_session_source_test.rs"]
mod tests;
