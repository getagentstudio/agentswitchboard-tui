use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use agentstudio_gateway::session::session_source::{
    ObservationStream, SessionSource, SessionSourceError,
};
use agentstudio_gateway_contract::{
    observation_records::{
        Cursor, EndReason, GapReason, OrderedRecord, SessionObservation, TranscriptEvent,
        TranscriptItem, TranscriptItemKind,
    },
    session_contract::{
        FixtureCapabilities, HistoryAvailability, InspectArguments, InspectResult,
        ObserveArguments, ProviderState, SessionRef,
    },
};
use futures_util::stream;
use tokio::{
    sync::{broadcast, watch},
    time::{self, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WitnessHold {
    BeforeFirst,
    BeforeCut,
    AfterCut,
    Released,
}

#[derive(Clone)]
pub struct WitnessFixture(Arc<FixtureState>);

struct FixtureState {
    target: SessionRef,
    history: Mutex<Vec<OrderedRecord>>,
    live: broadcast::Sender<OrderedRecord>,
    hold: watch::Sender<WitnessHold>,
    next_lease: AtomicU64,
}

impl WitnessFixture {
    pub fn new(target: SessionRef, initial: [String; 2], hold: WitnessHold) -> Self {
        let [first, second] = initial;
        Self(Arc::new(FixtureState {
            target,
            history: Mutex::new(vec![
                witness(9_007_199_254_740_992, first),
                witness(9_007_199_254_740_993, second),
            ]),
            live: broadcast::channel(32).0,
            hold: watch::channel(hold).0,
            next_lease: AtomicU64::new(1),
        }))
    }

    pub fn release_first(&self) -> bool {
        self.transition_hold(WitnessHold::BeforeFirst, WitnessHold::BeforeCut)
    }

    pub fn release_cut(&self) -> bool {
        self.transition_hold(WitnessHold::BeforeCut, WitnessHold::AfterCut)
    }

    pub fn release_live(&self) -> bool {
        self.transition_hold(WitnessHold::AfterCut, WitnessHold::Released)
    }

    fn transition_hold(&self, expected: WitnessHold, next: WitnessHold) -> bool {
        self.0.hold.send_if_modified(|state| {
            if *state == expected {
                *state = next;
                true
            } else {
                false
            }
        })
    }

    pub fn publish(&self, text: String) -> Result<u64, SessionSourceError> {
        let mut history = self
            .0
            .history
            .lock()
            .map_err(|_| SessionSourceError::Unavailable)?;
        let sequence = history
            .last()
            .and_then(|record| record.sequence.checked_add(1))
            .ok_or(SessionSourceError::Unavailable)?;
        let record = witness(sequence, text);
        history.push(record.clone());
        let _ = self.0.live.send(record);
        eprintln!("quinn-peer-fixture-published epoch=1 sequence={sequence}");
        Ok(sequence)
    }

    fn scope(&self, target: &SessionRef) -> Result<(), SessionSourceError> {
        if target == &self.0.target {
            Ok(())
        } else {
            Err(SessionSourceError::Denied)
        }
    }
}

impl SessionSource for WitnessFixture {
    async fn inspect(
        &self,
        request: InspectArguments,
    ) -> Result<InspectResult, SessionSourceError> {
        self.scope(&request.target)?;
        eprintln!(
            "quinn-peer-source-inspect session={}",
            request.target.session_id
        );
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
        cancel: CancellationToken,
    ) -> Result<ObservationStream, SessionSourceError> {
        self.scope(&request.target)?;
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(request.timeout_seconds))
            .ok_or(SessionSourceError::Unavailable)?;
        let (history, updates) = {
            let retained = self
                .0
                .history
                .lock()
                .map_err(|_| SessionSourceError::Unavailable)?;
            (retained.clone(), self.0.live.subscribe())
        };
        let position = history.last().map_or(0, |record| record.sequence);
        let id = self.0.next_lease.fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "quinn-peer-source-open lease={id} epoch=1 sequence={position} timeout={} max_events={} max_bytes={}",
            request.timeout_seconds, request.max_events, request.max_bytes
        );
        let state = WitnessObservation {
            lease: Lease(id),
            deadline,
            cancel,
            history: history.into_iter(),
            updates,
            hold: self.0.hold.subscribe(),
            position,
            cut_sent: false,
        };
        Ok(Box::pin(stream::unfold(Some(state), |state| async move {
            let mut state = state?;
            let frame = state.next().await?;
            let keep = matches!(
                frame,
                SessionObservation::History { .. }
                    | SessionObservation::SnapshotEnd(_)
                    | SessionObservation::Live { .. }
            );
            Some((frame, keep.then_some(state)))
        })))
    }
}

struct WitnessObservation {
    lease: Lease,
    deadline: Instant,
    cancel: CancellationToken,
    history: std::vec::IntoIter<OrderedRecord>,
    updates: broadcast::Receiver<OrderedRecord>,
    hold: watch::Receiver<WitnessHold>,
    position: u64,
    cut_sent: bool,
}

impl WitnessObservation {
    async fn next(&mut self) -> Option<SessionObservation> {
        if !self
            .wait_for_hold(|hold| hold != WitnessHold::BeforeFirst)
            .await
        {
            return None;
        }
        if let Some(record) = self.history.next() {
            return Some(SessionObservation::History {
                cursor: Cursor {
                    epoch: 1,
                    sequence: record.sequence,
                },
                record,
            });
        }
        if !self.cut_sent {
            eprintln!("quinn-peer-cut-wait lease={}", self.lease.0);
            if !self
                .wait_for_hold(|hold| matches!(hold, WitnessHold::AfterCut | WitnessHold::Released))
                .await
            {
                return None;
            }
            self.cut_sent = true;
            return Some(SessionObservation::SnapshotEnd(Cursor {
                epoch: 1,
                sequence: self.position,
            }));
        }
        eprintln!("quinn-peer-live-wait lease={}", self.lease.0);
        if !self
            .wait_for_hold(|hold| hold == WitnessHold::Released)
            .await
        {
            return None;
        }

        tokio::select! {
            () = self.cancel.cancelled() => None,
            () = time::sleep_until(self.deadline) => Some(SessionObservation::Ended(EndReason::Completed)),
            result = self.updates.recv() => Some(match result {
                Ok(record) => {
                    self.position = record.sequence;
                    SessionObservation::Live {
                        cursor: Cursor { epoch: 1, sequence: self.position },
                        record,
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => SessionObservation::Gap {
                    cursor: Cursor { epoch: 1, sequence: self.position },
                    reason: GapReason::Lag,
                },
                Err(broadcast::error::RecvError::Closed) => SessionObservation::Ended(EndReason::Closed),
            }),
        }
    }

    async fn wait_for_hold(&mut self, released: impl Fn(WitnessHold) -> bool) -> bool {
        tokio::select! {
            () = self.cancel.cancelled() => false,
            result = self.hold.wait_for(|hold|released(*hold)) => result.is_ok(),
        }
    }
}

struct Lease(u64);

impl Drop for Lease {
    fn drop(&mut self) {
        eprintln!("quinn-peer-source-close lease={}", self.0);
    }
}

fn witness(sequence: u64, text: String) -> OrderedRecord {
    OrderedRecord {
        sequence,
        event: TranscriptEvent::ItemStarted {
            item: TranscriptItem {
                item_id: format!("msg_peer_{sequence}"),
                kind: TranscriptItemKind::Notice,
                text: Some(text),
            },
        },
    }
}
