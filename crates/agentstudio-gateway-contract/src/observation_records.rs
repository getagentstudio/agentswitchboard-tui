use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub epoch: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OrderedRecord {
    pub sequence: u64,
    pub event: TranscriptEvent,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, Eq, PartialEq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum TranscriptEvent {
    TurnStarted {
        turn_id: String,
        input_id: String,
    },
    TurnEnded {
        turn_id: String,
        outcome: TurnOutcome,
    },
    ItemStarted {
        item: TranscriptItem,
    },
    ItemUpdated {
        item: TranscriptItem,
    },
    ItemCompleted {
        item_id: String,
    },
    ResyncRequired {
        replay_epoch: u64,
    },
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum TurnOutcome {
    Ended {
        stop_reason: StopReason,
        local_cause: Option<LocalCause>,
    },
    Lost {
        reason: TurnLostReason,
    },
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Cancelled,
    Unknown(String),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum LocalCause {
    OutputOverflow,
    Other(String),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TurnLostReason {
    ProviderRetired,
    ProviderTurnFailed,
    EndNotObservable,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TranscriptItem {
    pub item_id: String,
    pub kind: TranscriptItemKind,
    pub text: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum TranscriptItemKind {
    UserMessage,
    AgentMessage,
    Notice,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum GapReason {
    Lag,
    Epoch,
    EventTooLarge,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EndReason {
    Completed,
    Cancelled,
    Closed,
}

#[derive(Debug)]
pub enum SessionObservation {
    History {
        cursor: Cursor,
        record: OrderedRecord,
    },
    SnapshotEnd(Cursor),
    Live {
        cursor: Cursor,
        record: OrderedRecord,
    },
    Gap {
        cursor: Cursor,
        reason: GapReason,
    },
    Ended(EndReason),
    Result {
        epoch: u64,
        reason: ResultReason,
        records: Vec<OrderedRecord>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ResultReason {
    DeadlineReached,
    ResultLimitReached,
}
