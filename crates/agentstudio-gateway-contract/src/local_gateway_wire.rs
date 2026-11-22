use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::observation_records::{Cursor, EndReason, GapReason, OrderedRecord};

pub const VERSION_HEADER: &str = "agentstudio-wire-version";
pub const WIRE_VERSION: &str = "1";
pub const REQUEST_BYTES: usize = 256 * 1024;
pub const RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const SSE_DATA_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RequestId {
    Text(String),
    Integer(i64),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRequest<T> {
    pub jsonrpc: String,
    pub id: RequestId,
    pub method: String,
    pub params: ToolParameters<T>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolParameters<T> {
    pub name: String,
    pub arguments: T,
}

#[derive(Clone, Copy, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EnvelopeCode {
    IncompatibleVersion,
    InvalidFrame,
    ScopeDenied,
    FrameTooLarge,
    Unavailable,
}

#[derive(JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvelopeRefusal {
    pub envelope_error: EnvelopeError,
}

#[derive(JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvelopeError {
    pub code: EnvelopeCode,
    pub message: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported_versions: Option<[&'static str; 1]>,
}

#[derive(Serialize)]
pub struct ToolResponse<T> {
    pub jsonrpc: &'static str,
    pub id: RequestId,
    pub result: ToolResult<T>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolResult<T> {
    pub content: [TextContent; 1],
    pub structured_content: T,
    pub is_error: bool,
}

#[derive(Serialize)]
pub struct TextContent {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub text: String,
}

#[derive(JsonSchema, Serialize)]
pub struct WireCursor {
    pub epoch: String,
    pub sequence: String,
}

impl From<Cursor> for WireCursor {
    fn from(cursor: Cursor) -> Self {
        Self {
            epoch: cursor.epoch.to_string(),
            sequence: cursor.sequence.to_string(),
        }
    }
}

#[derive(JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ItemPhase {
    History,
    Live,
}

#[derive(JsonSchema, Serialize)]
pub struct ItemFrame {
    pub phase: ItemPhase,
    pub cursor: WireCursor,
    pub record: OrderedRecord,
}

#[derive(JsonSchema, Serialize)]
pub struct SnapshotEndFrame {
    pub cursor: WireCursor,
}

#[derive(JsonSchema, Serialize)]
pub struct GapFrame {
    pub cursor: GapPosition,
    pub reason: GapReason,
}

#[derive(JsonSchema, Serialize)]
#[serde(untagged)]
pub enum GapPosition {
    Emitted(WireCursor),
    Unpositioned(()),
}

impl From<Option<Cursor>> for GapPosition {
    fn from(cursor: Option<Cursor>) -> Self {
        match cursor {
            Some(cursor) => Self::Emitted(cursor.into()),
            None => Self::Unpositioned(()),
        }
    }
}

#[derive(JsonSchema, Serialize)]
pub struct EndedFrame {
    pub reason: EndReason,
}

#[derive(JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatFrame {
    pub server_ts_ms: u64,
}
