use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};

use crate::closed_kind::deserialize_closed_kind;

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionRef {
    pub endpoint: EndpointRef,
    pub session_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EndpointRef {
    pub service_id: String,
    pub endpoint_id: String,
}

impl SessionRef {
    pub fn is_valid(&self) -> bool {
        let service_id = self.endpoint.service_id.as_bytes();
        let endpoint_id = self.endpoint.endpoint_id.as_bytes();
        service_id.len() == 36
            && service_id
                .iter()
                .enumerate()
                .all(|(index, byte)| match index {
                    8 | 13 | 18 | 23 => *byte == b'-',
                    _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(byte),
                })
            && (1..=64).contains(&endpoint_id.len())
            && endpoint_id.first().is_some_and(u8::is_ascii_lowercase)
            && endpoint_id
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
            && (1..=4096).contains(&self.session_id.len())
            && !self.session_id.contains('\0')
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InspectArguments {
    pub target: SessionRef,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObserveArguments {
    pub target: SessionRef,
    pub timeout_seconds: u64,
    pub max_events: usize,
    pub max_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InspectResult {
    pub target: SessionRef,
    pub state: ProviderState,
    pub capabilities: FixtureCapabilities,
    pub history: HistoryAvailability,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ProviderState {
    Idle,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum HistoryAvailability {
    Available,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FixtureCapabilities {
    pub load: bool,
    pub resume: bool,
    pub close: bool,
    pub list: bool,
    pub steer: bool,
    pub modes: bool,
    pub config_options: bool,
    pub elicitation: bool,
    pub usage: bool,
    pub prompt_content: PromptCapabilities,
    pub auth_status: FixtureAuthStatus,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PromptCapabilities {
    pub image: bool,
    pub audio: bool,
    pub embedded_context: bool,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum FixtureAuthStatus {
    #[default]
    NotReported,
}

impl<'de> Deserialize<'de> for FixtureAuthStatus {
    fn deserialize<Input>(input: Input) -> Result<Self, Input::Error>
    where
        Input: Deserializer<'de>,
    {
        match deserialize_closed_kind(input)?.as_str() {
            "notReported" => Ok(Self::NotReported),
            unknown => Err(de::Error::unknown_variant(unknown, &["notReported"])),
        }
    }
}
