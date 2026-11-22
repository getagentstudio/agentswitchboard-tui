use std::fmt;

use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor, value::MapAccessDeserializer},
};

/// Decode an object with exactly one string-valued discriminator.
pub(crate) fn deserialize_closed_kind<'de, Input>(input: Input) -> Result<String, Input::Error>
where
    Input: Deserializer<'de>,
{
    input.deserialize_map(ClosedKindVisitor)
}

struct ClosedKindVisitor;

impl<'de> Visitor<'de> for ClosedKindVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object with a single kind field")
    }

    fn visit_map<Fields>(self, fields: Fields) -> Result<Self::Value, Fields::Error>
    where
        Fields: MapAccess<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct KindObject {
            kind: String,
        }

        KindObject::deserialize(MapAccessDeserializer::new(fields)).map(|object| object.kind)
    }
}
