use std::io;

use agentstudio_gateway_contract::local_gateway_wire::{
    EndedFrame, EnvelopeRefusal, GapFrame, HeartbeatFrame, ItemFrame, SnapshotEndFrame,
};
use schemars::{JsonSchema, schema_for};

#[derive(JsonSchema)]
#[allow(dead_code)]
struct LocalFrameSchemas {
    envelope_refusal: EnvelopeRefusal,
    item: ItemFrame,
    snapshot_end: SnapshotEndFrame,
    heartbeat: HeartbeatFrame,
    gap: GapFrame,
    ended: EndedFrame,
}

fn main() -> Result<(), serde_json::Error> {
    serde_json::to_writer_pretty(io::stdout().lock(), &schema_for!(LocalFrameSchemas))
}
