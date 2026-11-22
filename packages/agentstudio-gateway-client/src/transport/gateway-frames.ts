import { isDeepStrictEqual } from "node:util";
import { Schema } from "#gateway/effect-runtime";
import {
  BoundedObservation,
  EndedFrame,
  GapFrame,
  HeartbeatFrame,
  ItemFrame,
  OrderedRecord,
  SnapshotEndFrame,
} from "#gateway/contract/generated-contract";
import { decodeDecimalCursor, parseWireJson } from "#gateway/contract/exact-u64";

export interface GatewayCursor {
  readonly epoch: bigint;
  readonly sequence: bigint;
}

export type GatewayRecord = typeof OrderedRecord.Type;
export type GatewayTarget = (typeof BoundedObservation.Type)["target"];

export type GatewayFrame =
  | {
      readonly kind: "history" | "live";
      readonly cursor: GatewayCursor;
      readonly record: GatewayRecord;
    }
  | {
      readonly kind: "resultSuffix";
      readonly epoch: bigint;
      readonly records: readonly GatewayRecord[];
    }
  | { readonly kind: "snapshotEnd"; readonly cursor: GatewayCursor }
  | {
      readonly kind: "gap";
      readonly cursor: GatewayCursor | null;
      readonly reason: (typeof GapFrame.Type)["reason"];
    }
  | { readonly kind: "ended"; readonly reason: (typeof EndedFrame.Type)["reason"] };

const strict = { onExcessProperty: "error" } as const;
const resultEnvelope = Schema.Struct({
  jsonrpc: Schema.Literal("2.0"),
  id: Schema.String,
  result: Schema.Struct({
    isError: Schema.Boolean,
    content: Schema.Tuple([Schema.Struct({ type: Schema.Literal("text"), text: Schema.String })]),
    structuredContent: Schema.Unknown,
  }),
});

export function decodeToolResult<ResultSchema extends Schema.ConstraintDecoder<unknown>>(
  wire: unknown,
  requestId: string,
  resultSchema: ResultSchema,
): ResultSchema["Type"] {
  const response = Schema.decodeUnknownSync(resultEnvelope, strict)(wire);
  if (response.id !== requestId) throw new Error("Gateway result has a different request ID");
  if (
    !isDeepStrictEqual(
      parseWireJson(response.result.content[0].text),
      response.result.structuredContent,
    )
  ) {
    throw new Error("Gateway tool result contains divergent content");
  }
  if (response.result.isError) {
    const error = Schema.decodeUnknownSync(
      Schema.Struct({
        mcpResult: Schema.Literal("error"),
        kind: Schema.String,
        message: Schema.String,
      }),
    )(response.result.structuredContent);
    throw new Error(`Gateway tool failed (${error.kind}): ${error.message}`);
  }
  return Schema.decodeUnknownSync(resultSchema, strict)(response.result.structuredContent);
}

function decodeCursor(value: { readonly epoch: string; readonly sequence: string }): GatewayCursor {
  return { epoch: decodeDecimalCursor(value.epoch), sequence: decodeDecimalCursor(value.sequence) };
}

export async function* receiveGatewayFrames(
  body: ReadableStream<Uint8Array>,
  requestId: string,
  target: GatewayTarget,
): AsyncGenerator<GatewayFrame> {
  let phase: "history" | "live" = "history";
  let position: GatewayCursor | undefined;
  let terminal: Extract<GatewayFrame, { kind: "ended" | "gap" }> | undefined;
  const resultSuffix: GatewayRecord[] = [];
  const accepted = new Map<bigint, GatewayRecord>();

  for await (const frame of readFrames(body)) {
    if (terminal) throw new Error("Gateway sent data after its terminal frame");
    const value = parseWireJson(frame.data);
    switch (frame.event) {
      case "item": {
        const item = Schema.decodeUnknownSync(ItemFrame, strict)(value);
        const cursor = decodeCursor(item.cursor);
        if (item.phase !== phase || cursor.sequence !== item.record.sequence) {
          throw new Error("Gateway item does not match its phase or cursor");
        }
        const previous = accepted.get(cursor.sequence);
        if (previous !== undefined && cursor.epoch === position?.epoch) {
          if (!isDeepStrictEqual(previous, item.record))
            throw new Error("Divergent replay from Gateway");
          break;
        }
        if (
          position &&
          (position.epoch !== cursor.epoch || position.sequence + 1n !== cursor.sequence)
        ) {
          throw new Error("Gateway item broke cursor continuity");
        }
        if (item.record.event.kind === "resyncRequired")
          throw new Error("Expected an explicit Gateway gap");
        position = cursor;
        accepted.set(cursor.sequence, item.record);
        yield { kind: phase, cursor, record: item.record };
        break;
      }
      case "snapshotEnd": {
        const cursor = decodeCursor(
          Schema.decodeUnknownSync(SnapshotEndFrame, strict)(value).cursor,
        );
        if (
          phase !== "history" ||
          (position && (cursor.epoch !== position.epoch || cursor.sequence !== position.sequence))
        ) {
          throw new Error("Gateway snapshotEnd does not match the captured history");
        }
        phase = "live";
        position = cursor;
        yield { kind: "snapshotEnd", cursor };
        break;
      }
      case "heartbeat":
        Schema.decodeUnknownSync(HeartbeatFrame, strict)(value);
        break;
      case "gap": {
        const gap = Schema.decodeUnknownSync(GapFrame, strict)(value);
        if (gap.cursor === null) {
          if (phase !== "history" || position !== undefined || gap.reason !== "eventTooLarge") {
            throw new Error(
              "Gateway gap omitted an established position or used an invalid reason",
            );
          }
          terminal = { kind: "gap", cursor: null, reason: gap.reason };
          break;
        }
        const cursor = decodeCursor(gap.cursor);
        if (
          gap.reason === "eventTooLarge" &&
          (position?.epoch !== cursor.epoch || position.sequence !== cursor.sequence)
        ) {
          throw new Error("Gateway oversized gap did not carry its last emitted position");
        }
        terminal = { kind: "gap", cursor, reason: gap.reason };
        break;
      }
      case "ended": {
        const ended = Schema.decodeUnknownSync(EndedFrame, strict)(value);
        terminal = { kind: "ended", reason: ended.reason };
        break;
      }
      case "result": {
        const result = decodeToolResult(value, requestId, BoundedObservation);
        if (
          !isDeepStrictEqual(result.target, target) ||
          !result.attached ||
          typeof result.epoch !== "bigint"
        ) {
          throw new Error("Gateway terminal result does not match the attachment");
        }
        if (result.endReason !== "deadlineReached" && result.endReason !== "resultLimitReached") {
          throw new Error("Gateway terminal result requires an explicit ended or gap frame");
        }
        let lastResultRecord: GatewayRecord | undefined;
        let suffixPosition = position?.sequence;
        for (const input of result.events) {
          let record: GatewayRecord;
          try {
            record = Schema.decodeUnknownSync(OrderedRecord, strict)(input);
          } catch (cause) {
            throw new Error("Malformed Gateway terminal record", { cause });
          }
          if (lastResultRecord && record.sequence <= lastResultRecord.sequence) {
            if (isDeepStrictEqual(lastResultRecord, record)) continue;
            throw new Error("Gateway terminal records are unordered or divergent");
          }
          lastResultRecord = record;
          if (record.event.kind === "resyncRequired")
            throw new Error("Expected an explicit Gateway gap");
          if (phase === "history") continue;
          const previous = accepted.get(record.sequence);
          if (previous !== undefined) {
            if (!isDeepStrictEqual(previous, record))
              throw new Error("Divergent terminal replay from Gateway");
            continue;
          }
          if (suffixPosition === undefined || record.sequence !== suffixPosition + 1n) {
            throw new Error("Gateway terminal suffix is missing a record");
          }
          suffixPosition = record.sequence;
          resultSuffix.push(record);
        }
        if (phase === "live" && result.epoch !== position?.epoch)
          throw new Error("Gateway result changed epoch");
        terminal = { kind: "ended", reason: "completed" };
        break;
      }
      default:
        throw new Error("Unknown Gateway SSE event");
    }
  }
  if (!terminal)
    throw new Error("ConnectionLost: Gateway stream reached EOF without a terminal frame");
  if (position && phase === "live" && resultSuffix.length > 0) {
    yield { kind: "resultSuffix", epoch: position.epoch, records: resultSuffix };
  }
  yield terminal;
}

const maximumDataBytes = 1024 * 1024;
const dataPrefixBytes = "data: ".length;

interface RawFrame {
  readonly event: string;
  readonly data: string;
}

async function* readFrames(body: ReadableStream<Uint8Array>): AsyncGenerator<RawFrame> {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const line = new Uint8Array(maximumDataBytes + dataPrefixBytes + 1);
  let occupied = 0;
  let event: string | undefined;
  let data: string | undefined;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      let offset = 0;
      while (offset < next.value.length) {
        const end = next.value.indexOf(10, offset);
        const limit = end === -1 ? next.value.length : end;
        const segment = next.value.subarray(offset, limit);
        if (occupied + segment.length > line.length) throw new Error("Gateway SSE frame too large");
        line.set(segment, occupied);
        occupied += segment.length;
        offset = end === -1 ? limit : limit + 1;
        if (end === -1) continue;
        const lineLength = line[occupied - 1] === 13 ? occupied - 1 : occupied;
        if (lineLength > maximumDataBytes + dataPrefixBytes)
          throw new Error("Gateway SSE frame too large");
        const text = decoder.decode(line.subarray(0, lineLength));
        occupied = 0;
        if (
          text.startsWith("event: ") &&
          event === undefined &&
          data === undefined &&
          lineLength <= 64
        ) {
          event = text.slice("event: ".length);
        } else if (text.startsWith("data: ") && event !== undefined && data === undefined) {
          if (lineLength - dataPrefixBytes > maximumDataBytes)
            throw new Error("Gateway SSE frame too large");
          data = text.slice(dataPrefixBytes);
        } else if (text === "" && event !== undefined && data !== undefined) {
          yield { event, data };
          event = undefined;
          data = undefined;
        } else {
          throw new Error("Malformed Gateway SSE frame");
        }
      }
    }
    if (occupied !== 0 || event !== undefined)
      throw new Error("ConnectionLost: incomplete Gateway SSE frame");
  } finally {
    try {
      await reader.cancel();
    } finally {
      reader.releaseLock();
    }
  }
}
