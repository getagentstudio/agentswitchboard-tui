import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { Schema } from "#gateway/effect-runtime";
import { decodeDecimalCursor, parseWireJson } from "#gateway/contract/exact-u64";
import { OrderedRecord } from "#gateway/contract/generated-contract";
import {
  receiveGatewayFrames,
  type GatewayFrame,
  type GatewayTarget,
} from "#gateway/transport/gateway-frames";

const target: GatewayTarget = {
  endpoint: { serviceId: "00000000-0000-4000-8000-000000000001", endpointId: "claude-local" },
  sessionId: "ses_fixture",
};
const requestId = "contract-test";

function record(sequence: bigint, text = "text"): string {
  return `{"sequence":${sequence},"event":{"kind":"itemStarted","item":{"itemId":"msg_${sequence}","kind":{"kind":"notice"},"text":${JSON.stringify(text)}}}}`;
}

function frame(event: string, data: string): string {
  return `event: ${event}\ndata: ${data}\n\n`;
}

function cursor(sequence: bigint, epoch = 1n): string {
  return JSON.stringify({ epoch: epoch.toString(), sequence: sequence.toString() });
}

function item(phase: "history" | "live", sequence: bigint, text?: string): string {
  return frame(
    "item",
    `{"phase":"${phase}","cursor":${cursor(sequence)},"record":${record(sequence, text)}}`,
  );
}

function result(...events: readonly string[]): string {
  const body = `{"target":${JSON.stringify(target)},"generation":{"serviceEpoch":"00000000-0000-4000-8000-000000000002","generation":1},"attached":true,"events":[${events.join(",")}],"endReason":"deadlineReached","continuationGap":true,"epoch":1}`;
  const content = [{ type: "text", text: body }];
  return frame(
    "result",
    `{"jsonrpc":"2.0","id":"${requestId}","result":{"content":${JSON.stringify(content)},"isError":false,"structuredContent":${body}}}`,
  );
}

function frames(...chunks: readonly string[]): AsyncGenerator<GatewayFrame> {
  const encoder = new TextEncoder();
  return receiveGatewayFrames(
    new ReadableStream({
      start(controller): void {
        for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
        controller.close();
      },
    }),
    requestId,
    target,
  );
}

test("reads original Router Rust serde witnesses without JS integer rounding", (): void => {
  const raw = readFileSync(
    new URL(
      "../../../../crates/agentstudio-gateway-contract/contract/router-event-witnesses.json",
      import.meta.url,
    ),
    "utf8",
  );
  const witnesses = Schema.decodeUnknownSync(Schema.Array(OrderedRecord), {
    onExcessProperty: "error",
  })(parseWireJson(raw));
  expect(witnesses.map((entry) => entry.event.kind)).toEqual([
    "itemStarted",
    "turnStarted",
    "itemStarted",
    "itemUpdated",
    "itemCompleted",
    "turnEnded",
    "itemStarted",
    "resyncRequired",
  ]);
  expect(witnesses.slice(0, 3).map((entry) => entry.sequence)).toEqual([
    9_007_199_254_740_991n,
    9_007_199_254_740_992n,
    9_007_199_254_740_993n,
  ]);
  expect(witnesses.at(-1)).toEqual({
    sequence: 18_446_744_073_709_551_615n,
    event: { kind: "resyncRequired", replayEpoch: 18_446_744_073_709_551_615n },
  });
  for (const invalid of ["-1", "1.0", "1e2", "18446744073709551616"]) {
    expect(() => parseWireJson(`{"sequence":${invalid}}`)).toThrow();
    expect(() => decodeDecimalCursor(invalid)).toThrow();
  }
});

test("only the explicit marker completes history and a result-only suffix is published once", async (): Promise<void> => {
  const beforeCut = await Array.fromAsync(frames(item("history", 99n), result(record(99n))));
  const afterCut = await Array.fromAsync(
    frames(
      item("history", 99n),
      frame("snapshotEnd", `{"cursor":${cursor(99n)}}`),
      item("live", 100n),
      result(record(99n), record(100n), record(101n), record(102n)),
    ),
  );

  expect(beforeCut.map((entry) => entry.kind)).toEqual(["history", "ended"]);
  expect(afterCut.map((entry) => entry.kind)).toEqual([
    "history",
    "snapshotEnd",
    "live",
    "resultSuffix",
    "ended",
  ]);
  expect(afterCut.at(3)).toMatchObject({ records: [{ sequence: 101n }, { sequence: 102n }] });
});

test("rejects an entire terminal suffix if its replay diverges, skips a cursor or contains malformed data", async (): Promise<void> => {
  for (const [final, reason] of [
    [result(record(99n, "different")), "Divergent terminal replay"],
    [result(record(100n), record(102n)), "missing a record"],
    [
      result(record(100n), '{"sequence":101,"event":{"kind":"unknown"}}'),
      "Malformed Gateway terminal record",
    ],
  ] as const) {
    const seen: GatewayFrame[] = [];
    const read = async (): Promise<void> => {
      for await (const observed of frames(
        item("history", 99n),
        frame("snapshotEnd", `{"cursor":${cursor(99n)}}`),
        final,
      )) {
        seen.push(observed);
      }
    };
    await expect(read()).rejects.toThrow(reason);
    expect(seen.map((entry) => entry.kind)).toEqual(["history", "snapshotEnd"]);
  }
});

test("rejects early EOF, byte overflow and out-of-phase items, and accepts one explicit gap", async (): Promise<void> => {
  await expect(Array.fromAsync(frames(item("history", 99n)))).rejects.toThrow("ConnectionLost");
  await expect(Array.fromAsync(frames(item("live", 100n)))).rejects.toThrow("phase");
  await expect(Array.fromAsync(frames(frame("heartbeat", "é".repeat(524_289))))).rejects.toThrow(
    "too large",
  );
  const gap = frame("gap", `{"cursor":${cursor(99n)},"reason":"lag"}`);
  await expect(Array.fromAsync(frames(gap, gap))).rejects.toThrow("after its terminal frame");
  expect(await Array.fromAsync(frames(gap))).toEqual([
    { kind: "gap", cursor: { epoch: 1n, sequence: 99n }, reason: "lag" },
  ]);
});

test("accepts a required null cursor only for an oversized gap before the first position", async (): Promise<void> => {
  const unpositioned = frame("gap", '{"cursor":null,"reason":"eventTooLarge"}');
  expect(await Array.fromAsync(frames(unpositioned))).toEqual([
    { kind: "gap", cursor: null, reason: "eventTooLarge" },
  ]);
  await expect(Array.fromAsync(frames(frame("gap", '{"reason":"eventTooLarge"}')))).rejects.toThrow(
    "cursor",
  );
  await expect(
    Array.fromAsync(frames(frame("gap", '{"cursor":null,"reason":"lag"}'))),
  ).rejects.toThrow("invalid reason");
  await expect(Array.fromAsync(frames(item("history", 99n), unpositioned))).rejects.toThrow(
    "established position",
  );
  await expect(
    Array.fromAsync(frames(frame("snapshotEnd", `{"cursor":${cursor(99n)}}`), unpositioned)),
  ).rejects.toThrow("established position");
  expect(
    await Array.fromAsync(
      frames(
        item("history", 99n),
        frame("gap", `{"cursor":${cursor(99n)},"reason":"eventTooLarge"}`),
      ),
    ),
  ).toMatchObject([
    { kind: "history" },
    { kind: "gap", cursor: { epoch: 1n, sequence: 99n }, reason: "eventTooLarge" },
  ]);
});
