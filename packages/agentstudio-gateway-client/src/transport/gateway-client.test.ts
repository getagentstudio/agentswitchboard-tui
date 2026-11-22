import { expect, test } from "bun:test";
import { Effect } from "#gateway/effect-runtime";
import { createGatewayClient, type GatewayTraceEvent } from "#gateway/transport/gateway-client";
import type { GatewayTarget } from "#gateway/transport/gateway-frames";

test("the consumed history and cut receipts share the request ID and omit records", async (): Promise<void> => {
  const target: GatewayTarget = {
    endpoint: { serviceId: "00000000-0000-4000-8000-000000000001", endpointId: "claude-local" },
    sessionId: "ses_fixture",
  };
  const bytes = [
    'event: item\ndata: {"phase":"history","cursor":{"epoch":"2","sequence":"9007199254740993"},"record":{"sequence":9007199254740993,"event":{"kind":"itemStarted","item":{"kind":{"kind":"notice"},"itemId":"msg_hidden","text":"peer-only-witness"}}}}\n\n',
    'event: snapshotEnd\ndata: {"cursor":{"epoch":"2","sequence":"9007199254740993"}}\n\n',
    'event: ended\ndata: {"reason":"completed"}\n\n',
  ].join("");
  const platformFetch = Object.assign(
    async (): Promise<Response> =>
      new Response(bytes, { headers: { "content-type": "text/event-stream" } }),
    globalThis.fetch,
  );
  const captured: GatewayTraceEvent[] = [];
  const client = createGatewayClient({
    socketPath: "/test/socket",
    platformFetch,
    trace: (event): void => {
      captured.push(event);
    },
  });
  const stream = await Effect.runPromise(client.observe(target, new AbortController().signal));
  const request = captured.at(0);
  if (request?.kind !== "gateway-request") throw new Error("Expected a Gateway request trace");

  expect(captured.map((entry) => entry.kind)).toEqual(["gateway-request", "gateway-response"]);
  expect((await stream.next()).value?.kind).toBe("history");
  expect(captured.at(-1)).toEqual({
    kind: "gateway-frame-consumed",
    requestId: request.requestId,
    frame: "history",
    epoch: "2",
    sequence: "9007199254740993",
  });
  expect((await stream.next()).value?.kind).toBe("snapshotEnd");
  expect(captured.at(-1)).toEqual({
    kind: "gateway-frame-consumed",
    requestId: request.requestId,
    frame: "snapshotEnd",
    epoch: "2",
    sequence: "9007199254740993",
  });
  expect(JSON.stringify(captured)).not.toContain("peer-only-witness");
  await stream.return(undefined);
});
