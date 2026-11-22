import { expect, test } from "bun:test";
import { Effect } from "#gateway/effect-runtime";
import type { GatewayClient } from "#gateway/transport/gateway-client";
import type { GatewayFrame, GatewayTarget } from "#gateway/transport/gateway-frames";
import type { InspectSession } from "#gateway/contract/generated-contract";
import { createGatewayAttachment } from "#app/session/gateway-attachment";
import { SessionProjection } from "#app/session/session-projection";

const target: GatewayTarget = {
  endpoint: { serviceId: "00000000-0000-4000-8000-000000000001", endpointId: "claude-local" },
  sessionId: "ses_fixture",
};

const inspection: typeof InspectSession.Type = {
  target,
  history: "available",
  state: "idle",
  capabilities: {
    load: false,
    resume: false,
    close: false,
    list: false,
    steer: false,
    modes: false,
    configOptions: false,
    elicitation: false,
    usage: false,
    authStatus: { kind: "notReported" },
    promptContent: { image: false, audio: false, embeddedContext: false },
  },
};

function createControlledClient(...observations: readonly GatewayFrame[]): GatewayClient {
  async function* feed(): AsyncGenerator<GatewayFrame> {
    yield* observations;
  }
  return { inspect: () => Effect.succeed(inspection), observe: () => Effect.succeed(feed()) };
}

const cursor = (sequence: bigint): { readonly epoch: bigint; readonly sequence: bigint } => ({
  epoch: 1n,
  sequence,
});

test("does not make pre-cut history visible through the private attachment", async (): Promise<void> => {
  const client = createControlledClient(
    {
      kind: "history",
      cursor: cursor(1n),
      record: {
        sequence: 1n,
        event: {
          kind: "itemStarted",
          item: { itemId: "msg_1", kind: { kind: "notice" }, text: "Partial" },
        },
      },
    },
    { kind: "ended", reason: "completed" },
  );
  const admission = await createGatewayAttachment(client).open(target.sessionId);
  if (admission.kind !== "attached") throw new Error("Expected an attachment");
  const reader = admission.attachment.observations.getReader();

  expect(await reader.read()).toMatchObject({ value: { kind: "ended" } });
  expect((await reader.read()).done).toBe(true);
});

test("advances across non-transcript records and applies assistant edits under one stable row", async (): Promise<void> => {
  const client = createControlledClient(
    { kind: "snapshotEnd", cursor: cursor(1n) },
    {
      kind: "live",
      cursor: cursor(2n),
      record: {
        sequence: 2n,
        event: { kind: "turnStarted", turnId: "turn-1", inputId: "input-1" },
      },
    },
    {
      kind: "resultSuffix",
      epoch: 1n,
      records: [
        {
          sequence: 3n,
          event: {
            kind: "itemStarted",
            item: { itemId: "msg_1", kind: { kind: "agentMessage" }, text: "Starting" },
          },
        },
        {
          sequence: 4n,
          event: {
            kind: "itemUpdated",
            item: { itemId: "msg_1", kind: { kind: "agentMessage" }, text: "Finished" },
          },
        },
        { sequence: 5n, event: { kind: "itemCompleted", itemId: "msg_1" } },
      ],
    },
    { kind: "ended", reason: "completed" },
  );
  const admission = await createGatewayAttachment(client).open(target.sessionId);
  if (admission.kind !== "attached") throw new Error("Expected an attachment");
  const reader = admission.attachment.observations.getReader();
  const first = (await reader.read()).value;
  if (first?.kind !== "snapshot") throw new Error("Expected the explicit snapshot");
  const projection = new SessionProjection(first.snapshot);

  for (let index = 0; index < 4; index += 1) {
    const update = (await reader.read()).value;
    if (update?.kind !== "event") throw new Error("Expected live update");
    expect(projection.apply(update.event).kind).toBe("accepted");
  }

  expect(projection.history()).toMatchObject([
    { id: "msg_1", kind: "assistant", text: "Finished", completed: true },
  ]);
  expect((await reader.read()).value).toEqual({ kind: "ended", reason: "completed" });
});

test("does not expose the good prefix of a semantically divergent result suffix", async (): Promise<void> => {
  const client = createControlledClient(
    { kind: "snapshotEnd", cursor: cursor(1n) },
    {
      kind: "resultSuffix",
      epoch: 1n,
      records: [
        {
          sequence: 2n,
          event: {
            kind: "itemStarted",
            item: { itemId: "msg_1", kind: { kind: "agentMessage" }, text: "Starting" },
          },
        },
        {
          sequence: 3n,
          event: {
            kind: "itemUpdated",
            item: { itemId: "msg_1", kind: { kind: "userMessage" }, text: "Diverged" },
          },
        },
      ],
    },
  );
  const admission = await createGatewayAttachment(client).open(target.sessionId);
  if (admission.kind !== "attached") throw new Error("Expected an attachment");
  const reader = admission.attachment.observations.getReader();

  expect((await reader.read()).value?.kind).toBe("snapshot");
  await expect(reader.read()).rejects.toThrow("changed a transcript item kind");
});
