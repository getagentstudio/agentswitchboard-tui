import { isAbsolute } from "node:path";
import { createGatewayAttachment } from "#app/session/gateway-attachment";
import { Effect } from "#gateway/effect-runtime";
import { createGatewayClient } from "#gateway/transport/gateway-client";
import type { GatewayTarget } from "#gateway/transport/gateway-frames";

const socketPath = process.env.PHASE2A_CHECK_SOCKET;
if (!socketPath || !isAbsolute(socketPath))
  throw new Error("The test requires an absolute Gateway socket");

const target: GatewayTarget = {
  endpoint: { serviceId: "00000000-0000-4000-8000-000000000001", endpointId: "claude-local" },
  sessionId: "ses_fixture",
};
const client = createGatewayClient({ socketPath, platformFetch: globalThis.fetch });

const frames = await Effect.runPromise(client.observe(target, AbortSignal.timeout(5_000)));
const terminal = (await frames.next()).value;
if (
  terminal?.kind !== "gap" ||
  terminal.reason !== "eventTooLarge" ||
  terminal.cursor !== null ||
  !(await frames.next()).done
) {
  throw new Error("Expected one oversized Gateway gap with a null cursor");
}

const port = createGatewayAttachment(client);
const admission = await port.open(target.sessionId);
if (admission.kind !== "attached")
  throw new Error("Fixture inspect did not authorize the attachment");

const reader = admission.attachment.observations.getReader();
try {
  const first = (await reader.read()).value;
  if (first?.kind !== "gap" || first.reason !== "eventTooLarge" || !(await reader.read()).done) {
    throw new Error("Expected the app attachment to end before committing any snapshot");
  }
  process.stdout.write("pre-cut gap: null cursor, zero committed snapshots\n");
} finally {
  admission.attachment.close();
  reader.releaseLock();
}
