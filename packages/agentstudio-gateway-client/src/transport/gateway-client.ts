import { Effect, Schema } from "#gateway/effect-runtime";
import { parseWireJson } from "#gateway/contract/exact-u64";
import {
  BoundedObservation,
  EnvelopeRefusal,
  InspectSession,
  SessionRef,
} from "#gateway/contract/generated-contract";
import {
  decodeToolResult,
  receiveGatewayFrames,
  type GatewayFrame,
  type GatewayTarget,
} from "#gateway/transport/gateway-frames";

const maxResponseBytes = 4 * 1024 * 1024;
const origin = "http://agentstudio-gateway.invalid/mcp";

export class GatewayCallError extends Error {
  constructor(readonly detail: unknown) {
    super(detail instanceof Error ? detail.message : String(detail));
  }
}

export interface GatewayClient {
  inspect(
    target: GatewayTarget,
    signal: AbortSignal,
  ): Effect.Effect<typeof InspectSession.Type, GatewayCallError>;
  observe(
    target: GatewayTarget,
    signal: AbortSignal,
  ): Effect.Effect<AsyncGenerator<GatewayFrame>, GatewayCallError>;
}

export interface GatewayClientOptions {
  readonly socketPath: string;
  readonly platformFetch: typeof fetch;
  readonly trace?: (event: {
    readonly kind: string;
    readonly tool: string;
    readonly requestId: string;
    readonly status?: number;
  }) => void;
}

export function createGatewayClient(options: GatewayClientOptions): GatewayClient {
  async function send(
    tool: "provider_session_inspect" | "events_observe",
    target: GatewayTarget,
    signal: AbortSignal,
  ): Promise<{ readonly response: Response; readonly requestId: string }> {
    Schema.decodeUnknownSync(SessionRef, { onExcessProperty: "error" })(target);
    const requestId = crypto.randomUUID();
    const observe = tool === "events_observe";
    const argumentsValue = observe
      ? { target, timeoutSeconds: 86_400, maxEvents: 4096, maxBytes: 1_048_576 }
      : { target };
    options.trace?.({ kind: "gateway-request", tool, requestId });
    let response: Response;
    try {
      response = await options.platformFetch(origin, {
        unix: options.socketPath,
        method: "POST",
        headers: {
          "agentstudio-wire-version": "1",
          "content-type": "application/json",
          accept: observe ? "text/event-stream" : "application/json",
        },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: requestId,
          method: "tools/call",
          params: { name: tool, arguments: argumentsValue },
        }),
        timeout: false,
        redirect: "error",
        signal,
      });
    } catch (error) {
      throw new Error(
        `Unable to attach through the local Gateway socket: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    options.trace?.({ kind: "gateway-response", tool, requestId, status: response.status });
    if (response.status !== 200) {
      await requireContentType(response, "application/json");
      const refusal = Schema.decodeUnknownSync(EnvelopeRefusal, { onExcessProperty: "error" })(
        await readJson(response),
      );
      throw new Error(
        `Gateway refused (${refusal.envelopeError.code}): ${refusal.envelopeError.message}`,
      );
    }
    if (observe && contentType(response) === "application/json") {
      decodeToolResult(await readJson(response), requestId, BoundedObservation);
      throw new Error("Gateway observation returned JSON without an SSE stream");
    }
    await requireContentType(response, observe ? "text/event-stream" : "application/json");
    return { response, requestId };
  }

  return {
    inspect(target, signal): Effect.Effect<typeof InspectSession.Type, GatewayCallError> {
      return Effect.tryPromise({
        try: async (): Promise<typeof InspectSession.Type> => {
          const { response, requestId } = await send("provider_session_inspect", target, signal);
          const result = decodeToolResult(await readJson(response), requestId, InspectSession);
          if (
            result.target.sessionId !== target.sessionId ||
            result.target.endpoint.serviceId !== target.endpoint.serviceId ||
            result.target.endpoint.endpointId !== target.endpoint.endpointId
          ) {
            throw new Error("Gateway inspection returned a different session");
          }
          return result;
        },
        catch: (error): GatewayCallError => new GatewayCallError(error),
      });
    },
    observe(target, signal): Effect.Effect<AsyncGenerator<GatewayFrame>, GatewayCallError> {
      return Effect.tryPromise({
        try: async (): Promise<AsyncGenerator<GatewayFrame>> => {
          const { response, requestId } = await send("events_observe", target, signal);
          if (!response.body) throw new Error("Gateway observation has no body");
          return receiveGatewayFrames(response.body, requestId, target);
        },
        catch: (error): GatewayCallError => new GatewayCallError(error),
      });
    },
  };
}

function contentType(response: Response): string | undefined {
  return response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
}

async function requireContentType(response: Response, expected: string): Promise<void> {
  if (contentType(response) === expected) return;
  await response.body?.cancel();
  throw new Error(`Gateway response did not supply ${expected}`);
}

async function readJson(response: Response): Promise<unknown> {
  if (!response.body) throw new Error("Gateway JSON response has no body");
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const text: string[] = [];
  let size = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.length;
      if (size > maxResponseBytes) throw new Error("Gateway JSON response too large");
      text.push(decoder.decode(next.value, { stream: true }));
    }
    text.push(decoder.decode());
    return parseWireJson(text.join(""));
  } finally {
    try {
      await reader.cancel();
    } finally {
      reader.releaseLock();
    }
  }
}
