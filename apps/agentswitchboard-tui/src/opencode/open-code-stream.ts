import type { LocationPublicRef, OpenCodeEvent } from "#opencode-client";
import type { CompositionTrace } from "#app/launch/launch-transport";
import type { TranscriptEvent } from "#app/session/session-attachment";

type StreamWriter = (bytes: string) => void;
type AttachmentWarning = Extract<OpenCodeEvent, { type: "tui.toast.show" }>;

export class OpenCodeStream {
  private readonly listeners = new Set<StreamWriter>();
  private sequence = 0;
  private warning?: AttachmentWarning;
  private readonly seen: Set<string>;
  private readonly encoder = new TextEncoder();

  constructor(
    private readonly trace: CompositionTrace,
    private readonly location: LocationPublicRef,
    existingEntries: readonly { readonly id: string }[] = [],
  ) {
    this.seen = new Set(existingEntries.map((entry) => entry.id));
  }

  open(signal: AbortSignal): Response {
    let cleanup: () => void = (): void => {};
    const body = new ReadableStream<Uint8Array>({
      start: (controller): void => {
        const send: StreamWriter = (bytes): void => controller.enqueue(this.encoder.encode(bytes));
        const keepalive = setInterval((): void => {
          send(": keepalive\n\n");
          this.trace({ kind: "sse-keepalive" });
        }, 15_000);

        const stop = (): void => {
          cleanup();
          controller.close();
        };
        cleanup = (): void => {
          clearInterval(keepalive);
          this.listeners.delete(send);
          signal.removeEventListener("abort", stop);
          cleanup = (): void => {};
          this.trace({ kind: "sse-closed" });
        };

        if (signal.aborted) {
          stop();
          return;
        }
        signal.addEventListener("abort", stop, { once: true });

        const connected = {
          id: "evt_fixture_connected",
          type: "server.connected",
          data: {},
        } satisfies OpenCodeEvent;
        send(encodeEvent(connected));
        // A terminal Gateway frame can arrive before OpenCode opens its SSE stream.
        if (this.warning) send(encodeEvent(this.warning));
        this.trace({ kind: "sse-connected" });
        this.listeners.add(send);
      },
      cancel(): void {
        cleanup();
      },
    });
    return new Response(body, { headers: { "content-type": "text/event-stream" } });
  }

  publish(sessionId: string, event: TranscriptEvent): void {
    const { cursor } = event;
    if (event.kind === "progress") return;
    if (event.kind === "turn") {
      this.send({
        id: `evt_fixture_status_${++this.sequence}`,
        created: Date.now(),
        type: "session.status",
        data: { sessionID: sessionId, status: { type: event.status } },
      });
      return;
    }
    const { entry } = event;
    const alreadySeen = this.seen.has(entry.id);
    this.seen.add(entry.id);
    // OpenCode's sequence is numeric. A renderer-local counter preserves exact Router cursors in the projection.
    const durable = { aggregateID: sessionId, seq: ++this.sequence, version: 1 } as const;
    const eventId = `evt_gateway_${durable.seq}`;
    const created = entry.created;
    if (entry.kind === "assistant") {
      const data = { sessionID: sessionId, assistantMessageID: entry.id };
      if (!alreadySeen) {
        this.send({
          id: eventId,
          created,
          durable,
          type: "session.step.started",
          data: {
            ...data,
            agent: "fixture",
            model: { id: "fixture", providerID: "fixture" },
            started: created,
          },
        });
        this.send({
          id: `${eventId}_start`,
          created,
          durable,
          type: "session.text.started",
          data: { ...data, ordinal: 0 },
        });
      }
      this.send({
        id: `${eventId}_text`,
        created,
        durable,
        type: "session.text.ended",
        data: { ...data, ordinal: 0, text: entry.text },
      });
      if (entry.completed) {
        this.send({
          id: `${eventId}_end`,
          created,
          durable,
          type: "session.step.ended",
          data: {
            ...data,
            finish: "stop",
            cost: 0,
            tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
          },
        });
      }
    } else if (entry.kind === "notice" && !alreadySeen) {
      this.send({
        id: entry.id.replace(/^msg_/, "evt_"),
        created,
        durable,
        type: "session.synthetic",
        data: { sessionID: sessionId, text: entry.text, description: entry.text },
      });
    } else {
      const data = { sessionID: sessionId, inboxID: entry.id };
      const item =
        entry.kind === "user"
          ? ({ type: "user", delivery: "steer", payload: { text: entry.text } } as const)
          : ({
              type: "synthetic",
              delivery: "steer",
              payload: { text: entry.text, description: entry.text },
            } as const);
      this.send({
        id: eventId,
        created,
        durable,
        type: "session.inbox.enqueued",
        data: { ...data, item },
      });
      this.send({
        id: `${eventId}_delivered`,
        created,
        durable,
        type: "session.inbox.delivered",
        data,
      });
    }
    this.trace({
      kind: "sse-event",
      epoch: cursor.epoch.toString(),
      sequence: cursor.sequence.toString(),
      listeners: this.listeners.size,
    });
  }

  reportUnavailable(message: string): void {
    this.warning = {
      id: `evt_fixture_error_${++this.sequence}`,
      created: Date.now(),
      type: "tui.toast.show",
      location: this.location,
      data: { title: "Gateway observation ended", message, variant: "warning" },
    };
    this.send(this.warning);
  }

  private send(event: OpenCodeEvent): void {
    const bytes = encodeEvent(event);
    for (const send of this.listeners) send(bytes);
  }
}

function encodeEvent(event: OpenCodeEvent): string {
  return `data: ${JSON.stringify(event)}\n\n`;
}
