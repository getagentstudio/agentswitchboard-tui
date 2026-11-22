import { Effect } from "#gateway/effect-runtime";
import type { GatewayClient } from "#gateway/transport/gateway-client";
import type { GatewayRecord, GatewayTarget } from "#gateway/transport/gateway-frames";
import {
  type AttachmentAdmission,
  type AttachmentObservation,
  type SessionAttachmentPort,
  type TranscriptChange,
  type TranscriptEntry,
} from "#app/session/session-attachment";
import { fixtureSession } from "#app/session/fixture-display";

const fixtureEndpoint = {
  serviceId: "00000000-0000-4000-8000-000000000001",
  endpointId: "claude-local",
};

export function createGatewayAttachment(client: GatewayClient): SessionAttachmentPort {
  return {
    async open(sessionId): Promise<AttachmentAdmission> {
      const target: GatewayTarget = { endpoint: fixtureEndpoint, sessionId };
      const lifetime = new AbortController();
      const openDeadline = AbortSignal.timeout(5_000);
      const opening = (): void => lifetime.abort(openDeadline.reason);
      openDeadline.addEventListener("abort", opening, { once: true });
      try {
        const inspect = await Effect.runPromise(client.inspect(target, lifetime.signal));
        if (inspect.history !== "available") {
          lifetime.abort();
          return { kind: "denied" };
        }
        const frames = await Effect.runPromise(client.observe(target, lifetime.signal));
        let entries = new Map<string, TranscriptEntry>();
        let created = fixtureSession.created;
        const observations = new ReadableStream<AttachmentObservation>({
          async pull(controller): Promise<void> {
            try {
              while (true) {
                const next = await frames.next();
                if (next.done) {
                  controller.close();
                  return;
                }
                const frame = next.value;
                switch (frame.kind) {
                  case "history":
                    projectRecord(frame.record, entries, created++);
                    continue;
                  case "snapshotEnd":
                    openDeadline.removeEventListener("abort", opening);
                    controller.enqueue({
                      kind: "snapshot",
                      snapshot: {
                        session: { ...fixtureSession, id: inspect.target.sessionId },
                        cursor: frame.cursor,
                        entries: [...entries.values()],
                      },
                    });
                    return;
                  case "live":
                    controller.enqueue({
                      kind: "event",
                      event: {
                        ...projectRecord(frame.record, entries, created++),
                        cursor: frame.cursor,
                      },
                    });
                    return;
                  case "resultSuffix": {
                    const staged = new Map(entries);
                    const events = frame.records.map((record) => ({
                      ...projectRecord(record, staged, created++),
                      cursor: { epoch: frame.epoch, sequence: record.sequence },
                    }));
                    entries = staged;
                    for (const event of events) controller.enqueue({ kind: "event", event });
                    return;
                  }
                  case "gap":
                  case "ended":
                    controller.enqueue(frame);
                    controller.close();
                    openDeadline.removeEventListener("abort", opening);
                    lifetime.abort();
                    await frames.return(undefined);
                    return;
                }
              }
            } catch (error) {
              openDeadline.removeEventListener("abort", opening);
              lifetime.abort();
              await frames.return(undefined).catch((): void => {});
              controller.error(error);
            }
          },
          async cancel(): Promise<void> {
            openDeadline.removeEventListener("abort", opening);
            lifetime.abort();
            await frames.return(undefined);
          },
        });
        return {
          kind: "attached",
          attachment: {
            observations,
            close(): void {
              openDeadline.removeEventListener("abort", opening);
              lifetime.abort();
            },
          },
        };
      } catch (error) {
        openDeadline.removeEventListener("abort", opening);
        lifetime.abort();
        throw error;
      }
    },
  };
}

function projectRecord(
  record: GatewayRecord,
  entries: Map<string, TranscriptEntry>,
  created: number,
): TranscriptChange {
  const event = record.event;
  switch (event.kind) {
    case "itemStarted":
    case "itemUpdated": {
      const item = event.item;
      const previous = entries.get(item.itemId);
      if (event.kind === "itemStarted" && previous)
        throw new Error("Gateway repeated an item identity");
      if (item.text === undefined || item.text === null) return { kind: "progress" };
      const kind =
        item.kind.kind === "agentMessage"
          ? "assistant"
          : item.kind.kind === "userMessage"
            ? "user"
            : "notice";
      if (previous !== undefined && previous.kind !== kind)
        throw new Error("Gateway changed a transcript item kind");
      const entry = {
        id: item.itemId,
        text: item.text,
        kind,
        created: previous?.created ?? created,
        ...(previous?.completed ? { completed: true } : {}),
      } satisfies TranscriptEntry;
      entries.set(entry.id, entry);
      return { kind: "entry", entry };
    }
    case "itemCompleted": {
      const previous = entries.get(event.itemId);
      if (!previous || previous.completed) return { kind: "progress" };
      const entry = { ...previous, completed: true };
      entries.set(entry.id, entry);
      return { kind: "entry", entry };
    }
    case "turnStarted":
      return { kind: "turn", status: "busy" };
    case "turnEnded":
      return { kind: "turn", status: "idle" };
    case "resyncRequired":
      throw new Error("Expected a Gateway gap for resync");
    default: {
      const exhaustive: never = event;
      throw new Error(`Unhandled observation event ${String(exhaustive)}`);
    }
  }
}
