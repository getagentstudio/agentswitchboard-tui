import type {
  AttachmentAdmission,
  AttachmentCursor,
  AttachmentObservation,
  SessionAttachmentPort,
  TranscriptEntry,
} from "#app/session/session-attachment";

import { fixtureCreatedAt, fixtureSession, fixtureSessionId } from "#app/session/fixture-display";

export class FixtureSession implements SessionAttachmentPort {
  private cursor: AttachmentCursor = { epoch: 1n, sequence: 1n };
  private entries: TranscriptEntry[] = [
    {
      kind: "user",
      id: "msg_fixture_initial",
      created: fixtureCreatedAt,
      text: "Fixture history before attachment",
    },
  ];
  private readonly consumers = new Set<ReadableStreamDefaultController<AttachmentObservation>>();

  constructor(private readonly backlog = 32) {}

  async open(sessionId: string): Promise<AttachmentAdmission> {
    if (sessionId !== fixtureSessionId) return { kind: "denied" };

    let consumer: ReadableStreamDefaultController<AttachmentObservation> | undefined;
    const unsubscribe = (): boolean => consumer !== undefined && this.consumers.delete(consumer);
    const observations = new ReadableStream<AttachmentObservation>(
      {
        start: (controller): void => {
          consumer = controller;
          // Both operations are synchronous, so each later append is on the live side of this cut.
          this.consumers.add(controller);
          controller.enqueue({
            kind: "snapshot",
            snapshot: {
              session: fixtureSession,
              cursor: this.cursor,
              entries: this.entries.slice(),
            },
          });
        },
        cancel: (): void => {
          unsubscribe();
        },
      },
      { highWaterMark: this.backlog },
    );

    return {
      kind: "attached",
      attachment: {
        observations,
        close(): void {
          if (unsubscribe()) consumer?.close();
        },
      },
    };
  }

  appendNotice(notice: string): void {
    const cursor = { ...this.cursor, sequence: this.cursor.sequence + 1n };
    const entry = {
      kind: "notice",
      id: `msg_fixture_${cursor.epoch}_${cursor.sequence}`,
      created: fixtureCreatedAt + Number(cursor.sequence),
      text: notice,
    } satisfies TranscriptEntry;

    this.cursor = cursor;
    this.entries.push(entry);
    for (const controller of this.consumers) {
      if ((controller.desiredSize ?? 0) <= 0) {
        this.finish(controller, { kind: "gap", reason: "lag" });
      } else {
        controller.enqueue({ kind: "event", event: { kind: "entry", cursor, entry } });
      }
    }
  }

  resetEpoch(): void {
    this.cursor = { epoch: this.cursor.epoch + 1n, sequence: 1n };
    this.entries = [
      {
        kind: "user",
        id: `msg_fixture_recovery_${this.cursor.epoch}`,
        created: fixtureCreatedAt,
        text: "Fixture history from replacement snapshot",
      },
    ];
    for (const controller of this.consumers)
      this.finish(controller, { kind: "gap", reason: "epoch" });
  }

  disconnect(): void {
    for (const controller of this.consumers)
      this.finish(controller, { kind: "ended", reason: "closed" });
  }

  private finish(
    controller: ReadableStreamDefaultController<AttachmentObservation>,
    ending: Extract<AttachmentObservation, { kind: "gap" | "ended" }>,
  ): void {
    this.consumers.delete(controller);
    controller.enqueue(ending);
    controller.close();
  }
}
