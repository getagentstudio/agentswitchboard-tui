import type {
  SessionSnapshot,
  TranscriptEntry,
  TranscriptEvent,
} from "#app/session/session-attachment";

export type EventApplication =
  | { readonly kind: "accepted"; readonly event: TranscriptEvent }
  | { readonly kind: "duplicate" }
  | { readonly kind: "invalidated" };

export class SessionProjection {
  private cursor: SessionSnapshot["cursor"];
  private readonly entries: TranscriptEntry[];
  private readonly entryPositions: Map<string, number>;

  constructor(snapshot: SessionSnapshot) {
    this.cursor = snapshot.cursor;
    this.entries = [...snapshot.entries];
    this.entryPositions = new Map(this.entries.map((entry, index) => [entry.id, index]));
  }

  history(): readonly TranscriptEntry[] {
    return this.entries.slice();
  }

  apply(event: TranscriptEvent): EventApplication {
    if (event.cursor.epoch < this.cursor.epoch) return { kind: "duplicate" };
    if (event.cursor.epoch !== this.cursor.epoch) return { kind: "invalidated" };
    if (event.cursor.sequence <= this.cursor.sequence) return { kind: "duplicate" };
    if (event.cursor.sequence !== this.cursor.sequence + 1n) return { kind: "invalidated" };
    this.cursor = event.cursor;
    if (event.kind === "entry") {
      const previous = this.entryPositions.get(event.entry.id);
      if (previous === undefined) {
        this.entryPositions.set(event.entry.id, this.entries.length);
        this.entries.push(event.entry);
      } else {
        this.entries[previous] = event.entry;
      }
    }
    return { kind: "accepted", event };
  }
}
