export interface AttachmentCursor {
  readonly epoch: bigint;
  readonly sequence: bigint;
}

export interface TranscriptEntry {
  readonly id: string;
  readonly kind: "user" | "notice" | "assistant";
  readonly created: number;
  readonly text: string;
  readonly completed?: boolean;
}

export interface AttachedSession {
  readonly id: string;
  readonly title: string;
  readonly workspace: {
    readonly directory: string;
    readonly projectId: string;
    readonly name: string;
  };
  readonly created: number;
}

export interface SessionSnapshot {
  readonly session: AttachedSession;
  readonly cursor: AttachmentCursor;
  readonly entries: readonly TranscriptEntry[];
}

export type TranscriptChange =
  | { readonly kind: "entry"; readonly entry: TranscriptEntry }
  | { readonly kind: "progress" }
  | { readonly kind: "turn"; readonly status: "busy" | "idle" };

export type TranscriptEvent = TranscriptChange & { readonly cursor: AttachmentCursor };

export type AttachmentObservation =
  | { readonly kind: "snapshot"; readonly snapshot: SessionSnapshot }
  | { readonly kind: "event"; readonly event: TranscriptEvent }
  | { readonly kind: "gap"; readonly reason: "lag" | "epoch" | "eventTooLarge" }
  | { readonly kind: "ended"; readonly reason: "completed" | "cancelled" | "closed" };

export interface SessionAttachment {
  readonly observations: ReadableStream<AttachmentObservation>;
  close(): void;
}

export type AttachmentAdmission =
  | { readonly kind: "attached"; readonly attachment: SessionAttachment }
  | { readonly kind: "denied" };

export interface SessionAttachmentPort {
  open(sessionId: string): Promise<AttachmentAdmission>;
}
