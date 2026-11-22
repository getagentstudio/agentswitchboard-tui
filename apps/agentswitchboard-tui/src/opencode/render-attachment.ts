import { Effect } from "../../../../vendors/opencode/packages/tui/node_modules/effect/dist/index.js";
import { layer as nodeFileSystem } from "../../../../vendors/opencode/packages/cli/node_modules/@effect/platform-node/dist/NodeFileSystem.js";
import type { TuiInput } from "#opencode-tui";
import {
  installLaunchTransport,
  syntheticOrigin,
  type CompositionTrace,
} from "#app/launch/launch-transport";
import { createOpenCodeShim } from "#app/opencode/open-code-shim";
import { OpenCodeStream } from "#app/opencode/open-code-stream";
import { SessionProjection } from "#app/session/session-projection";
import type { SessionAttachmentPort } from "#app/session/session-attachment";
import type { LaunchProfile } from "#app/launch/launch-profile";

interface RenderOptions {
  readonly sessionId: string;
  readonly createPort: (platformFetch: typeof fetch) => SessionAttachmentPort;
  readonly profile: LaunchProfile;
  readonly trace: CompositionTrace;
}

const launchConfig: TuiInput["config"] = {
  get: async (): Promise<Awaited<ReturnType<TuiInput["config"]["get"]>>> => ({
    plugins: [],
    experimental: { "client-mcp": false },
    attention: { sound: false, notifications: false },
    prompt: { editor: false },
  }),
  update: async (): Promise<never> => {
    throw new Error("TUI configuration is managed by the app");
  },
};

export async function renderAttachment(options: RenderOptions): Promise<void> {
  // Keep the guards installed across all renderer scopes and before any vendored imports.
  let route: ReturnType<typeof createOpenCodeShim> = (): Response =>
    Response.json(
      { _tag: "ServiceUnavailable", message: "Session attachment is pending" },
      { status: 503 },
    );
  const transport = installLaunchTransport((request): Response => route(request), options.trace);

  try {
    const port = options.createPort(transport.platformFetch);
    const { run } = await import("#opencode-tui");
    const { Global } = await import("#opencode-global");
    const tuiInput: TuiInput = {
      app: { name: "AgentSwitchboard TUI", version: "0.1.0", channel: "composition" },
      server: { endpoint: { url: syntheticOrigin }, local: false },
      args: { sessionID: options.sessionId },
      config: launchConfig,
      packages: {
        prepare: async (): Promise<never> => {
          throw new Error("External plugins are disabled in attach-only mode");
        },
      },
      log: (level, message): void => {
        if (level === "error") options.trace({ kind: "tui-error", message });
        if (message === "event stream connected") options.trace({ kind: "tui-sse-connected" });
      },
    };

    let shouldReopen = true;
    while (shouldReopen) {
      const admission = await port.open(options.sessionId);
      if (admission.kind === "denied")
        throw new Error("The selected fixture session is unavailable");
      const attachment = admission.attachment;
      const reader = attachment.observations.getReader();
      const renderer = new AbortController();
      try {
        const initial = await reader.read();
        if (!initial.done && initial.value.kind === "gap") {
          throw new Error(
            `Gateway lost continuity before the initial snapshot (${initial.value.reason})`,
          );
        }
        if (initial.done || initial.value.kind !== "snapshot") {
          throw new Error("The session attachment ended before its snapshot");
        }

        const snapshot = initial.value.snapshot;
        if (snapshot.session.id !== options.sessionId)
          throw new Error("Attachment returned a different session");
        options.trace({
          kind: "snapshot",
          epoch: snapshot.cursor.epoch.toString(),
          sequence: snapshot.cursor.sequence.toString(),
          entries: snapshot.entries.length,
        });
        const projection = new SessionProjection(snapshot);
        const events = new OpenCodeStream(
          options.trace,
          { directory: snapshot.session.workspace.directory },
          snapshot.entries,
        );
        route = createOpenCodeShim({
          attached: snapshot.session,
          temporaryDirectory: options.profile.temporary,
          projection,
          events,
          trace: options.trace,
        });

        const monitorEvents = async (): Promise<void> => {
          try {
            while (true) {
              const next = await reader.read();
              if (next.done) return;
              if (next.value.kind === "gap") {
                options.trace({ kind: "attachment-gap", reason: next.value.reason });
                if (next.value.reason === "eventTooLarge") {
                  events.reportUnavailable("The Gateway stopped after an oversized event");
                } else {
                  renderer.abort();
                }
                return;
              }
              if (next.value.kind !== "event") {
                options.trace({ kind: "attachment-ended", reason: next.value.kind });
                events.reportUnavailable("Live updates ended");
                return;
              }
              const applied = projection.apply(next.value.event);
              if (applied.kind === "invalidated") {
                events.reportUnavailable("Session cursor lost continuity");
                return;
              }
              if (applied.kind === "accepted") events.publish(options.sessionId, applied.event);
            }
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            options.trace({ kind: "attachment-error", message });
            events.reportUnavailable(message);
          }
        };
        const monitoring = monitorEvents();
        try {
          await Effect.runPromise(
            run(tuiInput).pipe(
              Effect.provide(
                Global.layerWith({
                  home: options.profile.home,
                  tmp: options.profile.temporary,
                }),
              ),
              Effect.provide(nodeFileSystem),
            ),
            { signal: renderer.signal },
          );
        } catch (error) {
          if (!renderer.signal.aborted) throw error;
        } finally {
          shouldReopen = renderer.signal.aborted;
          attachment.close();
          await reader.cancel().catch((): void => {});
          await monitoring;
        }
      } finally {
        attachment.close();
        options.trace({ kind: "attachment-closed" });
      }
    }
  } finally {
    transport.close();
  }
}
