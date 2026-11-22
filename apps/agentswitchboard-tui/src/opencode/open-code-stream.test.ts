import { expect, test } from "bun:test";
import { OpenCode, type OpenCodeEvent } from "#opencode-client";
import { installLaunchTransport, syntheticOrigin } from "#app/launch/launch-transport";
import { OpenCodeStream } from "#app/opencode/open-code-stream";

const directory = "/switchboard-fixture/workspace";
const warning = "The Gateway stopped after an oversized event";

function startWarningStream(events: OpenCodeStream): {
  readonly stream: AsyncIterator<OpenCodeEvent>;
  close(): void;
} {
  const lifetime = new AbortController();
  const transport = installLaunchTransport(
    (request) => events.open(request.signal),
    (): void => {},
  );
  const client = OpenCode.make({ baseUrl: syntheticOrigin, fetch: globalThis.fetch });
  return {
    stream: client.event.subscribe({ signal: lifetime.signal })[Symbol.asyncIterator](),
    close(): void {
      lifetime.abort();
      transport.close();
    },
  };
}

test("the pinned OpenCode client receives a warning for the attached workspace", async (): Promise<void> => {
  const events = new OpenCodeStream((): void => {}, { directory });
  const { stream, close } = startWarningStream(events);

  try {
    expect((await stream.next()).value).toMatchObject({ type: "server.connected" });
    events.reportUnavailable(warning);

    expect((await stream.next()).value).toMatchObject({
      type: "tui.toast.show",
      location: { directory },
      data: { message: warning, variant: "warning" },
    });
  } finally {
    close();
    await stream.return?.();
  }
});

test("sends a pending warning after connected when the subscriber arrives late", async (): Promise<void> => {
  const events = new OpenCodeStream((): void => {}, { directory });
  events.reportUnavailable(warning);
  const { stream, close } = startWarningStream(events);

  try {
    expect((await stream.next()).value).toMatchObject({ type: "server.connected" });
    expect((await stream.next()).value).toMatchObject({
      type: "tui.toast.show",
      location: { directory },
      data: { message: warning },
    });
  } finally {
    close();
    await stream.return?.();
  }
}, 1_000);
