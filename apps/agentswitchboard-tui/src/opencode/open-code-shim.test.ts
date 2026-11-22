import { expect, test } from "bun:test";
import { OpenCode } from "#opencode-client";
import { fixtureDirectory, fixtureSession, fixtureSessionId } from "#app/session/fixture-display";
import {
  installLaunchTransport,
  syntheticOrigin,
  type CompositionTrace,
} from "#app/launch/launch-transport";
import { createOpenCodeShim } from "#app/opencode/open-code-shim";
import { OpenCodeStream } from "#app/opencode/open-code-stream";
import { SessionProjection } from "#app/session/session-projection";

test("the pinned client reads and streams one scoped transcript while mutation and other origins fail", async (): Promise<void> => {
  const trace: CompositionTrace = (): void => {};
  const projection = new SessionProjection({
    session: fixtureSession,
    cursor: { epoch: 1n, sequence: 2n },
    entries: [
      { kind: "user", id: "msg_first", created: 1, text: "First" },
      { kind: "user", id: "msg_second", created: 2, text: "Second" },
    ],
  });
  const events = new OpenCodeStream(trace, { directory: fixtureDirectory });
  const shim = createOpenCodeShim({
    events,
    projection,
    attached: fixtureSession,
    temporaryDirectory: "/fixture",
    trace,
  });
  const transport = installLaunchTransport(shim, trace);
  const api = OpenCode.make({ baseUrl: syntheticOrigin, fetch: globalThis.fetch });
  const cancellation = new AbortController();

  try {
    expect(transport.platformFetch).not.toBe(globalThis.fetch);
    expect(
      (await api.file.list({ location: { directory: "/unrelated" } })).location.directory,
    ).toBe(fixtureDirectory);
    expect((await api.model.list()).data.map((model): string => model.name)).toEqual([
      "Fixture model",
    ]);
    expect((await api.skill.list()).data).toEqual([]);
    await expect(api.file.find({ query: "unavailable" })).rejects.toThrow("Capability unavailable");
    await expect(api.message.list({ sessionID: "ses_denied" })).rejects.toThrow(
      "outside this attachment",
    );
    await expect(
      api.session.update({ sessionID: fixtureSessionId, title: "Forbidden" }),
    ).rejects.toThrow("Session mutations are disabled");
    expect((await api.session.get({ sessionID: fixtureSessionId })).title).toBe(
      "Switchboard attach fixture",
    );
    await expect(fetch("http://127.0.0.1:1")).rejects.toThrow("outside the TUI origin");
    expect(new WebSocket("ws://127.0.0.1:1").readyState).toBe(WebSocket.CLOSED);

    const firstPage = await api.message.list({
      sessionID: fixtureSessionId,
      order: "asc",
      limit: 1,
    });
    expect(firstPage.data.map((message): string => message.id)).toEqual(["msg_first"]);
    expect(firstPage.cursor.next).toBe("msg_first");
    const secondPage = await api.message.list({
      sessionID: fixtureSessionId,
      order: "asc",
      limit: 1,
      cursor: "msg_first",
    });
    expect(secondPage.data.map((message): string => message.id)).toEqual(["msg_second"]);

    const stream = api.event.subscribe({ signal: cancellation.signal })[Symbol.asyncIterator]();
    expect((await stream.next()).value).toMatchObject({ type: "server.connected" });
    const event = {
      kind: "entry",
      cursor: { epoch: 1n, sequence: 3n },
      entry: { kind: "notice", id: "msg_third", created: 3, text: "Third" },
    } as const;
    expect(projection.apply(event).kind).toBe("accepted");
    events.publish(fixtureSessionId, event);
    expect((await stream.next()).value).toMatchObject({
      id: "evt_third",
      type: "session.synthetic",
      data: { sessionID: fixtureSessionId, text: "Third" },
    });
    expect((await api.message.list({ sessionID: fixtureSessionId, limit: 1 })).data).toMatchObject([
      { id: "msg_third", type: "synthetic", text: "Third" },
    ]);
    await stream.return?.();
  } finally {
    cancellation.abort();
    transport.close();
  }
});
