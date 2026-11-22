import { describe, expect, test } from "bun:test";
import { FixtureSession } from "#app/session/fixture-session";
import { fixtureSessionId } from "#app/session/fixture-display";
import type { SessionAttachment } from "#app/session/session-attachment";
import { SessionProjection } from "#app/session/session-projection";

async function openFixture(fixture: FixtureSession): Promise<SessionAttachment> {
  const result = await fixture.open(fixtureSessionId);
  if (result.kind !== "attached") throw new Error("Expected the selected fixture session");
  return result.attachment;
}

describe("fixture attachment", () => {
  test("atomically captures history and follows updates exactly once", async (): Promise<void> => {
    const fixture = new FixtureSession();
    fixture.appendNotice("Before cut");
    const attachment = await openFixture(fixture);
    const observations = attachment.observations.getReader();

    fixture.appendNotice("After cut");
    const initial = (await observations.read()).value;
    const update = (await observations.read()).value;
    if (initial?.kind !== "snapshot" || update?.kind !== "event") {
      throw new Error("Expected the complete snapshot before the next event");
    }
    const projection = new SessionProjection(initial.snapshot);
    expect(initial.snapshot.entries.map((entry): string => entry.text)).toEqual([
      "Fixture history before attachment",
      "Before cut",
    ]);
    expect(projection.apply(update.event).kind).toBe("accepted");
    expect(projection.apply(update.event).kind).toBe("duplicate");
    expect(projection.apply({ ...update.event, cursor: { epoch: 0n, sequence: 10n } }).kind).toBe(
      "duplicate",
    );
    expect(projection.apply({ ...update.event, cursor: { epoch: 1n, sequence: 8n } }).kind).toBe(
      "invalidated",
    );
    expect(projection.history().map((entry): string => entry.text)).toEqual([
      "Fixture history before attachment",
      "Before cut",
      "After cut",
    ]);
    attachment.close();
    expect(await observations.read()).toEqual({ done: true, value: undefined });
  });

  test("terminates lag and refreshes the atomic cut under a new epoch", async (): Promise<void> => {
    const fixture = new FixtureSession(1);
    const lagging = await openFixture(fixture);
    fixture.appendNotice("Unconsumed before epoch reset");
    const lagged = lagging.observations.getReader();
    expect((await lagged.read()).value?.kind).toBe("snapshot");
    expect((await lagged.read()).value).toEqual({ kind: "gap", reason: "lag" });
    expect((await lagged.read()).done).toBe(true);

    const active = (await openFixture(fixture)).observations.getReader();
    await active.read();
    fixture.resetEpoch();
    expect((await active.read()).value).toEqual({ kind: "gap", reason: "epoch" });
    expect((await active.read()).done).toBe(true);

    const replacement = await openFixture(fixture);
    const recovered = replacement.observations.getReader();
    expect((await recovered.read()).value).toMatchObject({
      kind: "snapshot",
      snapshot: {
        cursor: { epoch: 2n, sequence: 1n },
        entries: [{ text: "Fixture history from replacement snapshot" }],
      },
    });
    fixture.disconnect();
    expect((await recovered.read()).value).toEqual({ kind: "ended", reason: "closed" });
    expect((await recovered.read()).done).toBe(true);
    expect(await fixture.open("ses_denied")).toEqual({ kind: "denied" });
  });
});
