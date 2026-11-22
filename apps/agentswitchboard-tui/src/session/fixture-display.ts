import type { AttachedSession } from "#app/session/session-attachment";

export const fixtureSessionId = "ses_fixture";
export const fixtureDirectory = "/switchboard-fixture/workspace";

export const fixtureCreatedAt = 1_700_000_000_000;
export const fixtureSession = {
  id: fixtureSessionId,
  title: "Switchboard attach fixture",
  workspace: {
    directory: fixtureDirectory,
    projectId: "fixture-project",
    name: "Fixture workspace",
  },
  created: fixtureCreatedAt,
} satisfies AttachedSession;
