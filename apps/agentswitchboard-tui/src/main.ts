import { closeSync, mkdirSync, openSync, writeFileSync, writeSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { parseArgs } from "node:util";
import { Schema } from "#gateway/effect-runtime";
import { createGatewayClient } from "#gateway/transport/gateway-client";
import { createGatewayAttachment } from "#app/session/gateway-attachment";
import { isolateLaunchProfile } from "#app/launch/launch-profile";
import type { CompositionTrace } from "#app/launch/launch-transport";

const absolutePath = Schema.String.check(Schema.makeFilter(isAbsolute));
const launchArguments = Schema.Struct({
  session: Schema.String.check(Schema.isMinLength(1)),
  "gateway-socket": absolutePath,
  "fixture-ide-port": Schema.optionalKey(
    Schema.NumberFromString.check(
      Schema.isInt(),
      Schema.isGreaterThanOrEqualTo(1),
      Schema.isLessThanOrEqualTo(65535),
    ),
  ),
  trace: Schema.optionalKey(absolutePath),
});

async function main(): Promise<void> {
  const parsed = parseArgs({
    options: {
      session: { type: "string" },
      "gateway-socket": { type: "string" },
      "fixture-ide-port": { type: "string" },
      trace: { type: "string" },
      help: { type: "boolean" },
    },
  });
  if (parsed.values.help) {
    process.stdout.write(
      "Usage: agstui --gateway-socket <absolute socket> --session <id> [--trace <absolute file>]\n",
    );
    return;
  }
  const args = Schema.decodeUnknownSync(launchArguments)(parsed.values);
  const descriptor = args.trace === undefined ? undefined : openSync(args.trace, "wx", 0o600);
  const trace: CompositionTrace = (event): void => {
    if (descriptor !== undefined)
      writeSync(descriptor, `${JSON.stringify({ at: Date.now(), pid: process.pid, ...event })}\n`);
  };

  try {
    const profile = isolateLaunchProfile();
    try {
      trace({ kind: "profile", root: profile.root, cwd: process.cwd() });
      const editorPort = args["fixture-ide-port"];
      if (editorPort !== undefined) {
        const locks = join(profile.home, ".claude", "ide");
        mkdirSync(locks, { recursive: true });
        writeFileSync(
          join(locks, `${editorPort}.lock`),
          JSON.stringify({ workspaceFolders: [process.cwd()], transport: "ws" }),
        );
      }
      const { renderAttachment } = await import("#app/opencode/render-attachment");
      await renderAttachment({
        sessionId: args.session,
        createPort: (platformFetch) =>
          createGatewayAttachment(
            createGatewayClient({ socketPath: args["gateway-socket"], platformFetch, trace }),
          ),
        profile,
        trace,
      });
    } finally {
      profile.close();
      trace({ kind: "profile-closed" });
    }
  } finally {
    if (descriptor !== undefined) closeSync(descriptor);
  }
}

try {
  await main();
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
}
