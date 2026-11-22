#!/usr/bin/env bun
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { parseArgs } from "node:util";

import {
  appTrace,
  completeLines,
  executable,
  must,
  startProcess,
  stopProcess,
  TerminalHarness,
  until,
  verifyDigest,
  verifySocket,
  type Executable,
} from "./fixture-terminal.ts";
import { checkHistoryReceipts, correlateObservation, observedHistory } from "./fixture-receipts.ts";

const usage = [
  "Usage: bun scripts/verification/tui/verify-quinn-fixture.ts",
  "  --app <absolute agstui> --app-sha256 <hex>",
  "  --proof <absolute agentstudio-gateway-proof> --proof-sha256 <hex>",
  "  --output <new absolute receipt directory>",
  "Uses a private Unix Gateway, real loopback Quinn, and isolated stock TUI terminals.",
].join("\n");

interface Inputs {
  readonly app: Executable;
  readonly proof: Executable;
  readonly output: string;
}

interface RemotePeer {
  readonly endpoint: string;
  readonly control: string;
  readonly certificate: string;
  readonly history: readonly [string, string];
  readonly log: string;
}

interface RemoteRun {
  readonly peer: RemotePeer;
  readonly gatewayLog: string;
  readonly socket: string;
  readonly terminal: TerminalHarness;
  control(command: Record<string, unknown>): Promise<void>;
}

function readInputs(): Inputs | undefined {
  const { values } = parseArgs({
    options: {
      app: { type: "string" },
      "app-sha256": { type: "string" },
      proof: { type: "string" },
      "proof-sha256": { type: "string" },
      output: { type: "string" },
      help: { type: "boolean" },
    },
  });
  if (values.help) return undefined;
  if (!values.output || !isAbsolute(values.output))
    throw new Error("--output requires a new absolute path");
  return {
    app: executable(values.app, values["app-sha256"]),
    proof: executable(values.proof, values["proof-sha256"]),
    output: values.output,
  };
}

function witness(name: string): string {
  return `${name}-${crypto.randomUUID()}`;
}

async function startPeer(
  input: Inputs,
  directory: string,
  log: string,
  hold: "open" | "hold-cut",
): Promise<{ readonly peer: RemotePeer; readonly process: Bun.Subprocess }> {
  mkdirSync(directory, { mode: 0o700 });
  const history = [witness("peer-history-one"), witness("peer-history-two")] as const;
  const child = startProcess(input.proof, ["peer", directory, ...history, hold], log);
  try {
    const endpoint = await until("bound Quinn peer", (): string | undefined =>
      completeLines(log)
        .map((entry) => /^quinn-proof-peer-ready endpoint=(\S+) /.exec(entry)?.[1])
        .find(Boolean),
    );
    const control = join(directory, "peer.sock");
    verifySocket(control);
    return {
      peer: { endpoint, control, certificate: join(directory, "peer.der"), history, log },
      process: child,
    };
  } catch (error) {
    await stopProcess(child);
    throw error;
  }
}

async function withRemote(
  input: Inputs,
  label: string,
  hold: "open" | "hold-cut",
  untrusted: boolean,
  drive: (run: RemoteRun) => Promise<void>,
): Promise<void> {
  const temporary = mkdtempSync("/private/tmp/ag2b-");
  const evidence = join(input.output, label);
  const children: Bun.Subprocess[] = [];
  let terminal: TerminalHarness | undefined;
  try {
    terminal = new TerminalHarness(input.app, temporary, evidence);
    const started = await startPeer(
      input,
      join(temporary, "peer"),
      join(evidence, "peer.log"),
      hold,
    );
    children.push(started.process);
    terminal.verifyImage(started.process.pid, input.proof);
    const peer = started.peer;
    let certificate = peer.certificate;
    if (untrusted) {
      const other = await startPeer(
        input,
        join(temporary, "wrong"),
        join(evidence, "untrusted-cert.log"),
        "open",
      );
      children.push(other.process);
      certificate = other.peer.certificate;
    }
    const socket = join(temporary, "gw.sock");
    const gatewayLog = join(evidence, "gateway.log");
    const gateway = startProcess(
      input.proof,
      ["gateway", socket, peer.endpoint, certificate],
      gatewayLog,
    );
    children.push(gateway);
    await until("private Unix Gateway", (): true | undefined =>
      completeLines(gatewayLog).includes(`gateway-ready socket=${socket}`) ? true : undefined,
    );
    verifySocket(socket);
    terminal.verifyImage(gateway.pid, input.proof);
    await drive({
      peer,
      gatewayLog,
      socket,
      terminal,
      async control(command): Promise<void> {
        const response =
          await Bun.$`${input.proof.path} control ${peer.control} ${JSON.stringify(command)}`
            .quiet()
            .text();
        const result: unknown = JSON.parse(response);
        must(
          typeof result === "object" &&
            result !== null &&
            "kind" in result &&
            (result.kind === "published" || result.kind === "applied"),
          "Fixture control was refused",
        );
      },
    });
  } finally {
    try {
      terminal?.stop();
    } finally {
      try {
        for (const child of children.reverse()) await stopProcess(child);
        must(
          !existsSync(join(temporary, "gw.sock")) &&
            !existsSync(join(temporary, "peer", "peer.sock")),
          "Fixture sockets were not removed",
        );
      } finally {
        rmSync(temporary, { recursive: true });
      }
    }
  }
}

async function twoTerminals(run: RemoteRun): Promise<void> {
  const { terminal, peer, socket, gatewayLog } = run;
  const a = terminal.launch("a", socket);
  await terminal.visible(a, "history", ...peer.history);
  const aPid = terminal.verifyRunning(a);
  const aCall = correlateObservation({ terminal: a, gatewayLog, peerLog: peer.log, ordinal: 1 });
  checkHistoryReceipts(appTrace(a), aCall.requestId, ["9007199254740992", "9007199254740993"]);

  const b = terminal.launch("b", socket);
  await terminal.visible(b, "history", ...peer.history);
  must(terminal.verifyRunning(b) !== aPid, "Expected two distinct app processes");
  const bCall = correlateObservation({ terminal: b, gatewayLog, peerLog: peer.log, ordinal: 2 });
  must(
    aCall.stream !== bCall.stream && aCall.lease !== bCall.lease,
    "Expected independent streams and leases",
  );

  const first = witness("peer-live-one");
  await run.control({ kind: "publish", text: first });
  for (const pane of [a, b]) {
    const screen = await terminal.visible(pane, "first-live", first);
    must(
      screen.indexOf(peer.history[1]) < screen.indexOf(first) && screen.split(first).length === 2,
      "Live event ordering or duplication",
    );
  }
  await terminal.detach(a);
  await until("peer observed actual A retirement", (): true | undefined =>
    completeLines(peer.log).includes(`quinn-peer-source-close lease=${aCall.lease}`)
      ? true
      : undefined,
  );
  must(
    !completeLines(peer.log).includes(`quinn-peer-source-close lease=${bCall.lease}`),
    "B retired with A",
  );
  const second = witness("peer-live-two");
  await run.control({ kind: "publish", text: second });
  await terminal.visible(b, "remaining-live", second);
  must(!terminal.screen(a).includes(second), "A displayed the post-detach item");
  correlateObservation({ terminal: b, gatewayLog, peerLog: peer.log, ordinal: 2 });
  await terminal.detach(b);
  await until("peer observed actual B retirement", (): true | undefined =>
    completeLines(peer.log).includes(`quinn-peer-source-close lease=${bCall.lease}`)
      ? true
      : undefined,
  );
}

async function heldCut(run: RemoteRun): Promise<void> {
  const { terminal, peer, socket, gatewayLog } = run;
  const pane = terminal.launch("held", socket);
  await until("two decoded history frames", (): true | undefined =>
    appTrace(pane).filter(
      (event) => event.kind === "gateway-frame-consumed" && event.frame === "history",
    ).length === 2
      ? true
      : undefined,
  );
  const call = correlateObservation({ terminal: pane, gatewayLog, peerLog: peer.log, ordinal: 1 });
  checkHistoryReceipts(appTrace(pane), call.requestId, ["9007199254740992", "9007199254740993"]);
  must(
    !appTrace(pane).some((event) => event.kind === "snapshot"),
    "App committed while the cut was held",
  );
  const live = witness("queued-live");
  await run.control({ kind: "publish", text: live });
  await run.control({ kind: "releaseCut" });
  const cut = await terminal.visible(pane, "marker-only", ...peer.history);
  terminal.verifyRunning(pane);
  const cutReads = appTrace(pane).filter(
    (event) =>
      event.kind === "gateway-frame-consumed" &&
      event.frame === "snapshotEnd" &&
      event.requestId === call.requestId,
  );
  const cutFrames = completeLines(peer.log).filter((line) =>
    line.startsWith(`quinn-peer-frame-sent stream=${call.stream} kind=`),
  );
  const commits = appTrace(pane).filter((event) => event.kind === "snapshot");
  must(
    cutReads.length === 1 &&
      commits.length === 1 &&
      cutReads[0]?.epoch === "1" &&
      cutReads[0]?.sequence === "9007199254740993" &&
      commits[0]?.epoch === "1" &&
      commits[0]?.sequence === "9007199254740993" &&
      cutFrames.map((line) => line.split("kind=")[1]).join(",") === "history,history,snapshotEnd" &&
      completeLines(peer.log).includes(`quinn-peer-live-wait lease=${call.lease}`) &&
      !cut.includes(live),
    "Marker-only release did not commit a cut while later frames remained held",
  );
  await run.control({ kind: "releaseLive" });
  const released = await terminal.visible(pane, "released-live", live);
  must(
    released.split(live).length === 2 && released.indexOf(peer.history[1]) < released.indexOf(live),
    "Queued live event ordering",
  );
  await terminal.detach(pane);
}

async function admittedLoss(run: RemoteRun, timing: "early" | "late"): Promise<void> {
  const { terminal, peer, socket, gatewayLog } = run;
  const pane = terminal.launch(timing, socket);
  let pid: number | undefined;
  if (timing === "early") {
    await until("app consumed history while cut was held", (): true | undefined =>
      appTrace(pane).filter(
        (event) => event.kind === "gateway-frame-consumed" && event.frame === "history",
      ).length === 2
        ? true
        : undefined,
    );
  } else {
    await terminal.visible(pane, "committed", ...peer.history);
    pid = terminal.verifyRunning(pane);
  }
  const call = correlateObservation({ terminal: pane, gatewayLog, peerLog: peer.log, ordinal: 1 });
  must(
    observedHistory(appTrace(pane), call.requestId).length === 2,
    "Loss did not reach the intended history consumer",
  );
  await run.control({ kind: "abort", stream: Number(call.stream) });
  if (timing === "early") {
    await terminal.exited(pane, 1);
    const screen = terminal.screen(pane);
    terminal.save("early-error.txt", screen);
    must(
      screen.includes("ConnectionLost") &&
        peer.history.every((text) => !screen.includes(text)) &&
        appTrace(pane).every((event) => event.kind !== "snapshot"),
      "Early loss committed or rendered a transcript",
    );
  } else {
    await terminal.visible(
      pane,
      "late-warning",
      "Gateway observation ended",
      "ConnectionLost",
      ...peer.history,
    );
    must(terminal.verifyRunning(pane) === pid, "Late loss reopened the renderer");
    correlateObservation({ terminal: pane, gatewayLog, peerLog: peer.log, ordinal: 1 });
    await terminal.detach(pane);
  }
}

async function wrongPeer(run: RemoteRun): Promise<void> {
  const pane = run.terminal.launch("untrusted-peer", run.socket);
  await run.terminal.exited(pane, 1);
  const screen = run.terminal.screen(pane);
  run.terminal.save("wrong-peer.txt", screen);
  const gateway = completeLines(run.gatewayLog);
  must(
    gateway.some((line) => line.startsWith("gateway-dispatch tool=provider_session_inspect ")) &&
      gateway.some((line) => line.startsWith("quinn-gateway-call-failed operation=inspect ")) &&
      completeLines(run.peer.log).every((line) => !line.startsWith("quinn-peer-source-")) &&
      appTrace(pane).every((event) => event.kind !== "snapshot") &&
      run.peer.history.every((text) => !screen.includes(text)) &&
      screen.includes("Gateway tool failed (unavailable)"),
    "Untrusted peer failed somewhere other than the real Unix-to-Quinn call",
  );
}

async function main(input: Inputs): Promise<void> {
  await verifyDigest(input.app);
  await verifyDigest(input.proof);
  mkdirSync(input.output, { mode: 0o700 });
  const outcomes: string[] = [];
  let completed = false;
  try {
    await withRemote(input, "two-terminals", "open", false, twoTerminals);
    outcomes.push("two terminals, peer-only history and live, lease retirement");
    await withRemote(input, "held-cut", "hold-cut", false, heldCut);
    outcomes.push("held cut and marker-only release");
    await withRemote(input, "early-loss", "hold-cut", false, async (run): Promise<void> =>
      admittedLoss(run, "early"),
    );
    outcomes.push("admitted early loss");
    await withRemote(input, "late-loss", "open", false, async (run): Promise<void> =>
      admittedLoss(run, "late"),
    );
    outcomes.push("same-renderer late loss");
    await withRemote(input, "wrong-peer", "open", true, wrongPeer);
    outcomes.push("untrusted peer through reachable Unix Gateway");
    await verifyDigest(input.app);
    await verifyDigest(input.proof);
    completed = true;
  } finally {
    writeFileSync(
      join(input.output, "receipt.json"),
      JSON.stringify(
        { input, outcomes, passed: outcomes.length, exit: completed ? 0 : 1 },
        null,
        2,
      ) + "\n",
      { mode: 0o600 },
    );
  }
  for (const outcome of outcomes) console.log(`PASS ${outcome}`);
}

if (import.meta.main) {
  const input = readInputs();
  if (input) await main(input);
  else console.log(usage);
}
