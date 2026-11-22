import { spawnSync } from "node:child_process";
import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  realpathSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { isAbsolute, join } from "node:path";

export interface Executable {
  readonly path: string;
  readonly sha256: string;
}

export interface Terminal {
  readonly name: string;
  readonly pane: string;
  readonly traceFile: string;
  readonly exitFile: string;
}

export interface AppTrace {
  readonly kind: string;
  readonly pid: number;
  readonly [key: string]: unknown;
}

export function executable(path: string | undefined, sha256: string | undefined): Executable {
  if (!path || !isAbsolute(path) || !sha256 || !/^[a-f0-9]{64}$/i.test(sha256)) {
    throw new Error("Each executable needs an absolute path and a 64-digit SHA-256");
  }
  return { path: realpathSync(path), sha256: sha256.toLowerCase() };
}

export async function verifyDigest(binary: Executable): Promise<void> {
  const hash = new Bun.CryptoHasher("sha256");
  for await (const chunk of Bun.file(binary.path).stream()) hash.update(chunk);
  must(hash.digest("hex") === binary.sha256, "Executable digest changed");
}

export function must(verified: boolean, label: string): asserts verified {
  if (!verified) throw new Error(label);
}

export function completeLines(file: string): string[] {
  return existsSync(file) ? readFileSync(file, "utf8").split("\n").slice(0, -1) : [];
}

export function appTrace(terminal: Terminal): AppTrace[] {
  return completeLines(terminal.traceFile).map((line): AppTrace => {
    const value: unknown = JSON.parse(line);
    if (!isTrace(value)) throw new Error(`Invalid app trace for ${terminal.name}`);
    return value;
  });
}

function isTrace(value: unknown): value is AppTrace {
  return (
    typeof value === "object" &&
    value !== null &&
    "kind" in value &&
    typeof value.kind === "string" &&
    "pid" in value &&
    typeof value.pid === "number"
  );
}

export async function until<Value>(
  label: string,
  inspect: () => Value | undefined,
  milliseconds = 10_000,
): Promise<Value> {
  const end = performance.now() + milliseconds;
  while (performance.now() < end) {
    const value = inspect();
    if (value !== undefined) return value;
    await Bun.sleep(40);
  }
  throw new Error(`Timed out waiting for ${label}`);
}

export function startProcess(
  binary: Executable,
  argv: readonly string[],
  log: string,
): Bun.Subprocess {
  const descriptor = openSync(log, "wx", 0o600);
  try {
    return Bun.spawn([binary.path, ...argv], {
      stdin: "ignore",
      stdout: descriptor,
      stderr: descriptor,
    });
  } finally {
    closeSync(descriptor);
  }
}

export async function stopProcess(child: Bun.Subprocess): Promise<void> {
  if (child.exitCode === null) child.kill("SIGTERM");
  const forced = setTimeout((): void => {
    if (child.exitCode === null) child.kill("SIGKILL");
  }, 5_000);
  try {
    must((await child.exited) === 0, `Process ${child.pid} did not stop cleanly`);
  } finally {
    clearTimeout(forced);
  }
}

function shellWord(text: string): string {
  return `'${text.replaceAll("'", "'\\''")}'`;
}

export class TerminalHarness {
  private readonly socket: string;
  private readonly tmuxPath: string;
  private readonly lsofPath: string;

  constructor(
    private readonly app: Executable,
    private readonly temporary: string,
    private readonly evidence: string,
  ) {
    this.socket = join(temporary, "tmux.sock");
    const tmux = Bun.which("tmux");
    const lsof = Bun.which("lsof");
    if (!tmux || !lsof) throw new Error("tmux and lsof are required");
    this.tmuxPath = tmux;
    this.lsofPath = lsof;
    mkdirSync(evidence, { mode: 0o700 });
    this.tmux("new-session", "-d", "-s", "fixture", "-x", "130", "-y", "40", "read keep");
  }

  launch(name: string, socket: string): Terminal {
    const pane = `fixture:${name}`;
    const traceFile = join(this.evidence, `${name}.jsonl`);
    const exitFile = join(this.evidence, `${name}.exit`);
    const invoke = [
      "/usr/bin/env",
      "-i",
      `HOME=${this.temporary}`,
      `TMPDIR=${this.temporary}`,
      "PATH=/usr/bin:/bin",
      "TERM=xterm-256color",
      this.app.path,
      "--gateway-socket",
      socket,
      "--session",
      "ses_fixture",
      "--trace",
      traceFile,
    ]
      .map(shellWord)
      .join(" ");
    this.tmux(
      "new-window",
      "-d",
      "-t",
      "fixture",
      "-n",
      name,
      `${invoke}; printf '%s\\n' "$?" > ${shellWord(exitFile)}; read keep`,
    );
    return { name, pane, traceFile, exitFile };
  }

  async visible(terminal: Terminal, label: string, ...required: string[]): Promise<string> {
    const screen = await until(label, (): string | undefined => {
      const visible = this.screen(terminal);
      return required.every((text) => visible.includes(text)) ? visible : undefined;
    });
    this.save(`${terminal.name}-${label}.txt`, screen);
    return screen;
  }

  screen(terminal: Terminal): string {
    return this.tmux("capture-pane", "-p", "-t", terminal.pane);
  }

  async exited(terminal: Terminal, expected: number): Promise<void> {
    const exit = await until("terminal exit", (): string | undefined =>
      existsSync(terminal.exitFile) ? readFileSync(terminal.exitFile, "utf8").trim() : undefined,
    );
    must(Number(exit) === expected, `Unexpected terminal exit ${terminal.name}: ${exit}`);
    must(
      appTrace(terminal).some((event) => event.kind === "profile-closed"),
      "App profile did not close",
    );
  }

  async detach(terminal: Terminal): Promise<void> {
    this.tmux("send-keys", "-t", terminal.pane, "C-c");
    await this.exited(terminal, 0);
  }

  verifyImage(pid: number, binary: Executable): void {
    const image = spawnSync(this.lsofPath, ["-nP", "-a", "-p", String(pid), "-d", "txt", "-Fn"], {
      encoding: "utf8",
      timeout: 4_000,
    });
    must(
      image.status === 0 && image.stdout.split("\n").includes(`n${binary.path}`),
      "Wrong process image",
    );
  }

  verifyRunning(terminal: Terminal): number {
    const entry = appTrace(terminal)[0];
    if (!entry) throw new Error("No app launch receipt");
    must(
      this.tmux("display-message", "-p", "-t", terminal.pane, "#{alternate_on}").trim() === "1",
      "No alternate screen",
    );
    this.verifyImage(entry.pid, this.app);
    must(
      appTrace(terminal).every((event) => event.pid === entry.pid),
      "App PID changed",
    );
    return entry.pid;
  }

  save(name: string, data: string): void {
    writeFileSync(join(this.evidence, name), data, { mode: 0o600 });
  }

  stop(): void {
    this.tmux("kill-server");
  }

  private tmux(...args: string[]): string {
    const reply = spawnSync(this.tmuxPath, ["-f", "/dev/null", "-S", this.socket, ...args], {
      encoding: "utf8",
      timeout: 4_000,
    });
    if (reply.status !== 0)
      throw new Error(`tmux ${args.join(" ")}: ${reply.error ?? reply.stderr}`);
    return reply.stdout;
  }
}

export function verifySocket(path: string): void {
  const metadata = statSync(path);
  must(metadata.isSocket() && (metadata.mode & 0o777) === 0o600, "Expected a private Unix socket");
}
