import { mkdtempSync, mkdirSync, realpathSync, rmSync } from "node:fs";
import os from "node:os";
import { join } from "node:path";

export interface LaunchProfile {
  readonly root: string;
  readonly home: string;
  readonly temporary: string;
  close(): void;
}

export function isolateLaunchProfile(): LaunchProfile {
  const originalDirectory = process.cwd();
  const platformHomedir = os.homedir;
  const root = mkdtempSync(join(realpathSync(os.tmpdir()), "agentswitchboard-tui-"));
  const home = join(root, "home");
  const temporary = join(root, "tmp");
  const neutralDirectory = join(root, "cwd");

  for (const name of Object.keys(process.env)) {
    if (/^(OPENCODE_|CLAUDE_CODE_SSE_PORT$|ZED_TERM$)|^(HTTP|HTTPS|ALL|NO)_PROXY$/i.test(name)) {
      delete process.env[name];
    }
  }

  if (process.env.TERM_PROGRAM?.toLowerCase() === "zed") delete process.env.TERM_PROGRAM;
  delete process.env.VISUAL;
  delete process.env.EDITOR;

  for (const directory of [home, temporary, neutralDirectory]) mkdirSync(directory);
  process.env.XDG_CONFIG_HOME = join(root, "config");
  process.env.XDG_DATA_HOME = join(root, "data");
  process.env.XDG_CACHE_HOME = join(root, "cache");
  process.env.XDG_STATE_HOME = join(root, "state");
  process.env.XDG_RUNTIME_DIR = join(root, "runtime");
  mkdirSync(process.env.XDG_RUNTIME_DIR, { mode: 0o700 });
  process.env.HOME = home;
  // Bun caches the process home before the managed launch can change HOME.
  Object.defineProperty(os, "homedir", { value: (): string => home });
  process.env.OPENCODE_TEST_HOME = home;
  process.env.TMPDIR = temporary;
  process.chdir(neutralDirectory);

  return {
    root,
    home,
    temporary,
    close(): void {
      Object.defineProperty(os, "homedir", { value: platformHomedir });
      process.chdir(originalDirectory);
      rmSync(root, { recursive: true });
    },
  };
}
