import { dirname, isAbsolute, join } from "node:path";
import { createSolidTransformPlugin } from "../../vendors/opencode/packages/tui/node_modules/@opentui/solid/scripts/solid-plugin.js";

const output = process.argv[2];
if (!output || !isAbsolute(output)) {
  throw new Error("Usage: bun apps/agentswitchboard-tui/build.ts <absolute output path>");
}

// The pinned CLI replaces runtime package discovery when compiling. Our offline app
// has no external plugin modules to expose to the runtime package loader.
const offlinePluginRuntime: Bun.BunPlugin = {
  name: "offline-plugin-runtime",
  setup(build): void {
    build.onLoad({ filter: /[/\\]plugin[/\\]src[/\\]runtime-modules\.ts$/ }, () => ({
      loader: "ts",
      contents:
        "export const resolveHostPackageRoots = () => []; export const loadRuntimeModules = () => ({});",
    }));
  },
};

const entrypoint = join(import.meta.dirname, "src/main.ts");
process.chdir(dirname(output));
const result = await Bun.build({
  entrypoints: [entrypoint],
  plugins: [offlinePluginRuntime, createSolidTransformPlugin()],
  compile: {
    outfile: output,
    autoloadBunfig: false,
    autoloadDotenv: false,
    autoloadPackageJson: false,
    autoloadTsconfig: false,
  },
  format: "esm",
  target: "bun",
});

if (!result.success) throw new AggregateError(result.logs, "Unable to compile the TUI");
