import type {
  AgentInfo,
  ExperimentalMigrationV1StatusOutput,
  McpResourceCatalogOutput,
  LocationPublicInfo,
  ModelInfo,
  Project,
  ProviderInfo,
  ServerInfo,
  SessionInfo,
  SessionMessageInfo,
  SessionMessagesResponse,
  SessionsResponse,
  VcsGetOutput,
} from "#opencode-client";
import { Schema } from "#gateway/effect-runtime";
import type { CompositionTrace, InProcessFetch } from "#app/launch/launch-transport";
import type { OpenCodeStream } from "#app/opencode/open-code-stream";
import type { AttachedSession, TranscriptEntry } from "#app/session/session-attachment";
import type { SessionProjection } from "#app/session/session-projection";

interface ShimOptions {
  readonly attached: AttachedSession;
  readonly temporaryDirectory: string;
  readonly projection: SessionProjection;
  readonly events: OpenCodeStream;
  readonly trace: CompositionTrace;
}

const fixtureModel = { id: "fixture", providerID: "fixture" };
const fixtureAgent = {
  id: "fixture",
  name: "Fixture",
  model: fixtureModel,
  request: { settings: {}, headers: {}, body: {} },
  mode: "primary",
  hidden: false,
  permissions: [],
} satisfies AgentInfo;
const fixtureCatalog = [
  {
    ...fixtureModel,
    modelID: fixtureModel.id,
    name: "Fixture model",
    status: "active",
    enabled: true,
    capabilities: { tools: false, input: ["text"], output: ["text"] },
    limit: { context: 4096, output: 1024 },
    time: { released: 1_700_000_000_000 },
    variants: [],
    cost: [],
  },
] satisfies ModelInfo[];
const fixtureProviders = [
  {
    id: fixtureModel.providerID,
    name: "In-process fixture",
    activation: "enabled",
    package: "fixture",
  },
] satisfies ProviderInfo[];
const emptyLocationLists = new Set([
  "/api/command",
  "/api/skill",
  "/api/integration",
  "/api/mcp",
  "/api/reference",
  "/api/shell",
  "/api/form",
  "/api/plugin",
  "/api/vcs/status",
  "/api/vcs/branch",
  "/api/vcs/diff",
]);
const pageQuery = Schema.Struct({
  limit: Schema.optionalKey(
    Schema.NumberFromString.check(
      Schema.isInt(),
      Schema.isGreaterThanOrEqualTo(1),
      Schema.isLessThanOrEqualTo(500),
    ),
  ),
  order: Schema.optionalKey(Schema.Literals(["asc", "desc"])),
  type: Schema.optionalKey(
    Schema.Literals([
      "user",
      "synthetic",
      "agent-switched",
      "model-switched",
      "location-switched",
      "system",
      "skill",
      "shell",
      "assistant",
      "compaction",
      "idle",
    ]),
  ),
  cursor: Schema.optionalKey(Schema.String),
});

export function createOpenCodeShim(options: ShimOptions): InProcessFetch {
  const { attached } = options;
  const { directory, projectId, name } = attached.workspace;
  const fixtureLocation = { directory };
  const fixtureProject = {
    id: projectId,
    canonical: directory,
    name,
    time: { created: attached.created, updated: attached.created, active: attached.created },
    sandboxes: [],
  } satisfies Project;
  const location = {
    ...fixtureLocation,
    project: { id: projectId, directory, canonical: directory },
  } satisfies LocationPublicInfo;
  const atLocation = <ResponseData>(data: ResponseData): Response =>
    Response.json({ location: fixtureLocation, data });
  const session = {
    id: attached.id,
    projectID: projectId,
    location: fixtureLocation,
    agent: fixtureAgent.id,
    model: fixtureModel,
    title: attached.title,
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: attached.created, updated: attached.created },
  } satisfies SessionInfo;
  const sessionPath = `/api/session/${encodeURIComponent(session.id)}`;

  return (request): Response => {
    const url = new URL(request.url);
    const pathname = url.pathname;
    const query = url.searchParams;
    const response = respond();
    options.trace({
      kind: "route",
      method: request.method,
      path: pathname,
      status: response.status,
    });
    return response;

    function respond(): Response {
      if (request.method === "POST" && pathname === "/api/rpc/opencode.goal/get") {
        return unavailable();
      }
      if (request.method !== "GET") {
        return refuse(400, "BadRequest", "Session mutations are disabled in attach-only mode");
      }
      if (
        pathname.startsWith("/api/session/") &&
        pathname !== "/api/session/active" &&
        pathname !== sessionPath &&
        !pathname.startsWith(`${sessionPath}/`)
      ) {
        return refuse(404, "NotFound", "Session is outside this attachment");
      }
      const requestedSession = query.get("sessionID");
      if (requestedSession !== null && requestedSession !== session.id) {
        return refuse(404, "NotFound", "Session is outside this attachment");
      }

      switch (pathname) {
        case "/api/info":
          return Response.json({
            version: "0.1.0",
            pid: process.pid,
            urls: [],
            paths: { tmp: options.temporaryDirectory },
            capabilities: { persistentPty: false },
          } satisfies ServerInfo);
        case "/api/fs/list":
          return query.has("path")
            ? unavailable()
            : Response.json({ location: fixtureLocation, data: [] });
        case "/api/location":
          return Response.json(location);
        case "/api/event":
          return options.events.open(request.signal);
        case "/api/session":
          return Response.json({
            data: sessionMatches(query, session) ? [session] : [],
            cursor: {},
          } satisfies SessionsResponse);
        case "/api/session/active":
          return Response.json({ data: {} });
        case sessionPath:
          return Response.json({ data: session });
        case `${sessionPath}/message`:
          return transcriptPage(options.projection, query);
        case `${sessionPath}/inbox`:
        case `${sessionPath}/form`:
        case `${sessionPath}/permission`:
        case "/api/permission/request":
        case "/api/permission/saved":
          return Response.json({ data: [] });
        case "/api/project":
          return Response.json([fixtureProject]);
        case "/api/vcs":
          return atLocation({ branch: {} } satisfies VcsGetOutput["data"]);
        case "/api/vcs/base":
        case "/api/experimental/usage/limits":
          return atLocation(null);
        case "/api/config":
          return Response.json([]);
        case "/api/mcp/resource":
          return Response.json({
            location: fixtureLocation,
            data: { resources: [], templates: [] },
          } satisfies McpResourceCatalogOutput);
        case "/api/experimental/migration/v1":
          // A fresh fixture profile has no v1 state to migrate.
          return Response.json({
            status: "completed",
          } satisfies ExperimentalMigrationV1StatusOutput);
        case "/api/agent":
          return atLocation([fixtureAgent]);
        case "/api/model":
          return atLocation(fixtureCatalog);
        case "/api/provider":
          return atLocation(fixtureProviders);
        default:
          return emptyLocationLists.has(pathname) ? atLocation([]) : unavailable();
      }
    }
  };
}

function sessionMatches(query: URLSearchParams, session: SessionInfo): boolean {
  const directory = query.get("directory");
  const projectId = query.get("projectID");
  const parentId = query.get("parentID");
  return (
    (directory === null || directory === session.location.directory) &&
    (projectId === null || projectId === session.projectID) &&
    (parentId === null || parentId === "null") &&
    query.get("archived") !== "true"
  );
}

function transcriptPage(projection: SessionProjection, query: URLSearchParams): Response {
  let decoded: typeof pageQuery.Type;
  try {
    decoded = Schema.decodeUnknownSync(pageQuery)(Object.fromEntries(query));
  } catch {
    return refuse(400, "BadRequest", "Invalid transcript page");
  }
  const { limit = 50, order = "desc", cursor, type } = decoded;
  const entries = projection
    .history()
    .map(toOpenCodeMessage)
    .filter((message): boolean => !type || message.type === type);
  if (order === "desc") entries.reverse();
  const offset =
    cursor === undefined ? 0 : entries.findIndex((entry): boolean => entry.id === cursor) + 1;
  if (cursor !== undefined && offset === 0)
    return refuse(400, "BadRequest", "Unknown transcript cursor");

  const data = entries.slice(offset, offset + limit);
  const next = offset + limit < entries.length ? data.at(-1)?.id : undefined;
  return Response.json({ data, cursor: { next } } satisfies SessionMessagesResponse);
}

function toOpenCodeMessage(entry: TranscriptEntry): SessionMessageInfo {
  const message = { id: entry.id, time: { created: entry.created }, text: entry.text };
  switch (entry.kind) {
    case "user":
      return { ...message, type: "user" };
    case "notice":
      return { ...message, type: "synthetic", description: entry.text };
    case "assistant":
      return {
        id: entry.id,
        type: "assistant",
        time: { created: entry.created, ...(entry.completed ? { completed: entry.created } : {}) },
        agent: fixtureAgent.id,
        model: fixtureModel,
        content: [{ type: "text", text: entry.text }],
        ...(entry.completed ? { finish: "stop" } : {}),
      };
  }
}

function unavailable(): Response {
  return refuse(404, "NotFound", "Capability unavailable in this fixture");
}

function refuse(status: number, tag: string, message: string): Response {
  return Response.json({ _tag: tag, message }, { status });
}
