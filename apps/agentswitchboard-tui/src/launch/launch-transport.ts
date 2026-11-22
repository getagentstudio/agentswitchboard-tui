export type CompositionTrace = (event: Readonly<Record<string, string | number | boolean>>) => void;
export type InProcessFetch = (request: Request) => Response;

export const syntheticOrigin = "http://agentswitchboard.invalid";

export interface LaunchTransport {
  readonly platformFetch: typeof fetch;
  close(): void;
}

export function installLaunchTransport(
  route: InProcessFetch,
  trace: CompositionTrace,
): LaunchTransport {
  const platformFetch = globalThis.fetch;
  const platformWebSocket = globalThis.WebSocket;
  const scopedFetch = async (
    input: RequestInfo | URL,
    options?: RequestInit,
  ): Promise<Response> => {
    const request = new Request(input, options);
    const url = new URL(request.url);
    if (url.origin !== syntheticOrigin || url.username || url.password) {
      trace({ kind: "fetch-refused" });
      throw new Error("Fetch refused outside the TUI origin");
    }
    return route(request);
  };

  // The TUI opens editor sockets in onMount. Report a closed local socket to
  // keep that callback safe while preventing every platform constructor call.
  const deniedWebSocket = new Proxy(platformWebSocket, {
    construct(): EventTarget {
      trace({ kind: "websocket-refused" });
      return new (class extends EventTarget {
        readonly readyState = platformWebSocket.CLOSED;
        close(): void {}
        send(): never {
          throw new Error("WebSocket unavailable in attach-only mode");
        }
      })();
    },
  });

  Object.defineProperty(globalThis, "fetch", { value: scopedFetch });
  globalThis.WebSocket = deniedWebSocket;
  return {
    platformFetch,
    close(): void {
      Object.defineProperty(globalThis, "fetch", { value: platformFetch });
      globalThis.WebSocket = platformWebSocket;
    },
  };
}
