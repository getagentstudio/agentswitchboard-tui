import { appTrace, completeLines, must, type AppTrace, type Terminal } from "./fixture-terminal.ts";

export function observedHistory(
  events: readonly AppTrace[],
  requestId: string,
): readonly AppTrace[] {
  return events.filter(
    (event): boolean =>
      event.kind === "gateway-frame-consumed" &&
      event.requestId === requestId &&
      event.frame === "history",
  );
}

export function checkHistoryReceipts(
  events: readonly AppTrace[],
  requestId: string,
  expectedSequences: readonly string[],
): void {
  const records = observedHistory(events, requestId);
  must(
    records.length === expectedSequences.length &&
      records.every(
        (record, index): boolean =>
          record.sequence === expectedSequences[index] &&
          typeof record.epoch === "string" &&
          /^[0-9]+$/.test(record.epoch) &&
          Object.keys(record).every((key) =>
            ["kind", "pid", "at", "requestId", "frame", "epoch", "sequence"].includes(key),
          ),
      ),
    "History evidence did not match the selected request and exact cursor",
  );
}

export interface ObservationReceipt {
  readonly requestId: string;
  readonly stream: string;
  readonly lease: string;
}

interface RecordedCall {
  readonly operation: string;
  readonly stream: string;
  readonly stable: string;
}

function parseCall(line: string): RecordedCall {
  const [, operation, stream, stable] =
    /^quinn-gateway-call operation=(inspect|observe) stream=([0-9]+) stable=([0-9]+)$/.exec(line) ??
    [];
  if (!operation || stream === undefined || !stable)
    throw new Error("Malformed Gateway call receipt");
  return { operation, stream, stable };
}

export function correlateObservation(input: {
  readonly terminal: Terminal;
  readonly gatewayLog: string;
  readonly peerLog: string;
  readonly ordinal: number;
}): ObservationReceipt {
  const { terminal, gatewayLog, peerLog, ordinal } = input;
  const expected = ordinal * 2;
  const requested = appTrace(terminal).filter((event) => event.kind === "gateway-request");
  must(
    requested.length === 2 &&
      requested[0]?.tool === "provider_session_inspect" &&
      requested[1]?.tool === "events_observe",
    "Unexpected app call sequence during the correlation barrier",
  );
  const gateway = completeLines(gatewayLog);
  const dispatched = gateway.filter((entry) => entry.startsWith("gateway-dispatch "));
  const calls = gateway.filter((entry) => entry.startsWith("quinn-gateway-call ")).map(parseCall);
  const peer = completeLines(peerLog);
  const observed = peer.filter((entry) => entry.startsWith("quinn-peer-dispatch "));
  must(
    dispatched.length === expected && calls.length === expected && observed.length === expected,
    "Unmatched or competing calls during the correlation barrier",
  );
  const lastTwo = calls.slice(-2);
  for (const [index, call] of lastTwo.entries()) {
    const request = requested[index];
    const dispatch = dispatched[dispatched.length - 2 + index];
    const actual = observed[observed.length - 2 + index];
    must(
      typeof request?.requestId === "string" &&
        dispatch === `gateway-dispatch tool=${request.tool} request=Text("${request.requestId}")` &&
        (actual === `quinn-peer-dispatch stream=${call.stream} operation=${call.operation}` ||
          actual?.startsWith(
            `quinn-peer-dispatch stream=${call.stream} operation=${call.operation} `,
          )) === true,
      "Local request did not match the corresponding remote stream",
    );
  }
  const localConnections = gateway.filter((entry) => entry.startsWith("quinn-gateway-connection "));
  const peerConnections = peer.filter((entry) => entry.startsWith("quinn-peer-connection "));
  const [, local, remote, stable] =
    /^quinn-gateway-connection local=Ok\((.*)\) remote=(\S+) stable=(\d+)$/.exec(
      localConnections[0] ?? "",
    ) ?? [];
  must(
    localConnections.length === 1 &&
      peerConnections.length === 1 &&
      !!local &&
      !!remote &&
      calls.every((call) => call.stable === stable) &&
      (peerConnections[0]?.startsWith(
        `quinn-peer-connection local=Ok(${remote}) remote=${local} stable=`,
      ) ??
        false),
    "Proof endpoints did not pair one connection with no reconnects",
  );
  const lease = peer.filter((entry) => entry.startsWith("quinn-peer-source-open "));
  const [, leaseId] = /lease=([0-9]+) /.exec(lease.at(-1) ?? "") ?? [];
  const stream = lastTwo[1]?.stream;
  const requestId = requested[1]?.requestId;
  if (!leaseId || stream === undefined || typeof requestId !== "string")
    throw new Error("Missing observation lease");
  must(lease.length === ordinal, "Multiple source leases opened during an observation barrier");
  return { requestId, stream, lease: leaseId };
}
