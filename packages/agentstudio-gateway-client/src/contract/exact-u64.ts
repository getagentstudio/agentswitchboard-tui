import { Schema } from "#gateway/effect-runtime";

const maximumU64 = 18_446_744_073_709_551_615n;
const decimal = /^(0|[1-9][0-9]*)$/;
const numericFields = new Set(["epoch", "sequence", "afterSequence", "replayEpoch", "serverTsMs"]);

export const exactU64 = Schema.BigInt.check(
  Schema.isGreaterThanOrEqualToBigInt(0n),
  Schema.isLessThanOrEqualToBigInt(maximumU64),
);

export function decodeDecimalCursor(source: string): bigint {
  if (!decimal.test(source) || source.length > 20) throw new Error("Invalid decimal u64 cursor");
  return Schema.decodeUnknownSync(exactU64)(BigInt(source));
}

export function parseWireJson(source: string): unknown {
  return JSON.parse(
    source,
    (key: string, value: unknown, context?: { readonly source?: string }): unknown => {
      if (typeof value !== "number" || !numericFields.has(key)) return value;
      if (context?.source === undefined) throw new Error("Numeric cursor source is unavailable");
      return decodeDecimalCursor(context.source);
    },
  );
}
