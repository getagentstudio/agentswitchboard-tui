import { expect, test } from "bun:test";
import { checkHistoryReceipts } from "./fixture-receipts.ts";

test("history evidence is scoped to one request and preserves exact cursor strings", (): void => {
  const history = {
    kind: "gateway-frame-consumed",
    requestId: "matching",
    frame: "history",
    pid: 7,
    at: 1,
    epoch: "1",
    sequence: "9007199254740993",
  };
  const wrong = { ...history, requestId: "different" };
  expect(() =>
    checkHistoryReceipts([history, wrong], "matching", ["9007199254740993"]),
  ).not.toThrow();
  expect(() => checkHistoryReceipts([wrong], "matching", ["9007199254740993"])).toThrow();
  expect(() =>
    checkHistoryReceipts([history, history], "matching", ["9007199254740993"]),
  ).toThrow();
  expect(() =>
    checkHistoryReceipts([{ ...history, text: "unexpected" }], "matching", ["9007199254740993"]),
  ).toThrow();
});
