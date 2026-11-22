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
    checkHistoryReceipts([history, wrong], "matching", [
      { epoch: "1", sequence: "9007199254740993" },
    ]),
  ).not.toThrow();
  expect(() =>
    checkHistoryReceipts([wrong], "matching", [{ epoch: "1", sequence: "9007199254740993" }]),
  ).toThrow();
  expect(() =>
    checkHistoryReceipts([history, history], "matching", [
      { epoch: "1", sequence: "9007199254740993" },
    ]),
  ).toThrow();
  expect(() =>
    checkHistoryReceipts([{ ...history, text: "unexpected" }], "matching", [
      { epoch: "1", sequence: "9007199254740993" },
    ]),
  ).toThrow();
});

test("history evidence rejects the wrong epoch even with an exact sequence and request", (): void => {
  const history = {
    kind: "gateway-frame-consumed",
    requestId: "matching",
    frame: "history",
    pid: 7,
    at: 1,
    epoch: "1",
    sequence: "9007199254740993",
  };
  expect(() =>
    checkHistoryReceipts([history], "matching", [{ epoch: "1", sequence: "9007199254740993" }]),
  ).not.toThrow();
  expect(() =>
    checkHistoryReceipts([{ ...history, epoch: "42" }], "matching", [
      { epoch: "1", sequence: "9007199254740993" },
    ]),
  ).toThrow();
});

test("history evidence compares ordered decimal cursor pairs without normalization", (): void => {
  const first = {
    kind: "gateway-frame-consumed",
    requestId: "matching",
    frame: "history",
    pid: 7,
    at: 1,
    epoch: "1",
    sequence: "9007199254740992",
  };
  const second = { ...first, at: 2, sequence: "9007199254740993" };
  const expected = [
    { epoch: "1", sequence: "9007199254740992" },
    { epoch: "1", sequence: "9007199254740993" },
  ];
  expect(() => checkHistoryReceipts([first, second], "matching", expected)).not.toThrow();
  expect(() => checkHistoryReceipts([second, first], "matching", expected)).toThrow();
  expect(() =>
    checkHistoryReceipts([{ ...first, epoch: "01" }, second], "matching", expected),
  ).toThrow();
});
