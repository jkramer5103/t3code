import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { CreateScheduledPromptInput } from "./scheduling.ts";

const decode = Schema.decodeSync(CreateScheduledPromptInput);

it("rejects impossible calendar dates for both exact starts and interval starts", () => {
  for (const at of [
    "2026-02-30T09:00:00Z",
    "2025-02-29T09:00:00Z",
    "2100-02-29T09:00:00Z",
    "2026-04-31T09:00:00+02:00",
    "2026-00-01T09:00:00Z",
    "2026-01-00T09:00:00Z",
  ]) {
    expect(() => decode({ prompt: "Check", schedule: { kind: "once", at } })).toThrow();
    expect(() =>
      decode({ prompt: "Check", schedule: { kind: "interval", seconds: 60, startAt: at } }),
    ).toThrow();
  }
});

it("rejects invalid clocks, offsets, and timestamps without an offset", () => {
  for (const at of [
    "2026-10-01T24:00:00Z",
    "2026-10-01T09:60:00Z",
    "2026-10-01T09:00:60Z",
    "2026-10-01T09:00:00+24:00",
    "2026-10-01T09:00:00+02:60",
    "2026-10-01T09:00:00",
    "garbageTZ",
  ]) {
    expect(() => decode({ prompt: "Check", schedule: { kind: "once", at } })).toThrow();
  }
});

it("accepts leap days and explicit offsets without changing the input", () => {
  for (const at of [
    "2024-02-29T09:00:00Z",
    "2000-02-29T09:00:00Z",
    "2026-10-01T09:00:00.123+02:00",
    "2026-10-01T09:00:00-07:00",
  ]) {
    expect(decode({ prompt: "Check", schedule: { kind: "once", at } }).schedule).toEqual({
      kind: "once",
      at,
    });
  }
});
