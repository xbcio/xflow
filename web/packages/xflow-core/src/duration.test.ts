import { describe, expect, it } from "vitest";
import { formatNodeTimeout } from "./duration";

describe("formatNodeTimeout", () => {
  it.each([
    [1e9, "1s"],
    [30e9, "30s"],
    [90e9, "90s"],
    [60e9, "1m"],
    [3.6e12, "1h"],
    [1.5e9, "1500000000ns"],
    [1, "1ns"],
    [3600e9 + 1, "3600000000001ns"]
  ])("formats %d ns as %s", (ns, expected) => {
    expect(formatNodeTimeout(ns)).toBe(expected);
  });
});
