import { describe, expect, it } from "vitest";
import { retryDelayMs } from "./outbox.js";

describe("outbox retry", () => {
  it("uses capped exponential backoff and bounded jitter", () => {
    expect(retryDelayMs(1, 0)).toBe(100);
    expect(retryDelayMs(2, 0)).toBe(200);
    expect(retryDelayMs(20, 1)).toBeLessThanOrEqual(60_249);
  });
});
