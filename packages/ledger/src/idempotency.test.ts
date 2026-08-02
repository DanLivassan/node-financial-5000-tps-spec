import { describe, expect, it } from "vitest";
import { canonicalTransactionRequest, requestHash } from "./idempotency.js";

const request = {
  externalReference: "order-1",
  sourceAccountId: "d9428888-122b-11e1-b85c-61cd3cbb3210",
  destinationAccountId: "a9428888-122b-11e1-b85c-61cd3cbb3210",
  amountMinor: 123,
  currency: "BRL",
};

describe("HTTP idempotency", () => {
  it("canonicalizes with a fixed field order", () => {
    expect(canonicalTransactionRequest(request)).toBe(
      '{"amountMinor":123,"currency":"BRL","destinationAccountId":"a9428888-122b-11e1-b85c-61cd3cbb3210","endToEndId":null,"externalReference":"order-1","providerTransactionId":null,"sourceAccountId":"d9428888-122b-11e1-b85c-61cd3cbb3210"}',
    );
  });

  it("hashes deterministically and changes with the financial payload", () => {
    expect(requestHash(request)).toHaveLength(64);
    expect(requestHash(request)).toBe(requestHash({ ...request }));
    expect(requestHash(request)).not.toBe(requestHash({ ...request, amountMinor: 124 }));
  });
});
