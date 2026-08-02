import { describe, expect, it } from "vitest";
import fc from "fast-check";
import { assertBalanced, deterministicAccountOrder, signedBalanceDelta, transferPostings } from "./domain.js";

describe("ledger domain", () => {
  it("generates a balanced transfer", () => {
    const postings = transferPostings({ sourceAccountId: "a", destinationAccountId: "b", amountMinor: 12990n, currency: "BRL" });
    expect(() => assertBalanced(postings)).not.toThrow();
    expect(postings.map((p) => p.direction)).toEqual(["debit", "credit"]);
  });

  it("rejects an unbalanced journal", () => {
    expect(() => assertBalanced([
      { accountId: "a", direction: "debit", amountMinor: 2n, currency: "BRL", sequence: 1 },
      { accountId: "b", direction: "credit", amountMinor: 1n, currency: "BRL", sequence: 2 },
    ])).toThrow(/not balanced/);
  });

  it("uses debit-normal and credit-normal account semantics", () => {
    expect(signedBalanceDelta("asset", "debit", 10n)).toBe(10n);
    expect(signedBalanceDelta("liability", "credit", 10n)).toBe(10n);
  });

  it("credits an asset source and debits an asset destination", () => {
    const postings = transferPostings({ sourceAccountId: "a", destinationAccountId: "b", amountMinor: 10n,
      currency: "BRL", sourceDirection: "credit" });
    expect(postings.map((posting) => posting.direction)).toEqual(["credit", "debit"]);
    expect(() => assertBalanced(postings)).not.toThrow();
  });

  it("orders locks deterministically", () => {
    expect(deterministicAccountOrder(["b", "a", "b"])).toEqual(["a", "b"]);
  });

  it("is balanced for every generated positive transfer", () => {
    fc.assert(fc.property(fc.bigInt({ min: 1n, max: 9_000_000_000_000n }), (amountMinor) => {
      expect(() => assertBalanced(transferPostings({ sourceAccountId: "a", destinationAccountId: "b", amountMinor, currency: "BRL" }))).not.toThrow();
    }));
  });
});
