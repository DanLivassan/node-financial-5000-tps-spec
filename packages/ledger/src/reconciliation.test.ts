import { describe, expect, it } from "vitest";
import { closingBalance, matchMovements, type Movement } from "./reconciliation.js";

const now = new Date("2026-01-01T12:00:00Z");
const movement = (overrides: Partial<Movement> = {}): Movement => ({
  id: "internal-1", endToEndId: "e2e-1", providerTransactionId: "provider-1", externalReference: "order-1",
  direction: "credit", amountMinor: 100n, currency: "BRL", occurredAt: now, ...overrides,
});

describe("reconciliation", () => {
  it("prioritizes strong identifiers and validates secondary evidence", () => {
    const [item] = matchMovements([movement()], [movement({ id: "bank-1", providerTransactionId: "other" })]);
    expect(item).toMatchObject({ status: "matched", matchKey: "end_to_end:e2e-1" });
  });

  it("detects amount and direction mismatches", () => {
    expect(matchMovements([movement()], [movement({ id: "bank-1", amountMinor: 101n })])[0]?.status).toBe("amount_mismatch");
    expect(matchMovements([movement()], [movement({ id: "bank-1", direction: "debit" })])[0]?.status).toBe("direction_mismatch");
  });

  it("never automatically matches solely by amount", () => {
    const noIds = { endToEndId: null, providerTransactionId: null, externalReference: null };
    expect(matchMovements([movement(noIds)], [movement({ id: "bank-1", ...noIds })])[0]?.status).toBe("manual_review");
  });

  it("detects duplicate bank identifiers and missing internal movements", () => {
    const result = matchMovements([movement()], [movement({ id: "bank-1" }), movement({ id: "bank-2" })]);
    expect(result.map((x) => x.status)).toEqual(["matched", "duplicate_bank_entry"]);
  });

  it("validates opening + credits - debits = closing", () => {
    expect(closingBalance(1000n, [movement({ amountMinor: 50n }), movement({ direction: "debit", amountMinor: 20n })])).toBe(1030n);
  });
});
