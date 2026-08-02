export type Direction = "debit" | "credit";
export type AccountType = "asset" | "liability" | "equity" | "revenue" | "expense";

export interface PostingInput {
  accountId: string;
  direction: Direction;
  amountMinor: bigint;
  currency: string;
  sequence: number;
}

export function assertMoney(amountMinor: bigint, currency: string): void {
  if (amountMinor <= 0n) throw new Error("amountMinor must be greater than zero");
  if (!/^[A-Z]{3}$/.test(currency)) throw new Error("currency must be an ISO-4217 uppercase code");
}

export function transferPostings(input: {
  sourceAccountId: string;
  destinationAccountId: string;
  amountMinor: bigint;
  currency: string;
  sourceDirection?: Direction;
}): PostingInput[] {
  assertMoney(input.amountMinor, input.currency);
  if (input.sourceAccountId === input.destinationAccountId) throw new Error("accounts must differ");
  const sourceDirection = input.sourceDirection ?? "debit";
  const destinationDirection: Direction = sourceDirection === "debit" ? "credit" : "debit";
  return [
    { accountId: input.sourceAccountId, direction: sourceDirection, amountMinor: input.amountMinor, currency: input.currency, sequence: 1 },
    { accountId: input.destinationAccountId, direction: destinationDirection, amountMinor: input.amountMinor, currency: input.currency, sequence: 2 },
  ];
}

export function assertBalanced(postings: readonly PostingInput[]): void {
  const totals = new Map<string, { debit: bigint; credit: bigint }>();
  for (const posting of postings) {
    assertMoney(posting.amountMinor, posting.currency);
    const total = totals.get(posting.currency) ?? { debit: 0n, credit: 0n };
    total[posting.direction] += posting.amountMinor;
    totals.set(posting.currency, total);
  }
  if (totals.size === 0 || [...totals.values()].some((x) => x.debit !== x.credit)) {
    throw new Error("journal entry is not balanced per currency");
  }
}

export function signedBalanceDelta(accountType: AccountType, direction: Direction, amount: bigint): bigint {
  const debitNormal = accountType === "asset" || accountType === "expense";
  return (direction === "debit") === debitNormal ? amount : -amount;
}

export function deterministicAccountOrder(accountIds: readonly string[]): string[] {
  return [...new Set(accountIds)].sort((a, b) => a.localeCompare(b));
}
