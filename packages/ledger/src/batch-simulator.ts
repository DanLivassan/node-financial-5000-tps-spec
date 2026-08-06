import type { AccountRow } from "./account-repository.js";
import type { PreparedCommand } from "./batch-command-validator.js";
import type { BalanceDelta } from "./balance-repository.js";
import { assertBalanced, signedBalanceDelta, transferPostings } from "./domain.js";
import { DomainError } from "./transaction-service.js";
import type { BatchTransactionOutcome } from "./transaction-batch-service.js";

export interface AcceptedCommand extends PreparedCommand {
  postings: ReturnType<typeof transferPostings>;
}

export interface SimulationResult {
  accepted: AcceptedCommand[];
  rejectedIds: string[];
  deltas: BalanceDelta[];
}

function buildPostings(item: PreparedCommand, source: AccountRow) {
  const request = item.command.request;
  const debitNormal = source.account_type === "asset" || source.account_type === "expense";
  const sourceDirection = debitNormal ? "credit" as const : "debit" as const;
  const postings = transferPostings({
    sourceAccountId: request.sourceAccountId, destinationAccountId: request.destinationAccountId,
    amountMinor: BigInt(request.amountMinor), currency: request.currency, sourceDirection,
  });
  assertBalanced(postings);
  return postings;
}

function aggregateDeltas(accepted: AcceptedCommand[], accounts: Map<string, AccountRow>): BalanceDelta[] {
  const deltas = new Map<string, BalanceDelta>();
  for (const item of accepted) for (const posting of item.postings) {
    const account = accounts.get(posting.accountId)!;
    const key = `${posting.accountId}:${posting.currency}`;
    const current = deltas.get(key) ?? {
      accountId: posting.accountId, currency: posting.currency, delta: 0n, versions: 0n,
    };
    current.delta += signedBalanceDelta(account.account_type, posting.direction, posting.amountMinor);
    current.versions += 1n;
    deltas.set(key, current);
  }
  return [...deltas.values()];
}

export function simulateCommands(
  items: PreparedCommand[], accounts: Map<string, AccountRow>, balances: Map<string, bigint>,
  outcomes: Array<BatchTransactionOutcome | undefined>,
): SimulationResult {
  const accepted: AcceptedCommand[] = [];
  const rejectedIds: string[] = [];
  for (const item of items) {
    const source = accounts.get(item.command.request.sourceAccountId)!;
    const postings = buildPostings(item, source);
    const sourcePosting = postings.find((posting) => posting.accountId === source.id)!;
    const sourceDelta = signedBalanceDelta(source.account_type, sourcePosting.direction, sourcePosting.amountMinor);
    if (!source.allow_negative && (balances.get(source.id) ?? 0n) + sourceDelta < 0n) {
      outcomes[item.index] = { ok: false, error: new DomainError("INSUFFICIENT_FUNDS", 422,
        "source account has insufficient funds") };
      rejectedIds.push(item.id);
      continue;
    }
    for (const posting of postings) {
      const account = accounts.get(posting.accountId)!;
      const delta = signedBalanceDelta(account.account_type, posting.direction, posting.amountMinor);
      balances.set(posting.accountId, (balances.get(posting.accountId) ?? 0n) + delta);
    }
    accepted.push({ ...item, postings });
  }
  return { accepted, rejectedIds, deltas: aggregateDeltas(accepted, accounts) };
}
