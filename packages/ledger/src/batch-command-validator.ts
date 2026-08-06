import { randomUUID } from "node:crypto";
import type { TransactionResponse } from "../../contracts/src/transactions.js";
import { requestHash } from "./idempotency.js";
import { DomainError } from "./transaction-service.js";
import type { BatchTransactionCommand, BatchTransactionOutcome } from "./transaction-batch-service.js";
import type { AccountRow } from "./account-repository.js";

export interface PreparedCommand {
  index: number;
  id: string;
  journalEntryId: string;
  eventId: string;
  createdAt: string;
  hash: string;
  response: TransactionResponse;
  command: BatchTransactionCommand;
}

export interface ValidatedBatch {
  outcomes: Array<BatchTransactionOutcome | undefined>;
  prepared: PreparedCommand[];
}

function rejection(code: string, status: number, message: string): BatchTransactionOutcome {
  return { ok: false, error: new DomainError(code, status, message) };
}

function validateCommand(command: BatchTransactionCommand): BatchTransactionOutcome | undefined {
  if (!command.idempotencyKey || command.idempotencyKey.length > 128) {
    return rejection("INVALID_IDEMPOTENCY_KEY", 400,
      "Idempotency-Key is required and must have at most 128 characters");
  }
  if (command.request.sourceAccountId === command.request.destinationAccountId) {
    return rejection("INVALID_ACCOUNT_PAIR", 422, "source and destination accounts must differ");
  }
  return undefined;
}

function prepareCommand(command: BatchTransactionCommand, index: number): PreparedCommand {
  const id = randomUUID();
  const journalEntryId = randomUUID();
  const createdAt = new Date().toISOString();
  return {
    index, id, journalEntryId, eventId: randomUUID(), createdAt,
    hash: requestHash(command.request), command,
    response: {
      id, journalEntryId, externalReference: command.request.externalReference,
      status: "accepted", amountMinor: command.request.amountMinor,
      currency: command.request.currency, createdAt,
    },
  };
}

export function validateUniqueIdempotencyKeys(commands: BatchTransactionCommand[]): void {
  const keys = new Set<string>();
  for (const command of commands) {
    if (keys.has(command.idempotencyKey)) throw new Error("batch commands must have unique idempotency keys");
    keys.add(command.idempotencyKey);
  }
}

export function validateAndPrepareCommands(commands: BatchTransactionCommand[]): ValidatedBatch {
  const outcomes = new Array<BatchTransactionOutcome | undefined>(commands.length);
  const prepared: PreparedCommand[] = [];
  for (const [index, command] of commands.entries()) {
    const invalid = validateCommand(command);
    if (invalid) outcomes[index] = invalid;
    else prepared.push(prepareCommand(command, index));
  }
  return { outcomes, prepared };
}

export function validateAccounts(
  items: PreparedCommand[], accounts: Map<string, AccountRow>,
  outcomes: Array<BatchTransactionOutcome | undefined>,
): PreparedCommand[] {
  return items.filter((item) => {
    const request = item.command.request;
    const source = accounts.get(request.sourceAccountId);
    const destination = accounts.get(request.destinationAccountId);
    let error: BatchTransactionOutcome | undefined;
    if (!source || !destination) error = rejection("ACCOUNT_NOT_FOUND", 404,
      "source or destination account was not found");
    else if (source.status !== "active" || destination.status !== "active") {
      const blocked = source.status !== "active" ? source.id : destination.id;
      error = rejection("ACCOUNT_BLOCKED", 422, `account ${blocked} is blocked`);
    } else if (source.currency !== request.currency || destination.currency !== request.currency) {
      error = rejection("CURRENCY_MISMATCH", 422, "account currency differs from transaction currency");
    } else {
      const sourceDebitNormal = source.account_type === "asset" || source.account_type === "expense";
      const destinationDebitNormal = destination.account_type === "asset" || destination.account_type === "expense";
      if (sourceDebitNormal !== destinationDebitNormal) error = rejection("ACCOUNT_TYPE_MISMATCH", 422,
        "a two-posting transfer requires accounts with the same normal-balance side; use a multi-leg clearing workflow");
    }
    if (!error) return true;
    outcomes[item.index] = error;
    return false;
  });
}
