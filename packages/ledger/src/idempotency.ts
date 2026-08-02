import { createHash } from "node:crypto";
import type { TransactionRequest } from "../../contracts/src/transactions.js";

export function canonicalTransactionRequest(request: TransactionRequest): string {
  return JSON.stringify({
    amountMinor: request.amountMinor,
    currency: request.currency.toUpperCase(),
    destinationAccountId: request.destinationAccountId.toLowerCase(),
    endToEndId: request.endToEndId ?? null,
    externalReference: request.externalReference.trim(),
    providerTransactionId: request.providerTransactionId ?? null,
    sourceAccountId: request.sourceAccountId.toLowerCase(),
  });
}

export function requestHash(request: TransactionRequest): string {
  return createHash("sha256").update(canonicalTransactionRequest(request)).digest("hex");
}
