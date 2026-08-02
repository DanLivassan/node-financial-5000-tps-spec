import { z } from "zod";

export const transactionRequestSchema = z.object({
  externalReference: z.string().trim().min(1).max(128),
  sourceAccountId: z.string().uuid(),
  destinationAccountId: z.string().uuid(),
  amountMinor: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  currency: z.string().trim().length(3).transform((value) => value.toUpperCase()),
  endToEndId: z.string().trim().min(1).max(128).optional(),
  providerTransactionId: z.string().trim().min(1).max(128).optional(),
}).strict();

export type TransactionRequest = z.infer<typeof transactionRequestSchema>;

export interface TransactionResponse {
  id: string;
  journalEntryId: string;
  externalReference: string;
  status: "accepted";
  amountMinor: number;
  currency: string;
  createdAt: string;
}
