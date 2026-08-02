export interface FinancialEventEnvelope {
  eventId: string;
  eventType: string;
  eventVersion: number;
  aggregateId: string;
  partitionKey: string;
  payload: Record<string, unknown>;
  headers: Record<string, string>;
}
