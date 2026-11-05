import { Counter, Gauge, Histogram, Registry } from 'prom-client';
import type { Status } from '../domain/model';

export const registry = new Registry();

export const metrics = {
  transactions: new Counter({
    name: 'wager_transactions_total',
    help: 'Committed transaction outcomes',
    labelNames: ['status'],
    registers: [registry],
  }),
  duplicates: new Counter({
    name: 'wager_duplicates_total',
    help: 'Persistent deduplications',
    registers: [registry],
  }),
  retries: new Counter({
    name: 'wager_retries_total',
    help: 'Retry attempts',
    labelNames: ['component'],
    registers: [registry],
  }),
  dlq: new Counter({
    name: 'wager_dlq_total',
    help: 'Messages explicitly routed to DLQ',
    registers: [registry],
  }),
  locks: new Counter({
    name: 'wager_lock_conflicts_total',
    help: 'Deadlocks and lock timeouts',
    registers: [registry],
  }),
  lag: new Gauge({
    name: 'wager_outbox_lag_seconds',
    help: 'Age of oldest unpublished event',
    registers: [registry],
  }),
  latency: new Histogram({
    name: 'wager_processing_seconds',
    help: 'Processing latency',
    registers: [registry],
  }),
  divergence: new Counter({
    name: 'wager_reconciliation_divergences_total',
    help: 'Reconciliation mismatches',
    registers: [registry],
  }),
};

// Labelled series only appear once used, which breaks alerts that expect them from the start.
const STATUSES: Status[] = ['PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED'];
for (const status of STATUSES) metrics.transactions.inc({ status }, 0);
for (const component of ['consumer', 'outbox', 'reference']) {
  metrics.retries.inc({ component }, 0);
}

export interface LogFields {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
  code?: string;
  [key: string]: unknown;
}

export function log(event: string, fields: LogFields = {}): void {
  console.log(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      event,
      correlationId: null,
      messageId: null,
      transactionId: null,
      walletId: null,
      providerId: null,
      ...fields,
    }),
  );
}

/**
 * SQLSTATE or driver error code. Its absence means the failure did not come from an
 * external dependency, which is what separates a retryable outage from a bug.
 */
export function errorCode(error: unknown): string | undefined {
  const candidate = error as { code?: string; driverError?: { code?: string } } | null;
  return candidate?.driverError?.code ?? candidate?.code;
}
