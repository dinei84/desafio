import { createHash } from 'node:crypto';
import { DomainError, Money, type MoneyProps } from '../domain/money';
import type { Kind } from '../domain/model';

const KINDS: WagerInput['kind'][] = ['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'];
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MAX_FIELD_LENGTH = 200;

export class ApplicationError extends Error {
  constructor(
    readonly code: string,
    readonly httpStatus: number,
    /** Whether a queued message carrying this error is worth delivering again. */
    readonly retryable = false,
  ) {
    super(code);
  }
}

export interface WagerInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: Exclude<Kind, 'OPENING'>;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new ApplicationError('INVALID_PAYLOAD', 400);
  }
  return value as Record<string, unknown>;
}

export function textField(value: unknown, field: string): string {
  if (typeof value !== 'string' || !value.trim() || value.length > MAX_FIELD_LENGTH) {
    throw new ApplicationError(`INVALID_${field}`, 400);
  }
  return value;
}

export function uuid(value: unknown): string {
  const id = textField(value, 'ID');
  if (!UUID.test(id)) throw new ApplicationError('INVALID_UUID', 400);
  return id.toLowerCase();
}

export function parseMoney(value: unknown): Money {
  const props = record(value);
  return Money.from({ amount: props.amount as string, currency: props.currency as string });
}

export function parseWager(value: unknown): WagerInput {
  const payload = record(value);
  const kind = payload.kind as WagerInput['kind'];
  if (!KINDS.includes(kind)) throw new ApplicationError('INVALID_KIND', 400);

  const input: WagerInput = {
    providerId: textField(payload.providerId, 'PROVIDER_ID'),
    externalTransactionId: textField(payload.externalTransactionId, 'EXTERNAL_TRANSACTION_ID'),
    playerId: uuid(payload.playerId),
    walletId: uuid(payload.walletId),
    roundId: textField(payload.roundId, 'ROUND_ID'),
    gameId: textField(payload.gameId, 'GAME_ID'),
    kind,
    money: parseMoney(payload.money).toJSON(),
  };

  if (payload.referenceExternalTransactionId !== undefined) {
    input.referenceExternalTransactionId = textField(
      payload.referenceExternalTransactionId,
      'REFERENCE',
    );
  }
  if (['REFUND', 'ROLLBACK'].includes(kind) && !input.referenceExternalTransactionId) {
    throw new DomainError('REFERENCE_REQUIRED');
  }
  return input;
}

/** Recursive canonical JSON: keys sorted, undefined dropped, so hashing is order independent. */
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    const props = value as Record<string, unknown>;
    const entries = Object.keys(props)
      .filter(key => props[key] !== undefined)
      .sort()
      .map(key => `${JSON.stringify(key)}:${canonical(props[key])}`);
    return `{${entries.join(',')}}`;
  }
  return JSON.stringify(value);
}

export function hash(value: unknown): string {
  return createHash('sha256').update(canonical(value)).digest('hex');
}

export function assertReplay(storedHash: string, incomingHash: string): void {
  if (storedHash !== incomingHash) throw new ApplicationError('IDEMPOTENCY_CONFLICT', 409);
}
