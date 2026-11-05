import { randomUUID } from 'node:crypto';
import type { Money, MoneyProps } from './money';
import type { Direction, Kind, Status, WagerTransaction, Wallet, WalletLedgerEntry } from './model';

export interface EventContext {
  aggregateId: string;
  correlationId: string;
  causationId?: string;
}

export interface EventEnvelope<T> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string;
  version: number;
  data: T;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;

  readonly eventId = randomUUID();
  readonly occurredAt = new Date();
  readonly data: Readonly<T>;

  protected constructor(readonly context: EventContext, data: T) {
    this.data = Object.freeze(data);
  }

  toJSON(): EventEnvelope<T> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.context.aggregateId,
      correlationId: this.context.correlationId,
      ...(this.context.causationId ? { causationId: this.context.causationId } : {}),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data,
    };
  }
}

export interface WagerTransactionData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: Kind;
  status: Status;
  money: MoneyProps;
  balance: MoneyProps;
  referenceTransactionId?: string;
  failureCode?: string;
}

function dataOf(transaction: WagerTransaction, balance: Money): WagerTransactionData {
  return {
    transactionId: transaction.id,
    providerId: transaction.providerId,
    externalTransactionId: transaction.externalTransactionId,
    walletId: transaction.walletId,
    playerId: transaction.playerId,
    roundId: transaction.roundId,
    gameId: transaction.gameId,
    kind: transaction.kind,
    status: transaction.status,
    money: transaction.money.toJSON(),
    balance: balance.toJSON(),
    ...(transaction.referenceTransactionId
      ? { referenceTransactionId: transaction.referenceTransactionId }
      : {}),
    ...(transaction.failureCode ? { failureCode: transaction.failureCode } : {}),
  };
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  static from(transaction: WagerTransaction, balance: Money, context: EventContext) {
    return new WagerTransactionProcessed(context, dataOf(transaction, balance));
  }
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  static from(transaction: WagerTransaction, balance: Money, context: EventContext) {
    return new WagerTransactionRejected(context, dataOf(transaction, balance));
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  static from(transaction: WagerTransaction, balance: Money, context: EventContext) {
    return new WagerTransactionPendingReference(context, dataOf(transaction, balance));
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: Direction;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  static from(wallet: Wallet, entry: WalletLedgerEntry, context: EventContext) {
    return new WalletBalanceChanged(context, {
      walletId: entry.walletId,
      transactionId: entry.transactionId,
      direction: entry.direction,
      money: entry.money.toJSON(),
      balanceBefore: entry.balanceBefore.toJSON(),
      balanceAfter: entry.balanceAfter.toJSON(),
      walletVersion: wallet.version,
    });
  }
}
