import { describe, expect, test } from 'bun:test';
import { Money } from '../src/domain/money';
import { WagerTransaction, Wallet, WalletLedgerEntry, type Kind } from '../src/domain/model';
import { assertReplay, hash, parseWager } from '../src/application/contracts';
import { WagerTransactionProcessed, WalletBalanceChanged } from '../src/domain/events';
import { InboxMessage, OutboxMessage } from '../src/domain/messaging';

const money = (amount: string, currency = 'BRL') => Money.from({ amount, currency });

function transaction(kind: Kind, amount = '10.00') {
  return WagerTransaction.create({
    id: 'transaction-1',
    providerId: 'provider-a',
    externalTransactionId: 'external-1',
    idempotencyKey: 'provider-a:external-1',
    payloadHash: 'hash',
    walletId: 'wallet-1',
    playerId: 'player-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind,
    money: money(amount),
    ...(['REFUND', 'ROLLBACK'].includes(kind)
      ? { referenceExternalTransactionId: 'external-0' }
      : {}),
  });
}

describe('Money', () => {
  test('exact arithmetic, fixed scale, immutability and large values', () => {
    const cents = money('0.1');
    expect(cents.add(money('0.20')).toString()).toBe('0.30');
    expect(cents.toString()).toBe('0.10');

    expect(money('9999999999999999.99').subtract(money('0.01')).toString()).toBe(
      '9999999999999999.98',
    );
    expect(money('1').negate().toJSON()).toEqual({ amount: '-1.00', currency: 'BRL' });
    expect(Money.rehydrate({ amount: '-1.00', currency: 'BRL' }).isNegative()).toBe(true);

    expect(money('0').isZero()).toBe(true);
    expect(money('2').isPositive()).toBe(true);
    expect(money('999.99').scale).toBe(3);
    expect(money('9999999999999999.99').scale).toBe(16);
  });

  const invalid = ['', 'NaN', 'Infinity', '1e2', '-1', '1.234', ' 1', '1.', '.10', '01.00', '+1'];
  for (const amount of [...invalid, '10000000000000000', 1, null]) {
    test(`rejects ${JSON.stringify(amount)} without rounding`, () => {
      expect(() => money(amount as string)).toThrow();
    });
  }

  test('currency conflict', () => {
    for (const operation of ['add', 'subtract', 'equals', 'isLessThan'] as const) {
      expect(() => money('1')[operation](money('1', 'USD'))).toThrow('CURRENCY_MISMATCH');
    }
    expect(() => money('1', 'brl')).toThrow();
  });
});

describe('Wallet and ledger', () => {
  test('balance, version and insufficient funds', () => {
    const wallet = Wallet.open({ id: 'wallet-1', playerId: 'player-1', initialBalance: money('100') });

    wallet.debit(money('80'));
    expect(wallet.balance.toString()).toBe('20.00');
    expect(wallet.version).toBe(2);

    expect(() => wallet.debit(money('80'))).toThrow('INSUFFICIENT_FUNDS');
    expect(wallet.version).toBe(2);

    wallet.credit(money('0'));
    expect(wallet.version).toBe(2);
    wallet.credit(money('1'));
    expect(wallet.version).toBe(3);

    expect(() => wallet.credit(money('1', 'USD'))).toThrow('CURRENCY_MISMATCH');
    expect(() => wallet.debit(money('30'), 'REVERSAL_INSUFFICIENT_FUNDS')).toThrow(
      'REVERSAL_INSUFFICIENT_FUNDS',
    );
    expect(() => wallet.credit(money('1').negate())).toThrow();
  });

  test('ledger arithmetic and structural immutability', () => {
    const props = {
      id: 'entry-1',
      walletId: 'wallet-1',
      transactionId: 'transaction-1',
      direction: 'DEBIT' as const,
      money: money('20'),
      balanceBefore: money('100'),
      balanceAfter: money('80'),
      createdAt: new Date(),
    };

    const entry = WalletLedgerEntry.create(props);
    expect(entry.isBalanced()).toBe(true);
    expect(Object.isFrozen(entry)).toBe(true);

    expect(() => WalletLedgerEntry.create({ ...props, balanceAfter: money('81') })).toThrow();
    expect(() => WalletLedgerEntry.create({ ...props, money: money('0') })).toThrow();
  });
});

describe('Wager transaction', () => {
  test('directions and zero effect LOSS', () => {
    expect(transaction('BET').ledgerDirectionFor()).toBe('DEBIT');
    expect(transaction('WIN').ledgerDirectionFor()).toBe('CREDIT');
    expect(transaction('REFUND').ledgerDirectionFor()).toBe('CREDIT');
    expect(transaction('LOSS').affectsBalance()).toBe(false);
    expect(transaction('BET', '0.00').affectsBalance()).toBe(false);

    expect(transaction('ROLLBACK').ledgerDirectionFor(transaction('BET'))).toBe('CREDIT');
    expect(transaction('ROLLBACK').ledgerDirectionFor(transaction('WIN'))).toBe('DEBIT');
    expect(transaction('ROLLBACK').ledgerDirectionFor(transaction('REFUND'))).toBe('DEBIT');
    expect(() => transaction('ROLLBACK').ledgerDirectionFor()).toThrow('REFERENCE_REQUIRED');
    expect(() => transaction('LOSS').ledgerDirectionFor()).toThrow('NO_LEDGER_DIRECTION');
  });

  test('reversals require a reference', () => {
    for (const kind of ['REFUND', 'ROLLBACK'] as const) {
      expect(transaction(kind).requiresReference()).toBe(true);
    }
    for (const kind of ['BET', 'WIN', 'LOSS'] as const) {
      expect(transaction(kind).requiresReference()).toBe(false);
    }
  });

  test('explicit transitions and terminal protection', () => {
    const pending = transaction('BET');
    pending.markPendingReference();
    pending.markProcessed(undefined, new Date());

    const blocked = [
      () => pending.markProcessed(undefined, new Date()),
      () => pending.reject('X'),
      () => pending.fail('X'),
      () => pending.markPendingReference(),
    ];
    for (const attempt of blocked) expect(attempt).toThrow('INVALID_TRANSACTION_STATE');

    const rejected = transaction('BET');
    rejected.reject('INSUFFICIENT_FUNDS');
    expect(rejected.isTerminal()).toBe(true);
    expect(rejected.failureCode).toBe('INSUFFICIENT_FUNDS');

    const failed = transaction('BET');
    failed.fail('PERMANENT');
    expect(failed.isTerminal()).toBe(true);
  });

  test('canonical hashing and conflicting payload', () => {
    expect(hash({ b: 2, a: { z: 1, x: 3 } })).toBe(hash({ a: { x: 3, z: 1 }, b: 2 }));
    expect(() => assertReplay(hash({ amount: '1' }), hash({ amount: '2' }))).toThrow(
      'IDEMPOTENCY_CONFLICT',
    );
    expect(() => parseWager({ kind: 'OPENING' })).toThrow('INVALID_KIND');
    expect(() => parseWager({ kind: 'BET', money: { amount: 1, currency: 'BRL' } })).toThrow();
  });

  test('matchesPayload detects a divergent body behind the same key', () => {
    const stored = transaction('BET');
    expect(stored.matchesPayload('hash')).toBe(true);
    expect(stored.matchesPayload('other')).toBe(false);
  });
});

describe('Integration events', () => {
  const context = { aggregateId: 'wallet-1', correlationId: 'correlation-1' };

  test('a transaction event carries enough identity for a downstream consumer', () => {
    const processed = transaction('BET');
    processed.markProcessed(undefined, new Date());

    const event = WagerTransactionProcessed.from(processed, money('90'), context);
    const envelope = event.toJSON();

    expect(envelope.eventType).toBe('WagerTransactionProcessed');
    expect(envelope.version).toBe(1);
    expect(envelope.aggregateId).toBe('wallet-1');
    expect(envelope.occurredAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(envelope.data).toMatchObject({
      transactionId: 'transaction-1',
      providerId: 'provider-a',
      externalTransactionId: 'external-1',
      walletId: 'wallet-1',
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'game-1',
      kind: 'BET',
      status: 'PROCESSED',
      money: { amount: '10.00', currency: 'BRL' },
      balance: { amount: '90.00', currency: 'BRL' },
    });
  });

  test('balance events report money as decimal strings and the wallet version', () => {
    const wallet = Wallet.open({ id: 'wallet-1', playerId: 'player-1', initialBalance: money('100') });
    wallet.debit(money('10'));

    const entry = WalletLedgerEntry.create({
      id: 'entry-1',
      walletId: wallet.id,
      transactionId: 'transaction-1',
      direction: 'DEBIT',
      money: money('10'),
      balanceBefore: money('100'),
      balanceAfter: money('90'),
      createdAt: new Date(),
    });

    const event = WalletBalanceChanged.from(wallet, entry, context);
    expect(event.data).toEqual({
      walletId: 'wallet-1',
      transactionId: 'transaction-1',
      direction: 'DEBIT',
      money: { amount: '10.00', currency: 'BRL' },
      balanceBefore: { amount: '100.00', currency: 'BRL' },
      balanceAfter: { amount: '90.00', currency: 'BRL' },
      walletVersion: 2,
    });
    expect(Object.isFrozen(event.data)).toBe(true);
  });
});

test('inbox and outbox retry lifecycle', () => {
  const inbox = InboxMessage.receive({
    consumerName: 'wager',
    messageId: 'message-1',
    payloadHash: 'hash',
  });
  expect(inbox.isProcessed()).toBe(false);
  inbox.markProcessed(new Date());
  expect(() => inbox.markProcessed(new Date())).toThrow();

  const wallet = Wallet.open({ id: 'wallet-1', playerId: 'player-1', initialBalance: money('100') });
  const entry = WalletLedgerEntry.create({
    id: 'entry-1',
    walletId: wallet.id,
    transactionId: 'transaction-1',
    direction: 'CREDIT',
    money: money('100'),
    balanceBefore: money('0'),
    balanceAfter: money('100'),
    createdAt: new Date(),
  });

  const outbox = OutboxMessage.enqueue(
    WalletBalanceChanged.from(wallet, entry, {
      aggregateId: wallet.id,
      correlationId: 'correlation-1',
    }),
  );
  expect(outbox.eventType).toBe('WalletBalanceChanged');

  const now = new Date();
  outbox.scheduleRetry(now);
  expect(outbox.attempts).toBe(1);
  expect(outbox.isDue(now)).toBe(false);
  expect(outbox.isDue(new Date(now.getTime() + 2000))).toBe(true);

  outbox.markPublished(now);
  expect(outbox.isPending()).toBe(false);
});
