import { randomUUID } from 'node:crypto';
import type { DataSource, EntityManager } from 'typeorm';

import { DomainError, Money } from '../domain/money';
import {
  WagerTransaction,
  Wallet,
  WalletLedgerEntry,
  type Kind,
  type Status,
} from '../domain/model';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
  type IntegrationEvent,
} from '../domain/events';
import { InboxMessage, OutboxMessage } from '../domain/messaging';
import {
  ApplicationError,
  assertReplay,
  hash,
  parseMoney,
  parseWager,
  record,
  textField,
  uuid,
  type WagerInput,
} from './contracts';
import { WalletSchema, type WalletRow } from '../infrastructure/database';
import { errorCode, log, metrics } from '../infrastructure/observability';

const DEFAULT_CONSUMER = 'wager';
const REFERENCE_ATTEMPT_LIMIT = 12;
const REFERENCE_BATCH = 25;
const MAX_BACKOFF_SECONDS = 300;
const MAX_INTEGER_DIGITS = 16;
const DEFAULT_PAGE = 50;
const MAX_PAGE = 100;
const UNRESOLVED: Status[] = ['PENDING', 'PENDING_REFERENCE'];
const LOCK_CONFLICTS = ['40P01', '55P03', '57014'];

export interface TransactionRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: Kind;
  amount: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  status: Status;
  failure_code: string | null;
  response: Result;
  attempts: number;
  created_at: Date;
  processed_at: Date | null;
}

export interface Result {
  transactionId: string;
  status: Status;
  balance: { amount: string; currency: string };
  failureCode?: string;
  idempotentReplay: boolean;
}

export interface Context {
  correlationId: string;
  messageId?: string;
  consumerName?: string;
  inboxHash?: string;
}

function walletFrom(row: WalletRow): Wallet {
  return Wallet.rehydrate({
    id: row.id,
    playerId: row.player_id,
    balance: Money.rehydrate({ amount: row.balance, currency: row.currency }),
    version: row.version,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  });
}

function transactionFrom(row: TransactionRow): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: row.id,
    providerId: row.provider_id,
    externalTransactionId: row.external_transaction_id,
    idempotencyKey: row.idempotency_key,
    payloadHash: row.payload_hash,
    walletId: row.wallet_id,
    playerId: row.player_id,
    roundId: row.round_id,
    gameId: row.game_id,
    kind: row.kind,
    money: Money.rehydrate({ amount: row.amount, currency: row.currency }),
    referenceExternalTransactionId: row.reference_external_transaction_id ?? undefined,
    status: row.status,
    createdAt: row.created_at,
    referenceTransactionId: row.reference_transaction_id ?? undefined,
    failureCode: row.failure_code ?? undefined,
    processedAt: row.processed_at ?? undefined,
  });
}

export class Processor {
  constructor(readonly db: DataSource) {}

  async openWallet(value: unknown, correlationId: string) {
    const payload = record(value);
    const playerId = uuid(payload.playerId);
    const initialBalance = parseMoney(payload.initialBalance);
    const wallet = Wallet.open({ id: randomUUID(), playerId, initialBalance });

    try {
      return await this.db.transaction(async em => {
        // The row starts at zero; the opening ledger entry is what applies the initial balance.
        await em.query('INSERT INTO wallets(id,player_id,currency) VALUES($1,$2,$3)', [
          wallet.id,
          wallet.playerId,
          wallet.currency,
        ]);

        if (initialBalance.isPositive()) {
          await this.openWithBalance(em, wallet, initialBalance, correlationId);
        }

        return {
          id: wallet.id,
          playerId: wallet.playerId,
          balance: wallet.balance.toJSON(),
          version: wallet.version,
        };
      });
    } catch (error) {
      if (errorCode(error) === '23505') throw new ApplicationError('WALLET_ALREADY_EXISTS', 409);
      throw error;
    }
  }

  async submit(value: unknown, idempotencyKey: unknown, context: Context): Promise<Result> {
    const input = parseWager(value);
    const key = textField(idempotencyKey, 'IDEMPOTENCY_KEY');
    const payloadHash = hash(input);
    const stopTimer = metrics.latency.startTimer();

    try {
      const result = await this.db.transaction(async em => {
        const wallet = await this.lockWallet(em, input.walletId);

        const fromInbox = await this.replayFromInbox(em, context, payloadHash);
        if (fromInbox) return fromInbox;

        const created: TransactionRow[] = await em.query(
          `INSERT INTO wager_transactions
             (id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,
              player_id,round_id,game_id,kind,amount,currency,reference_external_transaction_id,status)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'PENDING')
           ON CONFLICT DO NOTHING RETURNING *`,
          [
            randomUUID(),
            input.providerId,
            input.externalTransactionId,
            key,
            payloadHash,
            input.walletId,
            input.playerId,
            input.roundId,
            input.gameId,
            input.kind,
            input.money.amount,
            input.money.currency,
            input.referenceExternalTransactionId ?? null,
          ],
        );

        const outcome = created.length
          ? await this.apply(em, created[0]!, wallet, context)
          : await this.replayFromTransaction(em, input, key, payloadHash);

        await this.recordInbox(em, context, payloadHash, outcome.transactionId);
        return outcome;
      });

      if (result.idempotentReplay) metrics.duplicates.inc();
      else metrics.transactions.inc({ status: result.status });

      log('transaction_committed', {
        correlationId: context.correlationId,
        messageId: context.messageId,
        transactionId: result.transactionId,
        walletId: input.walletId,
        providerId: input.providerId,
        status: result.status,
        failureCode: result.failureCode,
      });
      return result;
    } catch (error) {
      if (LOCK_CONFLICTS.includes(errorCode(error) ?? '')) metrics.locks.inc();
      throw error;
    } finally {
      stopTimer();
    }
  }

  async retryReferences(): Promise<number> {
    const due: TransactionRow[] = await this.db.query(
      `SELECT * FROM wager_transactions
        WHERE status='PENDING_REFERENCE' AND next_attempt_at<=now()
        ORDER BY next_attempt_at LIMIT ${REFERENCE_BATCH}`,
    );

    for (const candidate of due) {
      await this.db.transaction(async em => {
        const wallet = await this.lockWallet(em, candidate.wallet_id);
        const [current]: TransactionRow[] = await em.query(
          `SELECT * FROM wager_transactions
            WHERE id=$1 AND status='PENDING_REFERENCE' AND next_attempt_at<=now()
            FOR UPDATE`,
          [candidate.id],
        );
        if (!current) return;

        const result = await this.apply(em, current, wallet, { correlationId: current.id });
        metrics.retries.inc({ component: 'reference' });
        metrics.transactions.inc({ status: result.status });
      });
    }
    return due.length;
  }

  async wallet(id: string) {
    const row = await this.db.getRepository(WalletSchema).findOneBy({ id: uuid(id) });
    if (!row) throw new ApplicationError('WALLET_NOT_FOUND', 404, true);
    return {
      id: row.id,
      playerId: row.player_id,
      balance: { amount: row.balance, currency: row.currency },
      version: row.version,
    };
  }

  async transaction(id: string) {
    return this.lookup('id=$1', [uuid(id)]);
  }

  async providerTransaction(providerId: string, externalTransactionId: string) {
    return this.lookup('provider_id=$1 AND external_transaction_id=$2', [
      providerId,
      externalTransactionId,
    ]);
  }

  async ledger(id: string, cursor?: string, rawLimit?: string) {
    const walletId = uuid(id);
    await this.wallet(walletId);

    const limit = rawLimit === undefined ? DEFAULT_PAGE : Number(rawLimit);
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_PAGE) {
      throw new ApplicationError('INVALID_LIMIT', 400);
    }

    const after = cursor ? decodeCursor(cursor, walletId) : '0';
    const rows = await this.db.query(
      'SELECT * FROM wallet_ledger WHERE wallet_id=$1 AND sequence>$2 ORDER BY sequence LIMIT $3',
      [walletId, after, limit + 1],
    );

    const page = rows.slice(0, limit);
    const last = page.at(-1);
    return {
      entries: page.map((row: Record<string, unknown>) => ({
        id: row.id,
        transactionId: row.transaction_id,
        direction: row.direction,
        money: { amount: row.amount, currency: row.currency },
        balanceBefore: { amount: row.balance_before, currency: row.currency },
        balanceAfter: { amount: row.balance_after, currency: row.currency },
        createdAt: row.created_at,
      })),
      nextCursor: rows.length > limit && last ? encodeCursor(walletId, last.sequence) : null,
    };
  }

  async reconcile(id: string) {
    return this.db.transaction(async em => {
      const wallet = await this.lockWallet(em, uuid(id));
      const [totals] = await em.query(
        `SELECT COALESCE(sum(CASE WHEN direction='CREDIT' THEN amount ELSE -amount END),0)::numeric(18,2)::text AS balance,
                count(*)::integer AS count
           FROM wallet_ledger WHERE wallet_id=$1`,
        [wallet.id],
      );

      const calculated = Money.rehydrate({ amount: totals.balance, currency: wallet.currency });
      const difference = wallet.balance.subtract(calculated);
      const consistent = difference.isZero();

      if (!consistent) {
        metrics.divergence.inc();
        log('reconciliation_divergence', { walletId: wallet.id, correlationId: randomUUID() });
      }

      return {
        walletId: wallet.id,
        storedBalance: wallet.balance.toJSON(),
        calculatedBalance: calculated.toJSON(),
        difference: difference.toJSON(),
        consistent,
        checkedEntries: totals.count,
      };
    });
  }

  private async openWithBalance(
    em: EntityManager,
    wallet: Wallet,
    initialBalance: Money,
    correlationId: string,
  ) {
    const id = randomUUID();
    const payloadHash = hash({ walletId: wallet.id, initialBalance: initialBalance.toJSON() });
    const opening = WagerTransaction.create({
      id,
      providerId: 'internal',
      externalTransactionId: id,
      idempotencyKey: `internal:${id}`,
      payloadHash,
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: 'opening',
      gameId: 'internal',
      kind: 'OPENING',
      money: initialBalance,
    });
    opening.markProcessed(undefined, new Date());

    await em.query(
      `INSERT INTO wager_transactions
         (id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,
          player_id,round_id,game_id,kind,amount,currency,status,processed_at)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'PROCESSED',$13)`,
      [
        opening.id,
        opening.providerId,
        opening.externalTransactionId,
        opening.idempotencyKey,
        payloadHash,
        opening.walletId,
        opening.playerId,
        opening.roundId,
        opening.gameId,
        opening.kind,
        opening.money.toString(),
        opening.money.currency,
        opening.processedAt,
      ],
    );

    const entry = WalletLedgerEntry.create({
      id: randomUUID(),
      walletId: wallet.id,
      transactionId: opening.id,
      direction: 'CREDIT',
      money: initialBalance,
      balanceBefore: Money.zero(initialBalance.currency),
      balanceAfter: initialBalance,
      createdAt: new Date(),
    });
    await this.insertLedger(em, entry);

    const context = { aggregateId: wallet.id, correlationId };
    await this.enqueue(em, WagerTransactionProcessed.from(opening, initialBalance, context));
    await this.enqueue(em, WalletBalanceChanged.from(wallet, entry, context));
  }

  private async lockWallet(em: EntityManager, id: string): Promise<Wallet> {
    await em.query("SET LOCAL lock_timeout='5s'");
    const row = await em
      .getRepository(WalletSchema)
      .findOne({ where: { id }, lock: { mode: 'pessimistic_write' } });
    if (!row) throw new ApplicationError('WALLET_NOT_FOUND', 404, true);
    return walletFrom(row);
  }

  private async replayFromInbox(
    em: EntityManager,
    context: Context,
    payloadHash: string,
  ): Promise<Result | undefined> {
    if (!context.messageId) return undefined;

    const [inbox] = await em.query(
      'SELECT payload_hash,transaction_id FROM inbox WHERE consumer_name=$1 AND message_id=$2',
      [context.consumerName ?? DEFAULT_CONSUMER, context.messageId],
    );
    if (!inbox) return undefined;

    assertReplay(inbox.payload_hash, context.inboxHash ?? payloadHash);
    const [stored] = await em.query('SELECT response FROM wager_transactions WHERE id=$1', [
      inbox.transaction_id,
    ]);
    return { ...stored.response, idempotentReplay: true };
  }

  private async replayFromTransaction(
    em: EntityManager,
    input: WagerInput,
    key: string,
    payloadHash: string,
  ): Promise<Result> {
    const existing: TransactionRow[] = await em.query(
      `SELECT * FROM wager_transactions
        WHERE provider_id=$1 AND (idempotency_key=$2 OR external_transaction_id=$3)`,
      [input.providerId, key, input.externalTransactionId],
    );

    if (existing.length !== 1 || existing[0]!.idempotency_key !== key) {
      throw new ApplicationError('TRANSACTION_IDENTITY_CONFLICT', 409);
    }
    assertReplay(existing[0]!.payload_hash, payloadHash);
    return { ...existing[0]!.response, idempotentReplay: true };
  }

  private async apply(
    em: EntityManager,
    row: TransactionRow,
    wallet: Wallet,
    context: Context,
  ): Promise<Result> {
    const transaction = transactionFrom(row);
    const balanceBefore = wallet.balance;

    let reference: TransactionRow | undefined;
    let rejection: string | undefined;

    if (row.player_id !== wallet.playerId) rejection = 'PLAYER_MISMATCH';
    else if (row.currency !== wallet.currency) rejection = 'CURRENCY_MISMATCH';

    if (!rejection && row.reference_external_transaction_id) {
      reference = await this.findReference(em, row);

      if (!reference || UNRESOLVED.includes(reference.status)) {
        if (reference?.id === row.id) rejection = 'INVALID_REFERENCE';
        else if (row.attempts >= REFERENCE_ATTEMPT_LIMIT) rejection = 'REFERENCE_NOT_FOUND';
        else return await this.deferForReference(em, transaction, wallet, row, context);
      } else {
        rejection = await this.validateReference(em, transaction, row, reference);
      }
    }

    let entry: WalletLedgerEntry | undefined;
    if (!rejection) {
      try {
        entry = this.moveBalance(transaction, wallet, reference);
        transaction.markProcessed(reference?.id, new Date());
      } catch (error) {
        if (!(error instanceof DomainError)) throw error;
        rejection = error.code;
      }
    }

    if (rejection) {
      transaction.reject(rejection);
      entry = undefined;
    }

    const balance = rejection ? balanceBefore : wallet.balance;
    const result: Result = {
      transactionId: transaction.id,
      status: transaction.status,
      balance: balance.toJSON(),
      idempotentReplay: false,
      ...(rejection ? { failureCode: rejection } : {}),
    };

    await em.query(
      `UPDATE wager_transactions
          SET status=$2,reference_transaction_id=$3,failure_code=$4,processed_at=$5,response=$6
        WHERE id=$1`,
      [
        transaction.id,
        transaction.status,
        reference?.id ?? null,
        transaction.failureCode ?? null,
        transaction.processedAt ?? null,
        JSON.stringify(result),
      ],
    );

    const eventContext = {
      aggregateId: wallet.id,
      correlationId: context.correlationId,
      causationId: context.messageId,
    };

    if (entry) {
      await this.insertLedger(em, entry);
      await this.enqueue(em, WalletBalanceChanged.from(wallet, entry, eventContext));
    }

    await this.enqueue(
      em,
      rejection
        ? WagerTransactionRejected.from(transaction, balance, eventContext)
        : WagerTransactionProcessed.from(transaction, balance, eventContext),
    );

    return result;
  }

  private moveBalance(
    transaction: WagerTransaction,
    wallet: Wallet,
    reference?: TransactionRow,
  ): WalletLedgerEntry | undefined {
    if (!transaction.affectsBalance()) return undefined;

    const balanceBefore = wallet.balance;
    const direction = transaction.ledgerDirectionFor(
      reference ? transactionFrom(reference) : undefined,
    );

    if (direction === 'DEBIT') {
      const code =
        transaction.kind === 'ROLLBACK' ? 'REVERSAL_INSUFFICIENT_FUNDS' : 'INSUFFICIENT_FUNDS';
      wallet.debit(transaction.money, code);
    } else {
      wallet.credit(transaction.money);
    }

    if (wallet.balance.scale > MAX_INTEGER_DIGITS) {
      throw new DomainError('BALANCE_LIMIT_EXCEEDED');
    }

    return WalletLedgerEntry.create({
      id: randomUUID(),
      walletId: wallet.id,
      transactionId: transaction.id,
      direction,
      money: transaction.money,
      balanceBefore,
      balanceAfter: wallet.balance,
      createdAt: new Date(),
    });
  }

  private async findReference(
    em: EntityManager,
    row: TransactionRow,
  ): Promise<TransactionRow | undefined> {
    const [reference] = await em.query(
      'SELECT * FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2',
      [row.provider_id, row.reference_external_transaction_id],
    );
    return reference;
  }

  private async validateReference(
    em: EntityManager,
    transaction: WagerTransaction,
    row: TransactionRow,
    reference: TransactionRow,
  ): Promise<string | undefined> {
    if (reference.status !== 'PROCESSED') return 'REFERENCE_NOT_PROCESSED';

    const sameScope =
      reference.wallet_id === row.wallet_id &&
      reference.player_id === row.player_id &&
      reference.currency === row.currency &&
      reference.round_id === row.round_id;
    if (!sameScope) return 'REFERENCE_SCOPE_MISMATCH';

    const allowed = row.kind === 'ROLLBACK' ? ['BET', 'WIN', 'REFUND'] : ['BET'];
    if (!allowed.includes(reference.kind)) return 'REFERENCE_KIND_MISMATCH';

    if (!transaction.requiresReference()) return undefined;

    const amount = Money.rehydrate({ amount: reference.amount, currency: reference.currency });
    if (!amount.equals(transaction.money)) return 'REVERSAL_AMOUNT_MISMATCH';

    const [reversed] = await em.query(
      `SELECT id FROM wager_transactions
        WHERE reference_transaction_id=$1 AND kind IN ('REFUND','ROLLBACK') AND status='PROCESSED'`,
      [reference.id],
    );
    return reversed ? 'ALREADY_REVERSED' : undefined;
  }

  private async deferForReference(
    em: EntityManager,
    transaction: WagerTransaction,
    wallet: Wallet,
    row: TransactionRow,
    context: Context,
  ): Promise<Result> {
    transaction.markPendingReference();

    const result: Result = {
      transactionId: transaction.id,
      status: transaction.status,
      balance: wallet.balance.toJSON(),
      idempotentReplay: false,
    };
    const attempts = row.attempts + 1;

    await em.query(
      `UPDATE wager_transactions
          SET status='PENDING_REFERENCE',response=$2,attempts=$3,
              next_attempt_at=now()+($4 * interval '1 second')
        WHERE id=$1`,
      [
        transaction.id,
        JSON.stringify(result),
        attempts,
        Math.min(MAX_BACKOFF_SECONDS, 2 ** attempts),
      ],
    );

    if (row.status !== 'PENDING_REFERENCE') {
      await this.enqueue(
        em,
        WagerTransactionPendingReference.from(transaction, wallet.balance, {
          aggregateId: wallet.id,
          correlationId: context.correlationId,
          causationId: context.messageId,
        }),
      );
    }
    return result;
  }

  private async recordInbox(
    em: EntityManager,
    context: Context,
    payloadHash: string,
    transactionId: string,
  ) {
    if (!context.messageId) return;

    const message = InboxMessage.receive({
      messageId: context.messageId,
      consumerName: context.consumerName ?? DEFAULT_CONSUMER,
      payloadHash: context.inboxHash ?? payloadHash,
    });
    message.markProcessed(new Date());

    const inserted = await em.query(
      `INSERT INTO inbox(consumer_name,message_id,payload_hash,transaction_id)
       VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING message_id`,
      [message.consumerName, message.messageId, message.payloadHash, transactionId],
    );
    if (inserted.length) return;

    const [stored] = await em.query(
      'SELECT payload_hash,transaction_id FROM inbox WHERE consumer_name=$1 AND message_id=$2',
      [message.consumerName, message.messageId],
    );
    assertReplay(stored.payload_hash, message.payloadHash);
    if (stored.transaction_id !== transactionId) {
      throw new ApplicationError('INBOX_CONFLICT', 409);
    }
  }

  private async insertLedger(em: EntityManager, entry: WalletLedgerEntry) {
    await em.query(
      `INSERT INTO wallet_ledger
         (id,wallet_id,transaction_id,currency,direction,amount,balance_before,balance_after)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
      [
        entry.id,
        entry.walletId,
        entry.transactionId,
        entry.money.currency,
        entry.direction,
        entry.money.toString(),
        entry.balanceBefore.toString(),
        entry.balanceAfter.toString(),
      ],
    );
  }

  private async enqueue<T>(em: EntityManager, event: IntegrationEvent<T>) {
    const message = OutboxMessage.enqueue(event);
    await em.query(
      'INSERT INTO outbox(id,aggregate_id,event_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5)',
      [
        message.id,
        message.aggregateId,
        message.eventType,
        JSON.stringify(message.payload),
        message.occurredAt,
      ],
    );
  }

  private async lookup(
    where: string,
    params: string[],
  ): Promise<Result & { kind: Kind; providerId?: string; externalTransactionId?: string }> {
    const [row]: TransactionRow[] = await this.db.query(
      `SELECT * FROM wager_transactions WHERE ${where}`,
      params,
    );
    if (!row) throw new ApplicationError('TRANSACTION_NOT_FOUND', 404);

    if (row.response) {
      return {
        ...row.response,
        providerId: row.provider_id,
        externalTransactionId: row.external_transaction_id,
        kind: row.kind,
      };
    }

    const [entry] = await this.db.query(
      'SELECT balance_after FROM wallet_ledger WHERE transaction_id=$1',
      [row.id],
    );
    return {
      transactionId: row.id,
      status: row.status,
      kind: row.kind,
      balance: { amount: entry?.balance_after ?? '0.00', currency: row.currency },
      idempotentReplay: false,
    };
  }
}

function encodeCursor(walletId: string, sequence: string): string {
  return Buffer.from(JSON.stringify({ walletId, sequence })).toString('base64url');
}

function decodeCursor(cursor: string, walletId: string): string {
  try {
    const decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString());
    const valid =
      decoded.walletId === walletId &&
      typeof decoded.sequence === 'string' &&
      /^\d{1,19}$/.test(decoded.sequence) &&
      BigInt(decoded.sequence) <= 9223372036854775807n;
    if (!valid) throw new Error('invalid cursor');
    return decoded.sequence;
  } catch {
    throw new ApplicationError('INVALID_CURSOR', 400);
  }
}
