import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { DataSource } from 'typeorm';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
} from '@aws-sdk/client-sqs';

import { database } from '../src/infrastructure/database';
import { Processor, type Result } from '../src/application/processor';
import { Broker, Workers } from '../src/infrastructure/sqs';
import type { WagerInput } from '../src/application/contracts';

const suffix = randomUUID().replaceAll('-', '');
const dbName = `test_${suffix}`;
const prefix = `test-${suffix}-`;
const baseUrl = process.env.DATABASE_URL ?? 'postgres://wager:wager@localhost:5432/wager';
const testUrl = new URL(baseUrl);
testUrl.pathname = `/${dbName}`;

const env = {
  ...process.env,
  DATABASE_URL: testUrl.toString(),
  SQS_QUEUE_PREFIX: prefix,
  WORKERS_ENABLED: 'false',
  NODE_ENV: 'test',
};

let admin: DataSource;
let db: DataSource;
let processor: Processor;
let broker: Broker;
let workers: Workers;

const children: ReturnType<typeof Bun.spawn>[] = [];
const urls: string[] = [];

const ctx = () => ({ correlationId: randomUUID() });
const m = (amount: string) => ({ amount, currency: 'BRL' });

async function wallet(amount = '100.00') {
  return processor.openWallet({ playerId: randomUUID(), initialBalance: m(amount) }, randomUUID());
}

function wager(
  owner: { id: string; playerId: string },
  kind: WagerInput['kind'] = 'BET',
  amount = '10.00',
  reference?: string,
): WagerInput {
  return {
    providerId: `provider-${suffix}`,
    externalTransactionId: randomUUID(),
    playerId: owner.playerId,
    walletId: owner.id,
    roundId: 'round-1',
    gameId: 'game-1',
    kind,
    money: m(amount),
    ...(reference ? { referenceExternalTransactionId: reference } : {}),
  };
}

async function submit(input: WagerInput, key = `${input.providerId}:${input.externalTransactionId}`) {
  return processor.submit(input, key, ctx());
}

async function consistent(id: string) {
  const report = await processor.reconcile(id);
  expect(report.consistent).toBe(true);
  expect(report.difference.amount).toBe('0.00');
}

async function waitFor(condition: () => Promise<boolean>, timeout = 15000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await condition().catch(() => false)) return;
    await Bun.sleep(100);
  }
  throw new Error('Timed out waiting for observable condition');
}

function spawnApi(port: number) {
  return Bun.spawn(['bun', 'src/main.ts'], {
    env: { ...env, PORT: String(port) },
    stdout: 'ignore',
    stderr: 'inherit',
  });
}

beforeAll(async () => {
  admin = await database(baseUrl).initialize();
  await admin.query(`CREATE DATABASE "${dbName}"`);

  db = await database(testUrl.toString()).initialize();
  await db.runMigrations();
  await db.undoLastMigration();
  await db.runMigrations();
  processor = new Processor(db);

  process.env.SQS_QUEUE_PREFIX = prefix;
  broker = new Broker();
  await broker.initialize();
  workers = new Workers(db, processor, broker);

  const startPort = 18000 + Math.floor(Math.random() * 10000);
  for (let i = 0; i < 3; i++) {
    const port = startPort + i;
    urls.push(`http://127.0.0.1:${port}`);
    children.push(spawnApi(port));
  }
  await Promise.all(urls.map(url => waitFor(async () => (await fetch(`${url}/health/ready`)).ok)));
}, 60000);

afterAll(async () => {
  for (const child of children) child.kill('SIGTERM');
  await Promise.all(children.map(child => child.exited));

  if (broker?.urls) {
    await Promise.all(
      Object.values(broker.urls).map(QueueUrl =>
        broker.client.send(new DeleteQueueCommand({ QueueUrl })),
      ),
    );
    broker.client.destroy();
  }
  if (db?.isInitialized) await db.destroy();
  if (admin?.isInitialized) {
    await admin.query(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`);
    await admin.destroy();
  }
  delete process.env.SQS_QUEUE_PREFIX;
}, 30000);

test('migrations are reversible and constraints protect ledger, balance, identity and terminal state', async () => {
  const w = await wallet();

  await expect(
    processor.openWallet({ playerId: w.playerId, initialBalance: m('100') }, randomUUID()),
  ).rejects.toThrow('WALLET_ALREADY_EXISTS');

  await expect(db.query('UPDATE wallets SET balance=-1 WHERE id=$1', [w.id])).rejects.toThrow();
  await expect(
    db.query('UPDATE wallets SET balance=balance+1 WHERE id=$1', [w.id]),
  ).rejects.toThrow('wallet/ledger invariant');
  await expect(
    db.query('UPDATE wallet_ledger SET amount=amount+1 WHERE wallet_id=$1', [w.id]),
  ).rejects.toThrow('immutable');
  await expect(db.query('DELETE FROM wallet_ledger WHERE wallet_id=$1', [w.id])).rejects.toThrow(
    'immutable',
  );

  const bet = await submit(wager(w));
  await expect(
    db.query("UPDATE wager_transactions SET status='PENDING' WHERE id=$1", [bet.transactionId]),
  ).rejects.toThrow('terminal');

  const [entry] = await db.query('SELECT * FROM wallet_ledger WHERE transaction_id=$1', [
    bet.transactionId,
  ]);
  await expect(
    db.query(
      `INSERT INTO wallet_ledger(id,wallet_id,transaction_id,currency,direction,amount,balance_before,balance_after)
       VALUES($1,$2,$3,'BRL','DEBIT',10,90,80)`,
      [randomUUID(), w.id, entry.transaction_id],
    ),
  ).rejects.toThrow();

  await consistent(w.id);
});

test('50 simultaneous replays across THREE real HTTP processes debit exactly once', async () => {
  const w = await wallet();
  const input = wager(w);
  const key = randomUUID();

  const results = await Promise.all(
    Array.from({ length: 50 }, async (_, i) => {
      const response = await fetch(`${urls[i % 3]}/wagering/transactions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
        body: JSON.stringify(input),
      });
      expect(response.status).toBe(200);
      return (await response.json()) as Result;
    }),
  );

  expect(new Set(results.map(r => r.transactionId)).size).toBe(1);
  expect(results.filter(r => !r.idempotentReplay)).toHaveLength(1);
  expect(results.every(r => r.balance.amount === '90.00')).toBe(true);
  expect((await processor.wallet(w.id)).balance.amount).toBe('90.00');

  await submit(wager(w));
  expect((await processor.submit(input, key, ctx())).balance.amount).toBe('90.00');

  const [entries] = await db.query(
    'SELECT count(*)::int AS count FROM wallet_ledger WHERE transaction_id=$1',
    [results[0]!.transactionId],
  );
  expect(entries.count).toBe(1);

  await expect(processor.submit({ ...input, money: m('11') }, key, ctx())).rejects.toThrow(
    'IDEMPOTENCY_CONFLICT',
  );
  await expect(processor.submit(input, randomUUID(), ctx())).rejects.toThrow(
    'TRANSACTION_IDENTITY_CONFLICT',
  );

  await consistent(w.id);
});

test('an idempotency key only collides inside its own provider', async () => {
  const w = await wallet();
  const key = `shared-${randomUUID()}`;

  const first = { ...wager(w), providerId: `provider-a-${suffix}` };
  const second = { ...wager(w), providerId: `provider-b-${suffix}` };

  expect((await submit(first, key)).status).toBe('PROCESSED');
  expect((await submit(second, key)).status).toBe('PROCESSED');
  expect((await processor.wallet(w.id)).balance.amount).toBe('80.00');

  const replay = await submit(second, key);
  expect(replay.idempotentReplay).toBe(true);

  await consistent(w.id);
});

test('two bets of 80 compete for 100; only one debit survives, including retries', async () => {
  const w = await wallet();
  const a = wager(w, 'BET', '80');
  const b = wager(w, 'BET', '80');

  const results = await Promise.all([submit(a), submit(b)]);
  expect(results.filter(r => r.status === 'PROCESSED')).toHaveLength(1);
  expect(results.find(r => r.status === 'REJECTED')!.failureCode).toBe('INSUFFICIENT_FUNDS');

  await Promise.all([submit(a), submit(b)]);
  expect((await processor.wallet(w.id)).balance.amount).toBe('20.00');

  const page = await processor.ledger(w.id);
  expect(page.entries.filter((e: { direction: string }) => e.direction === 'DEBIT')).toHaveLength(1);

  await consistent(w.id);
});

test('distinct wallets process in parallel without a global wallet lock', async () => {
  const wallets = await Promise.all(Array.from({ length: 8 }, () => wallet()));

  const runner = db.createQueryRunner();
  await runner.connect();
  await runner.startTransaction();
  try {
    await runner.query('SELECT id FROM wallets WHERE id=$1 FOR UPDATE', [wallets[0]!.id]);
    await Promise.all(wallets.slice(1).map(w => submit(wager(w))));
    await Promise.all(wallets.slice(1).map(w => consistent(w.id)));
  } finally {
    await runner.rollbackTransaction();
    await runner.release();
  }

  await consistent(wallets[0]!.id);
});

test('WIN, LOSS, REFUND, ROLLBACK and explicit reversal insufficient funds', async () => {
  const w = await wallet();
  const bet = wager(w, 'BET', '20');
  await submit(bet);

  const loss = await submit(wager(w, 'LOSS', '0'));
  expect(loss.balance.amount).toBe('80.00');

  const win = wager(w, 'WIN', '30', bet.externalTransactionId);
  await submit(win);

  const refunds = await Promise.all([
    submit(wager(w, 'REFUND', '20', bet.externalTransactionId)),
    submit(wager(w, 'REFUND', '20', bet.externalTransactionId)),
  ]);
  expect(refunds.filter(r => r.status === 'PROCESSED')).toHaveLength(1);
  expect(refunds.find(r => r.status === 'REJECTED')!.failureCode).toBe('ALREADY_REVERSED');

  const [refund] = await db.query(
    "SELECT * FROM wager_transactions WHERE wallet_id=$1 AND kind='REFUND' AND status='PROCESSED'",
    [w.id],
  );
  await submit(wager(w, 'ROLLBACK', '20', refund.external_transaction_id));
  await submit(wager(w, 'BET', '100'));

  const reversal = await submit(wager(w, 'ROLLBACK', '30', win.externalTransactionId));
  expect(reversal.failureCode).toBe('REVERSAL_INSUFFICIENT_FUNDS');
  expect(reversal.balance.amount).toBe('10.00');

  const [entries] = await db.query(
    'SELECT count(*)::int AS count FROM wallet_ledger WHERE transaction_id=ANY($1::uuid[])',
    [[loss.transactionId, reversal.transactionId]],
  );
  expect(entries.count).toBe(0);

  await consistent(w.id);
});

test('a processed transaction is reversed once, whichever reversal kind arrives first', async () => {
  for (const [first, second] of [
    ['REFUND', 'ROLLBACK'],
    ['ROLLBACK', 'REFUND'],
  ] as const) {
    const w = await wallet();
    const bet = wager(w, 'BET', '10');
    await submit(bet);

    const applied = await submit(wager(w, first, '10', bet.externalTransactionId));
    expect(applied.status).toBe('PROCESSED');

    const refused = await submit(wager(w, second, '10', bet.externalTransactionId));
    expect(refused.status).toBe('REJECTED');
    expect(refused.failureCode).toBe('ALREADY_REVERSED');

    expect((await processor.wallet(w.id)).balance.amount).toBe('100.00');
    await consistent(w.id);
  }
});

test('the database refuses a second reversal even when the application check is bypassed', async () => {
  const w = await wallet();
  const bet = wager(w, 'BET', '10');
  await submit(bet);

  const refund = await submit(wager(w, 'REFUND', '10', bet.externalTransactionId));
  const [reference] = await db.query(
    'SELECT reference_transaction_id FROM wager_transactions WHERE id=$1',
    [refund.transactionId],
  );

  await expect(
    db.query(
      `INSERT INTO wager_transactions
         (id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,
          round_id,game_id,kind,amount,currency,reference_external_transaction_id,
          reference_transaction_id,status,processed_at)
       VALUES($1,$2,$3,$4,$5,$6,$7,'round-1','game-1','ROLLBACK',10,'BRL',$8,$9,'PROCESSED',now())`,
      [
        randomUUID(),
        `provider-${suffix}`,
        randomUUID(),
        randomUUID(),
        'a'.repeat(64),
        w.id,
        w.playerId,
        bet.externalTransactionId,
        reference.reference_transaction_id,
      ],
    ),
  ).rejects.toThrow();

  await consistent(w.id);
});

test('reversal validates amount, scope, kind, failed reference and currency', async () => {
  const w = await wallet();
  const bet = wager(w);
  await submit(bet);

  expect((await submit(wager(w, 'REFUND', '9', bet.externalTransactionId))).failureCode).toBe(
    'REVERSAL_AMOUNT_MISMATCH',
  );
  expect(
    (
      await submit({
        ...wager(w, 'REFUND', '10', bet.externalTransactionId),
        roundId: 'wrong',
      })
    ).failureCode,
  ).toBe('REFERENCE_SCOPE_MISMATCH');

  const loss = wager(w, 'LOSS', '10');
  await submit(loss);
  expect((await submit(wager(w, 'ROLLBACK', '10', loss.externalTransactionId))).failureCode).toBe(
    'REFERENCE_KIND_MISMATCH',
  );

  const unfunded = wager(w, 'BET', '1000');
  await submit(unfunded);
  expect(
    (await submit(wager(w, 'REFUND', '1000', unfunded.externalTransactionId))).failureCode,
  ).toBe('REFERENCE_NOT_PROCESSED');

  expect(
    (await submit({ ...wager(w), money: { amount: '10.00', currency: 'USD' } })).failureCode,
  ).toBe('CURRENCY_MISMATCH');
  expect((await submit({ ...wager(w), playerId: randomUUID() })).failureCode).toBe(
    'PLAYER_MISMATCH',
  );

  await consistent(w.id);
});

test('REFUND and ROLLBACK before BET resolve after retry, missing references expire', async () => {
  for (const kind of ['REFUND', 'ROLLBACK'] as const) {
    const w = await wallet();
    const bet = wager(w);
    const reversal = wager(w, kind, '10', bet.externalTransactionId);

    const deferred = await submit(reversal);
    expect(deferred.status).toBe('PENDING_REFERENCE');

    await submit(bet);
    await db.query('UPDATE wager_transactions SET next_attempt_at=now() WHERE id=$1', [
      deferred.transactionId,
    ]);
    await processor.retryReferences();

    expect((await processor.transaction(deferred.transactionId)).status).toBe('PROCESSED');
    expect((await processor.wallet(w.id)).balance.amount).toBe('100.00');
    await consistent(w.id);
  }

  const w = await wallet();
  const orphan = await submit(wager(w, 'REFUND', '10', 'missing'));
  await db.query('UPDATE wager_transactions SET attempts=12,next_attempt_at=now() WHERE id=$1', [
    orphan.transactionId,
  ]);
  await processor.retryReferences();

  expect((await processor.transaction(orphan.transactionId)).failureCode).toBe(
    'REFERENCE_NOT_FOUND',
  );
  await consistent(w.id);
});

test('SQL rollback is atomic across transaction, wallet, ledger, inbox and outbox', async () => {
  const w = await wallet();
  const input = wager(w);
  const key = randomUUID();
  const context = { correlationId: 'atomic-test', messageId: randomUUID() };

  await db.query(`
    CREATE FUNCTION test_outbox_failure() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.payload->>'correlationId'='atomic-test' THEN RAISE EXCEPTION 'injected failure'; END IF;
      RETURN NEW;
    END $$;
    CREATE TRIGGER test_outbox_failure BEFORE INSERT ON outbox
      FOR EACH ROW EXECUTE FUNCTION test_outbox_failure();
  `);
  try {
    await expect(processor.submit(input, key, context)).rejects.toThrow('injected failure');
  } finally {
    await db.query('DROP TRIGGER test_outbox_failure ON outbox; DROP FUNCTION test_outbox_failure();');
  }

  expect((await processor.wallet(w.id)).balance.amount).toBe('100.00');

  const [transactions] = await db.query(
    'SELECT count(*)::int AS count FROM wager_transactions WHERE idempotency_key=$1',
    [key],
  );
  expect(transactions.count).toBe(0);

  const [inbox] = await db.query('SELECT count(*)::int AS count FROM inbox WHERE message_id=$1', [
    context.messageId,
  ]);
  expect(inbox.count).toBe(0);

  await processor.submit(input, key, context);
  await consistent(w.id);
});

test('inbox redelivery is persistent and divergent message IDs cannot move money', async () => {
  const w = await wallet();
  const input = wager(w);
  const key = randomUUID();
  const context = { ...ctx(), messageId: randomUUID(), inboxHash: 'a'.repeat(64) };

  const first = await processor.submit(input, key, context);
  const replay = await processor.submit(input, key, context);
  expect(replay.idempotentReplay).toBe(true);
  expect(replay.transactionId).toBe(first.transactionId);

  await expect(
    processor.submit(wager(w), randomUUID(), { ...context, inboxHash: 'b'.repeat(64) }),
  ).rejects.toThrow('IDEMPOTENCY_CONFLICT');

  expect((await processor.wallet(w.id)).balance.amount).toBe('90.00');
  await consistent(w.id);
});

test('two concurrent real SQS publishers drain the outbox; failure backs off and restart recovers', async () => {
  const w = await wallet();
  await submit(wager(w));

  const events = broker.urls.events;
  broker.urls.events = events.replace('wager-events.fifo', 'nonexistent.fifo');
  try {
    await workers.publishOnce();
  } finally {
    broker.urls.events = events;
  }

  const [backoff] = await db.query('SELECT * FROM outbox WHERE published_at IS NULL AND attempts>0');
  expect(backoff).toBeDefined();
  await db.query('UPDATE outbox SET next_attempt_at=now() WHERE published_at IS NULL');

  const restored = await database(testUrl.toString()).initialize();
  try {
    const second = new Workers(restored, new Processor(restored), broker);
    while ((await Promise.all([workers.publishOnce(), second.publishOnce()])).some(Boolean)) {
      /* both publishers compete for the same rows */
    }
  } finally {
    await restored.destroy();
  }

  const [pending] = await db.query(
    'SELECT count(*)::int AS count FROM outbox WHERE published_at IS NULL',
  );
  expect(pending.count).toBe(0);

  const delivered = await broker.client.send(
    new ReceiveMessageCommand({
      QueueUrl: broker.urls.events,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1,
    }),
  );
  expect(delivered.Messages?.length).toBeGreaterThan(0);
  for (const message of delivered.Messages ?? []) {
    const envelope = JSON.parse(message.Body!);
    expect(envelope.eventId).toBeDefined();
    expect(envelope.version).toBe(1);
    expect(envelope.data.walletId).toBeDefined();
  }

  await consistent(w.id);
});

async function nextInput() {
  const received = await broker.client.send(
    new ReceiveMessageCommand({
      QueueUrl: broker.urls.input,
      WaitTimeSeconds: 2,
      MaxNumberOfMessages: 1,
      VisibilityTimeout: 5,
      MessageSystemAttributeNames: ['ApproximateReceiveCount'],
    }),
  );
  expect(received.Messages?.length).toBe(1);
  return received.Messages![0]!;
}

function envelope(input: WagerInput) {
  return {
    messageId: randomUUID(),
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data: { ...input, idempotencyKey: randomUUID() },
  };
}

test('worker killed AFTER COMMIT BEFORE ACK redelivers through real SQS without another debit', async () => {
  const w = await wallet();
  const message = envelope(wager(w));
  await broker.send('input', message, w.id);

  const child = Bun.spawn(['bun', 'scripts/crash-consumer.ts'], {
    env: { ...env, TEST_CRASH_AFTER_COMMIT: 'true' },
    stdout: 'ignore',
    stderr: 'inherit',
  });
  expect(await child.exited).not.toBe(0);
  expect((await processor.wallet(w.id)).balance.amount).toBe('90.00');

  await Bun.sleep(1300);
  await workers.handle(await nextInput());
  expect((await processor.wallet(w.id)).balance.amount).toBe('90.00');

  const [inbox] = await db.query('SELECT count(*)::int AS count FROM inbox WHERE message_id=$1', [
    message.messageId,
  ]);
  expect(inbox.count).toBe(1);

  await consistent(w.id);
});

test('a message for a wallet that does not exist yet is retried, never dead-lettered', async () => {
  const absent = { id: randomUUID(), playerId: randomUUID() };
  await broker.send('input', envelope(wager(absent)), absent.id);

  await workers.handle(await nextInput());
  await Bun.sleep(2200);

  const redelivered = await nextInput();
  expect(Number(redelivered.Attributes?.ApproximateReceiveCount)).toBeGreaterThan(1);
  await broker.client.send(
    new DeleteMessageCommand({
      QueueUrl: broker.urls.input,
      ReceiptHandle: redelivered.ReceiptHandle,
    }),
  );
});

test('SQS transient error retries successfully; malformed message routes to DLQ', async () => {
  const w = await wallet();
  await broker.send('input', envelope(wager(w)), w.id);

  const uninitialized = database(testUrl.toString());
  const unavailable = new Workers(uninitialized, new Processor(uninitialized), broker);
  await unavailable.handle(await nextInput());
  expect((await processor.wallet(w.id)).balance.amount).toBe('100.00');

  await Bun.sleep(2200);
  await workers.handle(await nextInput());
  expect((await processor.wallet(w.id)).balance.amount).toBe('90.00');

  await broker.send('input', { invalid: true }, randomUUID());
  await workers.handle(await nextInput());

  const dlq = await broker.client.send(
    new ReceiveMessageCommand({
      QueueUrl: broker.urls.dlq,
      WaitTimeSeconds: 2,
      MaxNumberOfMessages: 10,
    }),
  );
  expect(dlq.Messages?.some(msg => JSON.parse(msg.Body!).failureCode === 'INVALID_MESSAGE_ID')).toBe(
    true,
  );
  for (const message of dlq.Messages ?? []) {
    await broker.client.send(
      new DeleteMessageCommand({ QueueUrl: broker.urls.dlq, ReceiptHandle: message.ReceiptHandle }),
    );
  }

  await consistent(w.id);
});

test('SQS retry limit routes persistent infrastructure failures to DLQ', async () => {
  const w = await wallet();
  await broker.send('input', envelope(wager(w)), w.id);

  const unavailable = new Workers(
    database(testUrl.toString()),
    new Processor(database(testUrl.toString())),
    broker,
  );

  for (let attempt = 1; attempt <= 5; attempt++) {
    const message = await nextInput();
    await unavailable.handle(message);
    if (attempt < 5) {
      await broker.client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: broker.urls.input,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: 0,
        }),
      );
    }
  }

  const dlq = await broker.client.send(
    new ReceiveMessageCommand({ QueueUrl: broker.urls.dlq, WaitTimeSeconds: 2 }),
  );
  expect(dlq.Messages?.length).toBeGreaterThan(0);
  expect((await processor.wallet(w.id)).balance.amount).toBe('100.00');

  await consistent(w.id);
});

test('HTTP contracts, stable pagination and process restart retain financial state', async () => {
  const url = urls[0]!;
  expect((await fetch(`${url}/health/live`)).status).toBe(200);
  expect((await fetch(`${url}/health/ready`)).status).toBe(200);
  expect((await fetch(`${url}/metrics`)).status).toBe(200);

  const w = await wallet();
  const post = (body: unknown, key?: string) =>
    fetch(`${url}/wagering/transactions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
      body: JSON.stringify(body),
    });

  expect((await post(wager(w))).status).toBe(400);
  expect((await post(wager(w, 'BET', '1000'), randomUUID())).status).toBe(422);
  expect((await post(wager(w, 'REFUND', '10', 'never'), randomUUID())).status).toBe(202);

  const bet = wager(w);
  await submit(bet);
  await submit(wager(w));

  const page = await processor.ledger(w.id, undefined, '2');
  expect(page.entries).toHaveLength(2);
  expect(page.nextCursor).toBeTruthy();

  const next = await processor.ledger(w.id, page.nextCursor!, '2');
  expect(next.entries).toHaveLength(1);
  expect(new Set([...page.entries, ...next.entries].map((e: { id: string }) => e.id)).size).toBe(3);
  await expect(processor.ledger(w.id, 'broken')).rejects.toThrow('INVALID_CURSOR');

  children[0]!.kill('SIGTERM');
  await children[0]!.exited;
  children[0] = spawnApi(Number(new URL(url).port));
  await waitFor(async () => (await fetch(`${url}/health/ready`)).ok);

  const restored = await fetch(`${url}/wallets/${w.id}`);
  expect(((await restored.json()) as { balance: { amount: string } }).balance.amount).toBe('80.00');

  const external = await fetch(
    `${url}/providers/${bet.providerId}/wagering/transactions/${bet.externalTransactionId}`,
  );
  expect(external.status).toBe(200);

  await consistent(w.id);
});

test('running workers consume a pending SQS reversal and the scheduler resolves it', async () => {
  const w = await wallet();
  const bet = wager(w, 'BET', '25');
  const reversal = wager(w, 'REFUND', '25', bet.externalTransactionId);

  const running = new Workers(db, processor, broker);
  running.start();
  try {
    await broker.send('input', envelope(reversal), w.id);
    await waitFor(async () => {
      const [row] = await db.query(
        'SELECT status FROM wager_transactions WHERE external_transaction_id=$1',
        [reversal.externalTransactionId],
      );
      return row?.status === 'PENDING_REFERENCE';
    });

    await submit(bet);
    await waitFor(async () => {
      const [row] = await db.query(
        'SELECT status FROM wager_transactions WHERE external_transaction_id=$1',
        [reversal.externalTransactionId],
      );
      return row?.status === 'PROCESSED';
    });
  } finally {
    await running.stop();
  }

  expect((await processor.wallet(w.id)).balance.amount).toBe('100.00');
  await consistent(w.id);
});
