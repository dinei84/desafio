import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  SendMessageCommand,
  SQSClient,
  type Message,
} from '@aws-sdk/client-sqs';
import { randomUUID } from 'node:crypto';
import type { DataSource, EntityManager } from 'typeorm';

import { Processor } from '../application/processor';
import { ApplicationError, hash, parseWager, record, textField } from '../application/contracts';
import { DomainError } from '../domain/money';
import type { EventEnvelope } from '../domain/events';
import { OutboxMessage } from '../domain/messaging';
import { errorCode, log, metrics } from './observability';

export const queueNames = {
  input: 'wager-transactions.fifo',
  dlq: 'wager-transactions-dlq.fifo',
  events: 'wager-events.fifo',
} as const;

type QueueName = keyof typeof queueNames;

const VISIBILITY_SECONDS = 30;
const VISIBILITY_REFRESH_MS = 10_000;
const MAX_RECEIVES = 5;
const MAX_BACKOFF_SECONDS = 300;
const OUTBOX_CLAIM = 50;
const SQS_BATCH = 10;
const CONSUMER_NAME = 'wager';

function prefixed(name: string): string {
  return `${process.env.SQS_QUEUE_PREFIX ?? ''}${name}`;
}

/** A failure without a driver code is a bug in our code, not an outage worth retrying. */
function isPermanent(error: unknown): boolean {
  if (error instanceof ApplicationError) return !error.retryable;
  return error instanceof DomainError;
}

export interface BatchEntry {
  id: string;
  body: unknown;
  group: string;
}

export class Broker {
  readonly client = new SQSClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
    maxAttempts: 2,
  });

  urls!: Record<QueueName, string>;

  async connect() {
    const entries = await Promise.all(
      Object.entries(queueNames).map(async ([key, name]) => {
        const { QueueUrl } = await this.client.send(
          new GetQueueUrlCommand({ QueueName: prefixed(name) }),
        );
        return [key, QueueUrl!];
      }),
    );
    this.urls = Object.fromEntries(entries) as typeof this.urls;
  }

  async initialize() {
    const dlq = await this.client.send(
      new CreateQueueCommand({
        QueueName: prefixed(queueNames.dlq),
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }),
    );
    const attributes = await this.client.send(
      new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] }),
    );

    await this.client.send(
      new CreateQueueCommand({
        QueueName: prefixed(queueNames.input),
        Attributes: {
          FifoQueue: 'true',
          ContentBasedDeduplication: 'false',
          VisibilityTimeout: String(VISIBILITY_SECONDS),
          ReceiveMessageWaitTimeSeconds: '2',
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: attributes.Attributes!.QueueArn,
            maxReceiveCount: String(MAX_RECEIVES),
          }),
        },
      }),
    );
    await this.client.send(
      new CreateQueueCommand({
        QueueName: prefixed(queueNames.events),
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }),
    );
    await this.connect();
  }

  async ready() {
    await Promise.all(
      Object.values(this.urls).map(QueueUrl =>
        this.client.send(
          new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['QueueArn'] }),
          { abortSignal: AbortSignal.timeout(3000) },
        ),
      ),
    );
  }

  async send(queue: QueueName, body: unknown, group: string, dedup = randomUUID()) {
    return this.client.send(
      new SendMessageCommand({
        QueueUrl: this.urls[queue],
        MessageBody: JSON.stringify(body),
        MessageGroupId: group,
        MessageDeduplicationId: dedup,
      }),
      { abortSignal: AbortSignal.timeout(5000) },
    );
  }

  /** Returns the ids SQS refused, so the caller can keep them pending. */
  async sendBatch(queue: QueueName, entries: BatchEntry[]): Promise<Set<string>> {
    const result = await this.client.send(
      new SendMessageBatchCommand({
        QueueUrl: this.urls[queue],
        Entries: entries.map(entry => ({
          Id: entry.id,
          MessageBody: JSON.stringify(entry.body),
          MessageGroupId: entry.group,
          MessageDeduplicationId: entry.id,
        })),
      }),
      { abortSignal: AbortSignal.timeout(10_000) },
    );
    return new Set((result.Failed ?? []).map(failure => failure.Id!));
  }
}

interface OutboxRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: EventEnvelope<unknown>;
  occurred_at: Date;
  attempts: number;
  next_attempt_at: Date;
  published_at: Date | null;
}

export class Workers {
  private stopping = false;
  private tasks: Promise<void>[] = [];

  constructor(
    readonly db: DataSource,
    readonly processor: Processor,
    readonly broker: Broker,
  ) {}

  start() {
    this.tasks = [
      this.loop('consumer', () => this.consumeOnce()),
      this.loop('outbox', () => this.publishOnce()),
      this.loop('reference', () => this.processor.retryReferences()),
    ];
  }

  async stop() {
    this.stopping = true;
    await Promise.all(this.tasks);
  }

  async consumeOnce() {
    const received = await this.broker.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.broker.urls.input,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 2,
        VisibilityTimeout: VISIBILITY_SECONDS,
        MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
      }),
      { abortSignal: AbortSignal.timeout(5000) },
    );

    // One FIFO group stays sequential; separate groups are free to run at the same time.
    const groups = new Map<string, Message[]>();
    for (const message of received.Messages ?? []) {
      const group = message.Attributes?.MessageGroupId ?? 'batch';
      groups.set(group, [...(groups.get(group) ?? []), message]);
    }

    await Promise.all(
      [...groups.values()].map(async messages => {
        for (const message of messages) {
          if (this.stopping) await this.release(message);
          else await this.handle(message);
        }
      }),
    );
  }

  async handle(message: Message) {
    const keepAlive = setInterval(() => {
      void this.changeVisibility(message, VISIBILITY_SECONDS).catch(() =>
        log('visibility_extension_failed', { messageId: message.MessageId }),
      );
    }, VISIBILITY_REFRESH_MS);

    try {
      let messageId: string | undefined;
      try {
        const envelope = this.parse(message);
        messageId = envelope.messageId;

        await this.processor.submit(envelope.input, envelope.idempotencyKey, {
          correlationId: envelope.messageId,
          messageId: envelope.messageId,
          consumerName: CONSUMER_NAME,
          inboxHash: envelope.inboxHash,
        });

        if (process.env.NODE_ENV === 'test' && process.env.TEST_CRASH_AFTER_COMMIT === 'true') {
          process.kill(process.pid, 'SIGKILL');
        }
      } catch (error) {
        const receives = Number(message.Attributes?.ApproximateReceiveCount ?? 1);
        if (!isPermanent(error) && receives < MAX_RECEIVES) {
          metrics.retries.inc({ component: 'consumer' });
          await this.changeVisibility(message, Math.min(MAX_BACKOFF_SECONDS, 2 ** receives));
          log('message_retry', { messageId, code: errorCode(error) ?? 'TRANSIENT_ERROR' });
          return;
        }
        await this.deadLetter(message, messageId, error);
      }
      // Reached only after the SQL commit, or after the DLQ publication was confirmed.
      await this.broker.client.send(
        new DeleteMessageCommand({
          QueueUrl: this.broker.urls.input,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );
    } finally {
      clearInterval(keepAlive);
    }
  }

  async publishOnce(): Promise<boolean> {
    const published = await this.db.transaction(async em => {
      const rows: OutboxRow[] = await em.query(
        `SELECT * FROM outbox
          WHERE published_at IS NULL AND next_attempt_at<=now()
          ORDER BY occurred_at,id LIMIT ${OUTBOX_CLAIM}
          FOR UPDATE SKIP LOCKED`,
      );
      if (!rows.length) return false;

      for (let start = 0; start < rows.length; start += SQS_BATCH) {
        await this.publishBatch(em, rows.slice(start, start + SQS_BATCH));
      }
      return true;
    });

    const [oldest] = await this.db.query(
      `SELECT COALESCE(EXTRACT(EPOCH FROM now()-min(occurred_at)),0)::float8 AS seconds
         FROM outbox WHERE published_at IS NULL`,
    );
    metrics.lag.set(Math.max(0, oldest.seconds));
    return published;
  }

  private async publishBatch(em: EntityManager, rows: OutboxRow[]) {
    const messages = rows.map(row =>
      OutboxMessage.rehydrate({
        id: row.id,
        aggregateId: row.aggregate_id,
        eventType: row.event_type,
        payload: row.payload,
        occurredAt: row.occurred_at,
        attempts: row.attempts,
        nextAttemptAt: row.next_attempt_at,
        publishedAt: row.published_at ?? undefined,
      }),
    );

    const now = new Date();
    let refused: Set<string>;
    try {
      refused = await this.broker.sendBatch(
        'events',
        messages.map(message => ({
          id: message.id,
          body: message.payload,
          group: message.aggregateId,
        })),
      );
    } catch (error) {
      refused = new Set(messages.map(message => message.id));
      log('outbox_retry', {
        correlationId: rows[0]?.payload.correlationId,
        code: errorCode(error) ?? 'PUBLISH_FAILED',
      });
    }

    const delivered = messages.filter(message => !refused.has(message.id));
    for (const message of delivered) message.markPublished(now);

    if (delivered.length) {
      await em.query(
        'UPDATE outbox SET published_at=$2,attempts=attempts+1 WHERE id=ANY($1::uuid[])',
        [delivered.map(message => message.id), now],
      );
    }

    for (const message of messages.filter(candidate => refused.has(candidate.id))) {
      message.scheduleRetry(now);
      metrics.retries.inc({ component: 'outbox' });
      await em.query('UPDATE outbox SET attempts=$2,next_attempt_at=$3 WHERE id=$1', [
        message.id,
        message.attempts,
        message.nextAttemptAt,
      ]);
    }
  }

  private parse(message: Message) {
    let payload: unknown;
    try {
      payload = JSON.parse(message.Body ?? '');
    } catch {
      throw new ApplicationError('INVALID_JSON', 400);
    }

    const envelope = record(payload);
    const messageId = textField(envelope.messageId, 'MESSAGE_ID');
    const validEnvelope =
      envelope.type === 'WagerTransactionRequested' &&
      typeof envelope.occurredAt === 'string' &&
      Number.isFinite(Date.parse(envelope.occurredAt));
    if (!validEnvelope) throw new ApplicationError('INVALID_ENVELOPE', 400);

    const data = record(envelope.data);
    const input = parseWager(data);
    const idempotencyKey = textField(data.idempotencyKey, 'IDEMPOTENCY_KEY');

    return {
      messageId,
      input,
      idempotencyKey,
      inboxHash: hash({ type: envelope.type, data: { ...input, idempotencyKey } }),
    };
  }

  private async deadLetter(message: Message, messageId: string | undefined, error: unknown) {
    const failureCode = errorCode(error) ?? 'UNEXPECTED_ERROR';
    await this.broker.send(
      'dlq',
      { originalBody: message.Body, sourceMessageId: message.MessageId, failureCode },
      message.MessageId ?? randomUUID(),
    );
    metrics.dlq.inc();
    log('message_dlq', { messageId, code: failureCode });
  }

  private async release(message: Message) {
    await this.changeVisibility(message, 0);
  }

  private changeVisibility(message: Message, seconds: number) {
    return this.broker.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: this.broker.urls.input,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: seconds,
      }),
    );
  }

  private async loop(component: string, action: () => Promise<unknown>) {
    while (!this.stopping) {
      try {
        await action();
      } catch (error) {
        log('worker_error', { component, code: errorCode(error) ?? 'UNEXPECTED_ERROR' });
      }
      if (!this.stopping) await Bun.sleep(component === 'reference' ? 1000 : 200);
    }
  }
}
