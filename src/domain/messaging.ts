import type { EventEnvelope, IntegrationEvent } from './events';

const MAX_BACKOFF_SECONDS = 300;

export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
}

export interface InboxMessageState extends ReceiveInboxProps {
  receivedAt: Date;
  processedAt?: Date;
}

export class InboxMessage {
  private constructor(
    readonly messageId: string,
    readonly consumerName: string,
    readonly payloadHash: string,
    readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}

  static receive(props: ReceiveInboxProps): InboxMessage {
    return new InboxMessage(props.messageId, props.consumerName, props.payloadHash, new Date());
  }

  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(
      state.messageId,
      state.consumerName,
      state.payloadHash,
      state.receivedAt,
      state.processedAt,
    );
  }

  get processedAt() {
    return this._processedAt;
  }

  isProcessed(): boolean {
    return this._processedAt !== undefined;
  }

  markProcessed(at: Date): void {
    if (this.isProcessed()) throw new Error('INBOX_ALREADY_PROCESSED');
    this._processedAt = at;
  }
}

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: EventEnvelope<unknown>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
}

export class OutboxMessage {
  private constructor(
    readonly id: string,
    readonly aggregateId: string,
    readonly eventType: string,
    readonly payload: Readonly<EventEnvelope<unknown>>,
    readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}

  static enqueue<T>(event: IntegrationEvent<T>): OutboxMessage {
    return new OutboxMessage(
      event.eventId,
      event.context.aggregateId,
      event.eventType,
      Object.freeze(event.toJSON()),
      event.occurredAt,
      0,
    );
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateId,
      state.eventType,
      Object.freeze(state.payload),
      state.occurredAt,
      state.attempts,
      state.nextAttemptAt,
      state.publishedAt,
    );
  }

  get attempts() {
    return this._attempts;
  }

  get nextAttemptAt() {
    return this._nextAttemptAt;
  }

  get publishedAt() {
    return this._publishedAt;
  }

  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    return this.isPending() && (!this._nextAttemptAt || this._nextAttemptAt <= now);
  }

  markPublished(at: Date): void {
    this._publishedAt = at;
  }

  scheduleRetry(now: Date): void {
    this._attempts++;
    const seconds = Math.min(MAX_BACKOFF_SECONDS, 2 ** Math.min(this._attempts, 9));
    this._nextAttemptAt = new Date(now.getTime() + seconds * 1000);
  }
}
