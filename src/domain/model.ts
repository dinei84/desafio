import { DomainError, Money } from './money';

export type Kind = 'OPENING' | 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
export type Status = 'PENDING' | 'PENDING_REFERENCE' | 'PROCESSED' | 'REJECTED' | 'FAILED';
export type Direction = 'DEBIT' | 'CREDIT';

const TERMINAL: Status[] = ['PROCESSED', 'REJECTED', 'FAILED'];
const REVERSALS: Kind[] = ['REFUND', 'ROLLBACK'];

export interface WalletState {
  id: string;
  playerId: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export class Wallet {
  private constructor(private state: WalletState) {}

  static open(props: { id: string; playerId: string; initialBalance: Money }): Wallet {
    if (props.initialBalance.isNegative()) throw new DomainError('INVALID_AMOUNT');
    const now = new Date();
    return new Wallet({
      id: props.id,
      playerId: props.playerId,
      balance: props.initialBalance,
      version: 1,
      createdAt: now,
      updatedAt: now,
    });
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet({ ...state });
  }

  get id() {
    return this.state.id;
  }

  get playerId() {
    return this.state.playerId;
  }

  get currency() {
    return this.state.balance.currency;
  }

  get balance() {
    return this.state.balance;
  }

  get version() {
    return this.state.version;
  }

  get createdAt() {
    return this.state.createdAt;
  }

  get updatedAt() {
    return this.state.updatedAt;
  }

  debit(money: Money, insufficientFundsCode = 'INSUFFICIENT_FUNDS'): void {
    this.assertSameCurrency(money);
    if (money.isNegative()) throw new DomainError('INVALID_AMOUNT');
    if (this.balance.isLessThan(money)) throw new DomainError(insufficientFundsCode);
    this.moveTo(this.balance.subtract(money));
  }

  credit(money: Money): void {
    this.assertSameCurrency(money);
    if (money.isNegative()) throw new DomainError('INVALID_AMOUNT');
    this.moveTo(this.balance.add(money));
  }

  private moveTo(balance: Money): void {
    if (this.balance.equals(balance)) return;
    this.state = { ...this.state, balance, version: this.version + 1, updatedAt: new Date() };
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) throw new DomainError('CURRENCY_MISMATCH');
  }
}

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: Kind;
  money: Money;
  /** Identifier at the provider, never the internal id. */
  referenceExternalTransactionId?: string;
}

export interface WagerTransactionState extends CreateWagerTransactionProps {
  status: Status;
  createdAt: Date;
  referenceTransactionId?: string;
  failureCode?: string;
  processedAt?: Date;
}

export class WagerTransaction {
  private constructor(private state: WagerTransactionState) {}

  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (REVERSALS.includes(props.kind) && !props.referenceExternalTransactionId) {
      throw new DomainError('REFERENCE_REQUIRED');
    }
    if (props.money.isNegative()) throw new DomainError('INVALID_AMOUNT');
    return new WagerTransaction({ ...props, status: 'PENDING', createdAt: new Date() });
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction({ ...state });
  }

  get id() {
    return this.state.id;
  }

  get providerId() {
    return this.state.providerId;
  }

  get externalTransactionId() {
    return this.state.externalTransactionId;
  }

  get idempotencyKey() {
    return this.state.idempotencyKey;
  }

  get walletId() {
    return this.state.walletId;
  }

  get playerId() {
    return this.state.playerId;
  }

  get roundId() {
    return this.state.roundId;
  }

  get gameId() {
    return this.state.gameId;
  }

  get kind() {
    return this.state.kind;
  }

  get money() {
    return this.state.money;
  }

  get referenceExternalTransactionId() {
    return this.state.referenceExternalTransactionId;
  }

  get status() {
    return this.state.status;
  }

  get createdAt() {
    return this.state.createdAt;
  }

  get referenceTransactionId() {
    return this.state.referenceTransactionId;
  }

  get failureCode() {
    return this.state.failureCode;
  }

  get processedAt() {
    return this.state.processedAt;
  }

  isTerminal(): boolean {
    return TERMINAL.includes(this.status);
  }

  affectsBalance(): boolean {
    return this.kind !== 'LOSS' && !this.money.isZero();
  }

  requiresReference(): boolean {
    return REVERSALS.includes(this.kind);
  }

  matchesPayload(payloadHash: string): boolean {
    return this.state.payloadHash === payloadHash;
  }

  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.transition('PROCESSED');
    this.state = { ...this.state, referenceTransactionId, processedAt: at };
  }

  markPendingReference(): void {
    this.transition('PENDING_REFERENCE');
  }

  reject(code: string): void {
    this.transition('REJECTED');
    this.state = { ...this.state, failureCode: code };
  }

  fail(code: string): void {
    this.transition('FAILED');
    this.state = { ...this.state, failureCode: code };
  }

  ledgerDirectionFor(reference?: WagerTransaction): Direction {
    if (this.kind === 'LOSS') throw new DomainError('NO_LEDGER_DIRECTION');
    if (this.kind !== 'ROLLBACK') return this.kind === 'BET' ? 'DEBIT' : 'CREDIT';
    if (!reference) throw new DomainError('REFERENCE_REQUIRED');
    return reference.kind === 'BET' ? 'CREDIT' : 'DEBIT';
  }

  private transition(status: Status): void {
    if (this.isTerminal()) throw new DomainError('INVALID_TRANSACTION_STATE');
    this.state = { ...this.state, status };
  }
}

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: Direction;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export type LedgerEntryState = CreateLedgerEntryProps;

export class WalletLedgerEntry {
  private constructor(private readonly state: Readonly<LedgerEntryState>) {
    Object.freeze(state);
    Object.freeze(this);
  }

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    const entry = new WalletLedgerEntry({ ...props });
    const valid =
      props.money.isPositive() &&
      !props.balanceBefore.isNegative() &&
      !props.balanceAfter.isNegative() &&
      entry.isBalanced();
    if (!valid) throw new DomainError('INVALID_LEDGER_ENTRY');
    return entry;
  }

  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry({ ...state });
  }

  get id() {
    return this.state.id;
  }

  get walletId() {
    return this.state.walletId;
  }

  get transactionId() {
    return this.state.transactionId;
  }

  get direction() {
    return this.state.direction;
  }

  get money() {
    return this.state.money;
  }

  get balanceBefore() {
    return this.state.balanceBefore;
  }

  get balanceAfter() {
    return this.state.balanceAfter;
  }

  get createdAt() {
    return this.state.createdAt;
  }

  isBalanced(): boolean {
    const expected =
      this.direction === 'CREDIT'
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);
    return expected.equals(this.balanceAfter);
  }
}
