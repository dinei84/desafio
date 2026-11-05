export interface MoneyProps {
  amount: string;
  currency: string;
}

export class DomainError extends Error {
  constructor(readonly code: string, message = code) {
    super(message);
  }
}

const AMOUNT = /^(0|[1-9]\d{0,15})(\.\d{1,2})?$/;
const CURRENCY = /^[A-Z]{3}$/;

export class Money {
  private constructor(
    private readonly cents: bigint,
    readonly currency: string,
  ) {
    Object.freeze(this);
  }

  static from(props: MoneyProps): Money {
    if (!props || typeof props.amount !== 'string' || !AMOUNT.test(props.amount)) {
      throw new DomainError('INVALID_AMOUNT');
    }
    if (typeof props.currency !== 'string' || !CURRENCY.test(props.currency)) {
      throw new DomainError('INVALID_CURRENCY');
    }
    const [whole, fraction = ''] = props.amount.split('.');
    return new Money(BigInt(whole!) * 100n + BigInt(fraction.padEnd(2, '0')), props.currency);
  }

  /** Accepts the negative values produced internally by subtraction and stored differences. */
  static rehydrate(props: MoneyProps): Money {
    if (props.amount.startsWith('-')) {
      return Money.from({ ...props, amount: props.amount.slice(1) }).negate();
    }
    return Money.from(props);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: '0.00', currency });
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.cents + other.cents, this.currency);
  }

  subtract(other: Money): Money {
    return this.add(other.negate());
  }

  negate(): Money {
    return new Money(-this.cents, this.currency);
  }

  isZero(): boolean {
    return this.cents === 0n;
  }

  isPositive(): boolean {
    return this.cents > 0n;
  }

  isNegative(): boolean {
    return this.cents < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.cents < other.cents;
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.cents === other.cents;
  }

  /** Integer digits, used to keep values inside the numeric(18,2) columns. */
  get scale(): number {
    const absolute = this.cents < 0n ? -this.cents : this.cents;
    return String(absolute / 100n).length;
  }

  toString(): string {
    const absolute = this.cents < 0n ? -this.cents : this.cents;
    const sign = this.cents < 0n ? '-' : '';
    return `${sign}${absolute / 100n}.${String(absolute % 100n).padStart(2, '0')}`;
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) throw new DomainError('CURRENCY_MISMATCH');
  }
}
