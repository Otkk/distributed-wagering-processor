import Decimal from 'decimal.js';
import { CurrencyMismatchError, InvalidMoneyError } from '../errors/domain.error';

export interface MoneyProps {
  amount: string; // decimal string, ex: "25.00"
  currency: string; // ISO-4217, ex: "BRL"
}

export class Money {
  private static readonly DECIMAL_REGEX = /^\d+(\.\d{1,2})?$/;
  private static readonly CURRENCY_REGEX = /^[A-Z]{3}$/;

  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {}

  public static from(props: MoneyProps): Money {
    if (!props || typeof props !== 'object') {
      throw new InvalidMoneyError('MoneyProps must be a valid object');
    }

    const { amount, currency } = props;

    if (!currency || typeof currency !== 'string' || !this.CURRENCY_REGEX.test(currency.trim().toUpperCase())) {
      throw new InvalidMoneyError(`Invalid currency code: ${currency}`);
    }

    const normalizedCurrency = currency.trim().toUpperCase();

    if (typeof amount !== 'string') {
      throw new InvalidMoneyError(`Amount must be a string, received ${typeof amount}`);
    }

    const trimmedAmount = amount.trim();
    if (!trimmedAmount || !this.DECIMAL_REGEX.test(trimmedAmount)) {
      throw new InvalidMoneyError(
        `Amount '${amount}' must be a non-negative decimal string with at most 2 decimal places without scientific notation`,
      );
    }

    let decimalVal: Decimal;
    try {
      decimalVal = new Decimal(trimmedAmount);
      if (decimalVal.isNaN() || !decimalVal.isFinite()) {
        throw new InvalidMoneyError('Amount is NaN or Infinity');
      }
    } catch (err) {
      throw new InvalidMoneyError(`Failed to parse amount '${amount}': ${(err as Error).message}`);
    }

    return new Money(decimalVal, normalizedCurrency);
  }

  /**
   * Internal factory for rehydration or arithmetic results where negative values can exist internally
   */
  public static fromDecimal(decimal: Decimal, currency: string): Money {
    return new Money(decimal, currency);
  }

  public static zero(currency: string): Money {
    const normalizedCurrency = currency.trim().toUpperCase();
    if (!this.CURRENCY_REGEX.test(normalizedCurrency)) {
      throw new InvalidMoneyError(`Invalid currency code: ${currency}`);
    }
    return new Money(new Decimal(0), normalizedCurrency);
  }

  public add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  public subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.minus(other.value), this.currency);
  }

  public negate(): Money {
    return new Money(this.value.negated(), this.currency);
  }

  public isZero(): boolean {
    return this.value.isZero();
  }

  public isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  public isNegative(): boolean {
    return this.value.lessThan(0);
  }

  public isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  public equals(other: Money): boolean {
    if (this.currency !== other.currency) {
      return false;
    }
    return this.value.equals(other.value);
  }

  public get amount(): string {
    return this.value.toFixed(2);
  }

  public toJSON(): MoneyProps {
    return {
      amount: this.amount,
      currency: this.currency,
    };
  }

  public toString(): string {
    return `${this.amount} ${this.currency}`;
  }

  public assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }
}
