import { describe, expect, it } from 'bun:test';
import { CurrencyMismatchError, InvalidMoneyError } from '../../src/domain/errors/domain.error';
import { Money } from '../../src/domain/models/money';

describe('Money Value Object', () => {
  it('should create valid Money instance with 2 decimal places', () => {
    const money = Money.from({ amount: '25.00', currency: 'BRL' });
    expect(money.amount).toBe('25.00');
    expect(money.currency).toBe('BRL');
    expect(money.toString()).toBe('25.00 BRL');
    expect(money.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });

  it('should format 1 decimal place to 2 decimal places', () => {
    const money = Money.from({ amount: '25.5', currency: 'BRL' });
    expect(money.amount).toBe('25.50');
  });

  it('should format integer strings to 2 decimal places', () => {
    const money = Money.from({ amount: '100', currency: 'BRL' });
    expect(money.amount).toBe('100.00');
  });

  it('should reject invalid amounts: empty, negative, scientific, >2 decimals, non-numeric', () => {
    expect(() => Money.from({ amount: '', currency: 'BRL' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: '-10.00', currency: 'BRL' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: '1e5', currency: 'BRL' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: '25.999', currency: 'BRL' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: 'abc', currency: 'BRL' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: 'NaN', currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  it('should reject invalid currency codes', () => {
    expect(() => Money.from({ amount: '10.00', currency: 'BR' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: '10.00', currency: 'BRLX' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: '10.00', currency: '123' })).toThrow(InvalidMoneyError);
  });

  it('should create zero money', () => {
    const zero = Money.zero('BRL');
    expect(zero.amount).toBe('0.00');
    expect(zero.isZero()).toBe(true);
    expect(zero.isPositive()).toBe(false);
  });

  it('should add money of same currency correctly', () => {
    const m1 = Money.from({ amount: '25.50', currency: 'BRL' });
    const m2 = Money.from({ amount: '14.50', currency: 'BRL' });
    const sum = m1.add(m2);

    expect(sum.amount).toBe('40.00');
    expect(sum.currency).toBe('BRL');
    // Immutability check
    expect(m1.amount).toBe('25.50');
    expect(m2.amount).toBe('14.50');
  });

  it('should subtract money of same currency correctly', () => {
    const m1 = Money.from({ amount: '100.00', currency: 'BRL' });
    const m2 = Money.from({ amount: '80.00', currency: 'BRL' });
    const diff = m1.subtract(m2);

    expect(diff.amount).toBe('20.00');
    expect(diff.isPositive()).toBe(true);
  });

  it('should throw CurrencyMismatchError when operating on different currencies', () => {
    const brl = Money.from({ amount: '10.00', currency: 'BRL' });
    const usd = Money.from({ amount: '10.00', currency: 'USD' });

    expect(() => brl.add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.isLessThan(usd)).toThrow(CurrencyMismatchError);
    expect(brl.equals(usd)).toBe(false);
  });

  it('should correctly compare money instances', () => {
    const m1 = Money.from({ amount: '50.00', currency: 'BRL' });
    const m2 = Money.from({ amount: '100.00', currency: 'BRL' });
    const m3 = Money.from({ amount: '50.00', currency: 'BRL' });

    expect(m1.isLessThan(m2)).toBe(true);
    expect(m2.isLessThan(m1)).toBe(false);
    expect(m1.equals(m3)).toBe(true);
  });
});
