import { describe, expect, it } from 'bun:test';
import { CurrencyMismatchError, InsufficientBalanceError } from '../../src/domain/errors/domain.error';
import { Money } from '../../src/domain/models/money';
import { Wallet } from '../../src/domain/models/wallet';

describe('Wallet Aggregate Root', () => {
  const walletId = 'wallet-123';
  const playerId = 'player-456';

  it('should open a new wallet with initial balance and version 1', () => {
    const initialBalance = Money.from({ amount: '100.00', currency: 'BRL' });
    const wallet = Wallet.open({ id: walletId, playerId, initialBalance });

    expect(wallet.id).toBe(walletId);
    expect(wallet.playerId).toBe(playerId);
    expect(wallet.currency).toBe('BRL');
    expect(wallet.balance.amount).toBe('100.00');
    expect(wallet.version).toBe(1);
    expect(wallet.createdAt).toBeInstanceOf(Date);
    expect(wallet.updatedAt).toBeInstanceOf(Date);
  });

  it('should debit balance and increment version', () => {
    const wallet = Wallet.open({
      id: walletId,
      playerId,
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    const result = wallet.debit(Money.from({ amount: '35.00', currency: 'BRL' }));

    expect(result.previousBalance.amount).toBe('100.00');
    expect(result.newBalance.amount).toBe('65.00');
    expect(wallet.balance.amount).toBe('65.00');
    expect(wallet.version).toBe(2);
  });

  it('should credit balance and increment version', () => {
    const wallet = Wallet.open({
      id: walletId,
      playerId,
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    const result = wallet.credit(Money.from({ amount: '50.00', currency: 'BRL' }));

    expect(result.previousBalance.amount).toBe('100.00');
    expect(result.newBalance.amount).toBe('150.00');
    expect(wallet.balance.amount).toBe('150.00');
    expect(wallet.version).toBe(2);
  });

  it('should throw InsufficientBalanceError when debit exceeds balance', () => {
    const wallet = Wallet.open({
      id: walletId,
      playerId,
      initialBalance: Money.from({ amount: '50.00', currency: 'BRL' }),
    });

    expect(() => {
      wallet.debit(Money.from({ amount: '50.01', currency: 'BRL' }));
    }).toThrow(InsufficientBalanceError);

    // Balance and version must remain unchanged
    expect(wallet.balance.amount).toBe('50.00');
    expect(wallet.version).toBe(1);
  });

  it('should throw CurrencyMismatchError on debit or credit with different currency', () => {
    const wallet = Wallet.open({
      id: walletId,
      playerId,
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    const usdMoney = Money.from({ amount: '10.00', currency: 'USD' });
    expect(() => wallet.debit(usdMoney)).toThrow(CurrencyMismatchError);
    expect(() => wallet.credit(usdMoney)).toThrow(CurrencyMismatchError);
  });

  it('should rehydrate wallet accurately without revalidating initial transitions', () => {
    const now = new Date();
    const wallet = Wallet.rehydrate({
      id: walletId,
      playerId,
      currency: 'BRL',
      balance: Money.from({ amount: '250.00', currency: 'BRL' }),
      version: 5,
      createdAt: now,
      updatedAt: now,
    });

    expect(wallet.version).toBe(5);
    expect(wallet.balance.amount).toBe('250.00');
  });
});
