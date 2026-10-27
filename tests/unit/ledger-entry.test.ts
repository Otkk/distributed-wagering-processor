import { describe, expect, it } from 'bun:test';
import { InvalidLedgerEntryError } from '../../src/domain/errors/domain.error';
import { LedgerDirection } from '../../src/domain/models/ledger-direction';
import { Money } from '../../src/domain/models/money';
import { WalletLedgerEntry } from '../../src/domain/models/wallet-ledger-entry';

describe('WalletLedgerEntry Entity', () => {
  it('should create valid balanced DEBIT ledger entry', () => {
    const entry = WalletLedgerEntry.create({
      id: 'entry-1',
      walletId: 'w-1',
      transactionId: 'tx-1',
      direction: LedgerDirection.Debit,
      money: Money.from({ amount: '20.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '80.00', currency: 'BRL' }),
    });

    expect(entry.isBalanced()).toBe(true);
    expect(entry.direction).toBe(LedgerDirection.Debit);
  });

  it('should create valid balanced CREDIT ledger entry', () => {
    const entry = WalletLedgerEntry.create({
      id: 'entry-2',
      walletId: 'w-1',
      transactionId: 'tx-2',
      direction: LedgerDirection.Credit,
      money: Money.from({ amount: '50.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '150.00', currency: 'BRL' }),
    });

    expect(entry.isBalanced()).toBe(true);
    expect(entry.direction).toBe(LedgerDirection.Credit);
  });

  it('should throw InvalidLedgerEntryError if arithmetic does not match (unbalanced)', () => {
    expect(() =>
      WalletLedgerEntry.create({
        id: 'entry-3',
        walletId: 'w-1',
        transactionId: 'tx-3',
        direction: LedgerDirection.Debit,
        money: Money.from({ amount: '20.00', currency: 'BRL' }),
        balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
        balanceAfter: Money.from({ amount: '70.00', currency: 'BRL' }), // Incorrect: 100 - 20 !== 70
      }),
    ).toThrow(InvalidLedgerEntryError);
  });
});
