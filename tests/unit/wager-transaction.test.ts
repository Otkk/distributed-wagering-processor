import { describe, expect, it } from 'bun:test';
import {
  FailureCode,
  InvalidTransactionReferenceError,
  InvalidTransactionStateError,
} from '../../src/domain/errors/domain.error';
import { LedgerDirection } from '../../src/domain/models/ledger-direction';
import { Money } from '../../src/domain/models/money';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/models/wager-transaction';

describe('WagerTransaction Entity', () => {
  const baseProps = {
    id: 'tx-1',
    providerId: 'prov-a',
    externalTransactionId: 'ext-100',
    idempotencyKey: 'prov-a:ext-100',
    payloadHash: 'hash123',
    walletId: 'w-1',
    playerId: 'p-1',
    roundId: 'r-1',
    gameId: 'g-1',
    money: Money.from({ amount: '25.00', currency: 'BRL' }),
  };

  it('should create BET transaction in PENDING status', () => {
    const tx = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Bet,
    });

    expect(tx.status).toBe(WagerTransactionStatus.Pending);
    expect(tx.isTerminal()).toBe(false);
    expect(tx.affectsBalance()).toBe(true);
    expect(tx.requiresReference()).toBe(false);
    expect(tx.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
  });

  it('should require referenceExternalTransactionId for REFUND and ROLLBACK', () => {
    expect(() =>
      WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: '',
      }),
    ).toThrow(InvalidTransactionReferenceError);

    expect(() =>
      WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: undefined,
      }),
    ).toThrow(InvalidTransactionReferenceError);
  });

  it('should transition to PROCESSED and record processedAt and referenceId', () => {
    const tx = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Bet,
    });

    const now = new Date();
    tx.markProcessed(undefined, now);

    expect(tx.status).toBe(WagerTransactionStatus.Processed);
    expect(tx.isTerminal()).toBe(true);
    expect(tx.processedAt).toBe(now);

    // Cannot transition further from terminal state
    expect(() => tx.reject(FailureCode.INSUFFICIENT_FUNDS)).toThrow(
      InvalidTransactionStateError,
    );
  });

  it('should transition to REJECTED with FailureCode', () => {
    const tx = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Bet,
    });

    tx.reject(FailureCode.INSUFFICIENT_FUNDS);

    expect(tx.status).toBe(WagerTransactionStatus.Rejected);
    expect(tx.failureCode).toBe(FailureCode.INSUFFICIENT_FUNDS);
    expect(tx.isTerminal()).toBe(true);
  });

  it('should determine correct ledger direction for ROLLBACK depending on reference', () => {
    const rollbackTx = WagerTransaction.create({
      ...baseProps,
      id: 'tx-rollback',
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'ext-bet',
    });

    const betTx = WagerTransaction.create({
      ...baseProps,
      id: 'tx-bet',
      kind: WagerTransactionKind.Bet,
    });

    const winTx = WagerTransaction.create({
      ...baseProps,
      id: 'tx-win',
      kind: WagerTransactionKind.Win,
    });

    // Rollback of BET (debit) -> CREDIT
    expect(rollbackTx.ledgerDirectionFor(betTx)).toBe(LedgerDirection.Credit);
    // Rollback of WIN (credit) -> DEBIT
    expect(rollbackTx.ledgerDirectionFor(winTx)).toBe(LedgerDirection.Debit);
  });

  it('should throw error when requesting ledger direction for LOSS transaction', () => {
    const lossTx = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Loss,
    });

    expect(lossTx.affectsBalance()).toBe(false);
    expect(() => lossTx.ledgerDirectionFor()).toThrow();
  });
});
