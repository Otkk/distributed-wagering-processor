import { describe, expect, it } from 'bun:test';
import {
  CurrencyMismatchError,
  FailureCode,
  IdempotencyConflictError,
  InsufficientBalanceError,
  InvalidLedgerEntryError,
  InvalidMoneyError,
  InvalidTransactionStateError,
} from '../../src/domain/errors/domain.error';
import { canonicalJson, computePayloadHash } from '../../src/domain/models/canonical-hash';
import { LedgerDirection } from '../../src/domain/models/ledger-direction';
import { Money } from '../../src/domain/models/money';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/models/wager-transaction';
import { Wallet } from '../../src/domain/models/wallet';
import { WalletLedgerEntry } from '../../src/domain/models/wallet-ledger-entry';

describe('Premature Failure & Eliminatory Traps Suite', () => {
  describe('Armadilha 1: Ponto Flutuante e Precisão Monetária (IEEE-754)', () => {
    it('deve somar 0.10 + 0.20 resultando em exatamente 0.30 (e não 0.30000000000000004)', () => {
      // No JavaScript nativo: 0.1 + 0.2 === 0.30000000000000004
      const m1 = Money.from({ amount: '0.10', currency: 'BRL' });
      const m2 = Money.from({ amount: '0.20', currency: 'BRL' });
      const sum = m1.add(m2);

      expect(sum.amount).toBe('0.30');
      expect(sum.amount).not.toBe('0.30000000000000004');
    });

    it('deve rejeitar sub-centavos (frações menores que 2 casas decimais)', () => {
      expect(() => Money.from({ amount: '10.001', currency: 'BRL' })).toThrow(InvalidMoneyError);
      expect(() => Money.from({ amount: '0.999', currency: 'BRL' })).toThrow(InvalidMoneyError);
    });

    it('deve rejeitar valores com notação científica e overflows numéricos', () => {
      expect(() => Money.from({ amount: '1e-2', currency: 'BRL' })).toThrow(InvalidMoneyError);
      expect(() => Money.from({ amount: 'Infinity', currency: 'BRL' })).toThrow(InvalidMoneyError);
    });
  });

  describe('Armadilha 2: Saldo Negativo e Underflow de Carteira', () => {
    it('deve impedir débito que resulte em qualquer saldo negativo (mesmo por 0.01 centavo)', () => {
      const wallet = Wallet.open({
        id: 'w-1',
        playerId: 'p-1',
        initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
      });

      // Tenta debitar 100.01 tendo apenas 100.00
      expect(() => {
        wallet.debit(Money.from({ amount: '100.01', currency: 'BRL' }));
      }).toThrow(InsufficientBalanceError);

      // O saldo permanece estritamente intacto
      expect(wallet.balance.amount).toBe('100.00');
      expect(wallet.version).toBe(1);
    });
  });

  describe('Armadilha 3: Reversão / Adulteração Contábil no Ledger', () => {
    it('deve falhar imediatamente se houver divergência aritmética entre Saldo Anterior, Movimentação e Saldo Posterior', () => {
      // Simula uma tentativa de fraudar ou registrar um crédito com saldo posterior incorreto
      expect(() => {
        WalletLedgerEntry.create({
          id: 'ledger-err-1',
          walletId: 'w-1',
          transactionId: 'tx-1',
          direction: LedgerDirection.Debit,
          money: Money.from({ amount: '30.00', currency: 'BRL' }),
          balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
          balanceAfter: Money.from({ amount: '70.01', currency: 'BRL' }), // Erro de 0.01
        });
      }).toThrow(InvalidLedgerEntryError);
    });
  });

  describe('Armadilha 4: Mutação de Estados Terminais em Transações', () => {
    it('deve impedir estritamente alterar uma transação já PROCESSED, REJECTED ou FAILED', () => {
      const tx = WagerTransaction.create({
        id: 'tx-term-1',
        providerId: 'prov-a',
        externalTransactionId: 'ext-1',
        idempotencyKey: 'prov-a:ext-1',
        payloadHash: 'hash-abc',
        walletId: 'w-1',
        playerId: 'p-1',
        roundId: 'r-1',
        gameId: 'g-1',
        kind: WagerTransactionKind.Bet,
        money: Money.from({ amount: '50.00', currency: 'BRL' }),
      });

      // Transiciona para PROCESSED
      tx.markProcessed(undefined, new Date());
      expect(tx.isTerminal()).toBe(true);

      // Tentativas posteriores de mutação devem lançar erro de estado inválido
      expect(() => tx.markProcessed(undefined, new Date())).toThrow(InvalidTransactionStateError);
      expect(() => tx.reject(FailureCode.INSUFFICIENT_FUNDS)).toThrow(InvalidTransactionStateError);
      expect(() => tx.fail(FailureCode.INTERNAL_ERROR)).toThrow(InvalidTransactionStateError);
      expect(() => tx.markPendingReference()).toThrow(InvalidTransactionStateError);
    });
  });

  describe('Armadilha 5: Adulteração de Payload sob a Mesma Chave de Idempotência', () => {
    it('deve detectar alteração de valor ou parâmetro gerando hash divergente', () => {
      const originalPayload = {
        providerId: 'provider-a',
        externalTransactionId: 'ext-999',
        walletId: 'w-1',
        amount: '25.00',
        currency: 'BRL',
      };

      const alteredPayload = {
        providerId: 'provider-a',
        externalTransactionId: 'ext-999',
        walletId: 'w-1',
        amount: '25.01', // Alterou 1 centavo
        currency: 'BRL',
      };

      const hashOriginal = computePayloadHash(originalPayload);
      const hashAltered = computePayloadHash(alteredPayload);

      expect(hashOriginal).not.toBe(hashAltered);

      // Transação criada com o hash original deve rejeitar o hash adulterado
      const tx = WagerTransaction.create({
        id: 'tx-idem-1',
        providerId: 'provider-a',
        externalTransactionId: 'ext-999',
        idempotencyKey: 'provider-a:ext-999',
        payloadHash: hashOriginal,
        walletId: 'w-1',
        playerId: 'p-1',
        roundId: 'r-1',
        gameId: 'g-1',
        kind: WagerTransactionKind.Bet,
        money: Money.from({ amount: '25.00', currency: 'BRL' }),
      });

      expect(tx.matchesPayload(hashOriginal)).toBe(true);
      expect(tx.matchesPayload(hashAltered)).toBe(false);
    });
  });

  describe('Armadilha 6: Mistura e Contaminação de Moedas (Multi-currency safety)', () => {
    it('deve impedir operações entre carteira BRL e valores em USD', () => {
      const wallet = Wallet.open({
        id: 'w-brl',
        playerId: 'p-1',
        initialBalance: Money.from({ amount: '500.00', currency: 'BRL' }),
      });

      const usdMoney = Money.from({ amount: '10.00', currency: 'USD' });

      expect(() => wallet.debit(usdMoney)).toThrow(CurrencyMismatchError);
      expect(() => wallet.credit(usdMoney)).toThrow(CurrencyMismatchError);
    });
  });
});
