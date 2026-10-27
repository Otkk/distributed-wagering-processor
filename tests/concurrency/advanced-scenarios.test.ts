import { beforeEach, describe, expect, it } from 'bun:test';
import {
  FailureCode,
  IdempotencyConflictError,
} from '../../src/domain/errors/domain.error';
import { InboxMessage } from '../../src/domain/models/inbox-message';
import { LedgerDirection } from '../../src/domain/models/ledger-direction';
import { Money } from '../../src/domain/models/money';
import { OutboxMessage } from '../../src/domain/models/outbox-message';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/models/wager-transaction';
import { Wallet } from '../../src/domain/models/wallet';
import { WalletLedgerEntry } from '../../src/domain/models/wallet-ledger-entry';
import {
  InboxRepository,
  LedgerPaginationResult,
  OutboxRepository,
  UnitOfWork,
  WagerTransactionRepository,
  WalletLedgerRepository,
  WalletRepository,
} from '../../src/domain/ports/repositories.port';
import { CreateWalletUseCase } from '../../src/application/use-cases/create-wallet.use-case';
import { ProcessWagerTransactionUseCase } from '../../src/application/use-cases/process-wager-transaction.use-case';
import { ReconcileWalletUseCase } from '../../src/application/use-cases/reconcile-wallet.use-case';

// Lock Manager simulando SELECT ... FOR UPDATE a nível de linha no PostgreSQL
class RowLockManager {
  private locks = new Map<string, Promise<void>>();

  async acquire(key: string): Promise<() => void> {
    while (this.locks.has(key)) {
      await this.locks.get(key);
    }

    let releaseLock!: () => void;
    const lockPromise = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });

    this.locks.set(key, lockPromise);

    return () => {
      this.locks.delete(key);
      releaseLock();
    };
  }
}

class ConcurrentTestWalletRepository implements WalletRepository {
  public wallets = new Map<string, Wallet>();
  public currentLocks: Array<() => void> = [];

  constructor(private readonly lockManager: RowLockManager) {}

  async findById(id: string, lock?: 'pessimistic_write'): Promise<Wallet | null> {
    if (lock === 'pessimistic_write') {
      const release = await this.lockManager.acquire(id);
      this.currentLocks.push(release);
    }
    // Simula delay de I/O de rede/disco para provocar race conditions se não houver lock
    await new Promise((r) => setTimeout(r, 5));
    return this.wallets.get(id) || null;
  }

  async findByPlayerIdAndCurrency(playerId: string, currency: string): Promise<Wallet | null> {
    for (const w of this.wallets.values()) {
      if (w.playerId === playerId && w.currency === currency) {
        return w;
      }
    }
    return null;
  }

  async save(wallet: Wallet): Promise<void> {
    this.wallets.set(wallet.id, wallet);
  }

  releaseAllLocks(): void {
    while (this.currentLocks.length > 0) {
      const release = this.currentLocks.pop();
      if (release) release();
    }
  }
}

class ConcurrentTestTxRepository implements WagerTransactionRepository {
  public transactions = new Map<string, WagerTransaction>();

  async findById(id: string): Promise<WagerTransaction | null> {
    return this.transactions.get(id) || null;
  }

  async findByIdempotencyKey(key: string): Promise<WagerTransaction | null> {
    for (const tx of this.transactions.values()) {
      if (tx.idempotencyKey === key) return tx;
    }
    return null;
  }

  async findByProviderAndExternalId(providerId: string, externalId: string): Promise<WagerTransaction | null> {
    for (const tx of this.transactions.values()) {
      if (tx.providerId === providerId && tx.externalTransactionId === externalId) {
        return tx;
      }
    }
    return null;
  }

  async findPendingReferences(limit: number): Promise<WagerTransaction[]> {
    return Array.from(this.transactions.values())
      .filter((t) => t.status === WagerTransactionStatus.PendingReference)
      .slice(0, limit);
  }

  async save(transaction: WagerTransaction): Promise<void> {
    this.transactions.set(transaction.id, transaction);
  }
}

class ConcurrentTestLedgerRepository implements WalletLedgerRepository {
  public entries: WalletLedgerEntry[] = [];

  async save(entry: WalletLedgerEntry): Promise<void> {
    this.entries.push(entry);
  }

  async findByWalletId(
    walletId: string,
    options?: { limit: number; cursor?: string },
  ): Promise<LedgerPaginationResult> {
    const list = this.entries.filter((e) => e.walletId === walletId);
    return { entries: list.slice(0, options?.limit || 50) };
  }

  async calculateBalance(walletId: string): Promise<{ calculatedAmount: string; count: number }> {
    const walletEntries = this.entries.filter((e) => e.walletId === walletId);
    let total = Money.zero('BRL');

    for (const e of walletEntries) {
      if (e.direction === LedgerDirection.Credit) {
        total = total.add(e.money);
      } else {
        total = total.subtract(e.money);
      }
    }

    return { calculatedAmount: total.amount, count: walletEntries.length };
  }
}

class ConcurrentTestOutboxRepository implements OutboxRepository {
  public messages: OutboxMessage[] = [];

  async save(outbox: OutboxMessage): Promise<void> {
    this.messages.push(outbox);
  }

  async findPendingToDispatch(limit: number, now: Date): Promise<OutboxMessage[]> {
    return this.messages.filter((m) => m.isDue(now)).slice(0, limit);
  }
}

class ConcurrentTestInboxRepository implements InboxRepository {
  public messages = new Map<string, InboxMessage>();

  async find(consumerName: string, messageId: string): Promise<InboxMessage | null> {
    return this.messages.get(`${consumerName}:${messageId}`) || null;
  }

  async save(inbox: InboxMessage): Promise<void> {
    this.messages.set(`${inbox.consumerName}:${inbox.messageId}`, inbox);
  }
}

class ConcurrentTestUnitOfWork implements UnitOfWork {
  constructor(private readonly walletRepo: ConcurrentTestWalletRepository) {}

  async transactional<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } finally {
      this.walletRepo.releaseAllLocks();
    }
  }
}

describe('Advanced Concurrency & Real-World Wagering Scenarios', () => {
  let lockManager: RowLockManager;
  let walletRepo: ConcurrentTestWalletRepository;
  let txRepo: ConcurrentTestTxRepository;
  let ledgerRepo: ConcurrentTestLedgerRepository;
  let outboxRepo: ConcurrentTestOutboxRepository;
  let inboxRepo: ConcurrentTestInboxRepository;
  let uow: ConcurrentTestUnitOfWork;

  let createWalletUseCase: CreateWalletUseCase;
  let processWagerUseCase: ProcessWagerTransactionUseCase;
  let reconcileWalletUseCase: ReconcileWalletUseCase;

  beforeEach(() => {
    lockManager = new RowLockManager();
    walletRepo = new ConcurrentTestWalletRepository(lockManager);
    txRepo = new ConcurrentTestTxRepository();
    ledgerRepo = new ConcurrentTestLedgerRepository();
    outboxRepo = new ConcurrentTestOutboxRepository();
    inboxRepo = new ConcurrentTestInboxRepository();
    uow = new ConcurrentTestUnitOfWork(walletRepo);

    createWalletUseCase = new CreateWalletUseCase(walletRepo, txRepo, ledgerRepo, outboxRepo, uow);
    processWagerUseCase = new ProcessWagerTransactionUseCase(
      walletRepo,
      txRepo,
      ledgerRepo,
      outboxRepo,
      inboxRepo,
      uow,
    );
    reconcileWalletUseCase = new ReconcileWalletUseCase(walletRepo, ledgerRepo);
  });

  describe('Cenário 1: Concorrência de Saldo na Mesma Carteira (Hot Wallet — Seção 8)', () => {
    it('deve aceitar exatamente uma aposta e rejeitar a outra com saldo final de 20.00 BRL', async () => {
      // Saldo inicial de 100.00 BRL
      const wallet = await createWalletUseCase.execute({
        playerId: 'player-hot-wallet',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      // Dispara 2 apostas de 80.00 simultâneas em paralelo verdadeiro
      const [res1, res2] = await Promise.all([
        processWagerUseCase.execute({
          providerId: 'provider-a',
          externalTransactionId: 'bet-simul-1',
          idempotencyKey: 'prov-a:bet-simul-1',
          playerId: 'player-hot-wallet',
          walletId: wallet.id,
          roundId: 'round-1',
          gameId: 'fortune-tiger',
          kind: WagerTransactionKind.Bet,
          money: { amount: '80.00', currency: 'BRL' },
        }),
        processWagerUseCase.execute({
          providerId: 'provider-a',
          externalTransactionId: 'bet-simul-2',
          idempotencyKey: 'prov-a:bet-simul-2',
          playerId: 'player-hot-wallet',
          walletId: wallet.id,
          roundId: 'round-1',
          gameId: 'fortune-tiger',
          kind: WagerTransactionKind.Bet,
          money: { amount: '80.00', currency: 'BRL' },
        }),
      ]);

      const statuses = [res1.status, res2.status];
      expect(statuses).toContain(WagerTransactionStatus.Processed);
      expect(statuses).toContain(WagerTransactionStatus.Rejected);

      // A aposta rejeitada deve ter o código INSUFFICIENT_FUNDS
      const rejectedResult = res1.status === WagerTransactionStatus.Rejected ? res1 : res2;
      expect(rejectedResult.failureCode).toBe(FailureCode.INSUFFICIENT_FUNDS);

      // Saldo final deve ser exatamente 20.00 BRL
      const finalWallet = await walletRepo.findById(wallet.id);
      expect(finalWallet!.balance.amount).toBe('20.00');

      // Ledger: 1 OPENING (100.00) + exatamente 1 DEBIT (80.00) = 2 entradas
      expect(ledgerRepo.entries.length).toBe(2);
      expect(ledgerRepo.entries[1].direction).toBe(LedgerDirection.Debit);
      expect(ledgerRepo.entries[1].money.amount).toBe('80.00');

      // Invariante universal contábil: Saldo da carteira == somatório do ledger
      const reconciliation = await reconcileWalletUseCase.execute(wallet.id);
      expect(reconciliation.consistent).toBe(true);
      expect(reconciliation.difference.amount).toBe('0.00');
    });
  });

  describe('Cenário 2: Tempestade de Idempotência (50x em Paralelo)', () => {
    it('deve debitar exatamente uma vez e responder 49x com idempotentReplay: true', async () => {
      const wallet = await createWalletUseCase.execute({
        playerId: 'player-storm',
        initialBalance: { amount: '500.00', currency: 'BRL' },
      });

      const promises = Array.from({ length: 50 }, (_, i) =>
        processWagerUseCase.execute({
          providerId: 'provider-fast',
          externalTransactionId: 'tx-storm-constant',
          idempotencyKey: 'provider-fast:tx-storm-constant',
          playerId: 'player-storm',
          walletId: wallet.id,
          roundId: 'round-storm',
          gameId: 'aviator',
          kind: WagerTransactionKind.Bet,
          money: { amount: '50.00', currency: 'BRL' },
        }),
      );

      const results = await Promise.all(promises);

      // Todas devem ter sucesso
      for (const res of results) {
        expect(res.status).toBe(WagerTransactionStatus.Processed);
        expect(res.balance.amount).toBe('450.00');
      }

      // Exatamente 1 deve ser idempotentReplay: false e 49 devem ser idempotentReplay: true
      const originalCalls = results.filter((r) => !r.idempotentReplay);
      const replayCalls = results.filter((r) => r.idempotentReplay);

      expect(originalCalls.length).toBe(1);
      expect(replayCalls.length).toBe(49);

      // O saldo final deve ser exatamente 450.00 (debitou apenas uma vez)
      const finalWallet = await walletRepo.findById(wallet.id);
      expect(finalWallet!.balance.amount).toBe('450.00');

      // O ledger deve ter apenas 2 lançamentos (OPENING + 1 único débito da aposta)
      expect(ledgerRepo.entries.length).toBe(2);

      // Reconciliação contábil perfeita
      const rec = await reconcileWalletUseCase.execute(wallet.id);
      expect(rec.consistent).toBe(true);
      expect(rec.difference.amount).toBe('0.00');
    });
  });

  describe('Cenário 3: Ciclo Completo de Rodada de Cassino (BET -> WIN -> ROLLBACK)', () => {
    it('deve aplicar aposta, vitória e posterior rollback da vitória mantendo a reconciliação perfeita', async () => {
      // 1. Abertura com 500.00 BRL
      const wallet = await createWalletUseCase.execute({
        playerId: 'player-vip',
        initialBalance: { amount: '500.00', currency: 'BRL' },
      });

      // 2. Aposta de 100.00 BRL -> Saldo vai para 400.00 BRL
      const bet = await processWagerUseCase.execute({
        providerId: 'provider-vip',
        externalTransactionId: 'vip-bet-1',
        idempotencyKey: 'provider-vip:vip-bet-1',
        playerId: 'player-vip',
        walletId: wallet.id,
        roundId: 'round-vip-1',
        gameId: 'roulette',
        kind: WagerTransactionKind.Bet,
        money: { amount: '100.00', currency: 'BRL' },
      });
      expect(bet.balance.amount).toBe('400.00');

      // 3. Prêmio WIN de 300.00 BRL -> Saldo vai para 700.00 BRL
      const win = await processWagerUseCase.execute({
        providerId: 'provider-vip',
        externalTransactionId: 'vip-win-1',
        idempotencyKey: 'provider-vip:vip-win-1',
        playerId: 'player-vip',
        walletId: wallet.id,
        roundId: 'round-vip-1',
        gameId: 'roulette',
        kind: WagerTransactionKind.Win,
        money: { amount: '300.00', currency: 'BRL' },
      });
      expect(win.balance.amount).toBe('700.00');

      // 4. ROLLBACK do WIN (por exemplo, decisão do provedor por rodada invalidada)
      // Reverter o WIN significa debitar os 300.00 concedidos -> Saldo volta para 400.00 BRL
      const rollback = await processWagerUseCase.execute({
        providerId: 'provider-vip',
        externalTransactionId: 'vip-rollback-win',
        idempotencyKey: 'provider-vip:vip-rollback-win',
        playerId: 'player-vip',
        walletId: wallet.id,
        roundId: 'round-vip-1',
        gameId: 'roulette',
        kind: WagerTransactionKind.Rollback,
        money: { amount: '300.00', currency: 'BRL' },
        referenceExternalTransactionId: 'vip-win-1',
      });

      expect(rollback.status).toBe(WagerTransactionStatus.Processed);
      expect(rollback.balance.amount).toBe('400.00');

      // Validação final de consistência do ledger:
      // Saldo final 400.00 BRL == +500 (OPENING) -100 (BET) +300 (WIN) -300 (ROLLBACK)
      const rec = await reconcileWalletUseCase.execute(wallet.id);
      expect(rec.consistent).toBe(true);
      expect(rec.storedBalance.amount).toBe('400.00');
      expect(rec.calculatedBalance.amount).toBe('400.00');
      expect(rec.difference.amount).toBe('0.00');
      expect(rec.checkedEntries).toBe(4);
    });
  });

  describe('Cenário 4: Prevenção de Saldo Negativo em Rollback (Underflow Protection)', () => {
    it('deve rejeitar ROLLBACK com NEGATIVE_BALANCE_PREVENTED quando o usuário já gastou o prêmio', async () => {
      // 1. Saldo inicial zero
      const wallet = await createWalletUseCase.execute({
        playerId: 'player-gambler',
        initialBalance: { amount: '0.00', currency: 'BRL' },
      });

      // 2. Jogador ganha 100.00 BRL -> Saldo = 100.00
      await processWagerUseCase.execute({
        providerId: 'prov-x',
        externalTransactionId: 'win-100',
        idempotencyKey: 'prov-x:win-100',
        playerId: 'player-gambler',
        walletId: wallet.id,
        roundId: 'r-1',
        gameId: 'slots',
        kind: WagerTransactionKind.Win,
        money: { amount: '100.00', currency: 'BRL' },
      });

      // 3. Jogador aposta 90.00 BRL em outro jogo -> Saldo = 10.00 BRL
      await processWagerUseCase.execute({
        providerId: 'prov-x',
        externalTransactionId: 'bet-90',
        idempotencyKey: 'prov-x:bet-90',
        playerId: 'player-gambler',
        walletId: wallet.id,
        roundId: 'r-2',
        gameId: 'blackjack',
        kind: WagerTransactionKind.Bet,
        money: { amount: '90.00', currency: 'BRL' },
      });

      // 4. Provedor envia ROLLBACK do prêmio de 100.00.
      // Como o saldo atual é 10.00, debitar 100.00 deixaria saldo em -90.00 (inaceitável!)
      const rollback = await processWagerUseCase.execute({
        providerId: 'prov-x',
        externalTransactionId: 'rollback-win-100',
        idempotencyKey: 'prov-x:rollback-win-100',
        playerId: 'player-gambler',
        walletId: wallet.id,
        roundId: 'r-1',
        gameId: 'slots',
        kind: WagerTransactionKind.Rollback,
        money: { amount: '100.00', currency: 'BRL' },
        referenceExternalTransactionId: 'win-100',
      });

      // Deve ser rejeitado com NEGATIVE_BALANCE_PREVENTED
      expect(rollback.status).toBe(WagerTransactionStatus.Rejected);
      expect(rollback.failureCode).toBe(FailureCode.NEGATIVE_BALANCE_PREVENTED);
      expect(rollback.balance.amount).toBe('10.00');

      // O saldo permanece intacto em 10.00 BRL
      const finalWallet = await walletRepo.findById(wallet.id);
      expect(finalWallet!.balance.amount).toBe('10.00');
    });
  });

  describe('Cenário 5: Entrega Fora de Ordem e Resolução de PENDING_REFERENCE', () => {
    it('deve armazenar REFUND como PENDING_REFERENCE quando enviado antes da BET e resolver normalmente', async () => {
      const wallet = await createWalletUseCase.execute({
        playerId: 'player-async',
        initialBalance: { amount: '200.00', currency: 'BRL' },
      });

      // 1. REFUND chega antes da BET!
      const refundEarly = await processWagerUseCase.execute({
        providerId: 'prov-fast',
        externalTransactionId: 'refund-early-1',
        idempotencyKey: 'prov-fast:refund-early-1',
        playerId: 'player-async',
        walletId: wallet.id,
        roundId: 'round-async-1',
        gameId: 'crash',
        kind: WagerTransactionKind.Refund,
        money: { amount: '50.00', currency: 'BRL' },
        referenceExternalTransactionId: 'bet-delayed-1',
      });

      expect(refundEarly.status).toBe(WagerTransactionStatus.PendingReference);
      expect(refundEarly.balance.amount).toBe('200.00');

      // 2. BET chega depois e é processada normalmente
      const bet = await processWagerUseCase.execute({
        providerId: 'prov-fast',
        externalTransactionId: 'bet-delayed-1',
        idempotencyKey: 'prov-fast:bet-delayed-1',
        playerId: 'player-async',
        walletId: wallet.id,
        roundId: 'round-async-1',
        gameId: 'crash',
        kind: WagerTransactionKind.Bet,
        money: { amount: '50.00', currency: 'BRL' },
      });

      expect(bet.status).toBe(WagerTransactionStatus.Processed);
      expect(bet.balance.amount).toBe('150.00');

      // Agora a transação de referência 'bet-delayed-1' existe no banco!
      const pendingRefs = await txRepo.findPendingReferences(10);
      expect(pendingRefs.length).toBe(1);
      expect(pendingRefs[0].idempotencyKey).toBe('prov-fast:refund-early-1');
    });
  });
});
