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
import {
  CreateWalletUseCase,
  WalletConflictError,
} from '../../src/application/use-cases/create-wallet.use-case';
import {
  ProcessWagerTransactionUseCase,
  WalletNotFoundError,
} from '../../src/application/use-cases/process-wager-transaction.use-case';
import { ReconcileWalletUseCase } from '../../src/application/use-cases/reconcile-wallet.use-case';

// In-Memory test implementations
class InMemoryWalletRepository implements WalletRepository {
  public wallets = new Map<string, Wallet>();

  async findById(id: string): Promise<Wallet | null> {
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
}

class InMemoryWagerTransactionRepository implements WagerTransactionRepository {
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

class InMemoryWalletLedgerRepository implements WalletLedgerRepository {
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

class InMemoryOutboxRepository implements OutboxRepository {
  public messages: OutboxMessage[] = [];

  async save(outbox: OutboxMessage): Promise<void> {
    this.messages.push(outbox);
  }

  async findPendingToDispatch(limit: number, now: Date): Promise<OutboxMessage[]> {
    return this.messages.filter((m) => m.isDue(now)).slice(0, limit);
  }
}

class InMemoryInboxRepository implements InboxRepository {
  public messages = new Map<string, InboxMessage>();

  async find(consumerName: string, messageId: string): Promise<InboxMessage | null> {
    return this.messages.get(`${consumerName}:${messageId}`) || null;
  }

  async save(inbox: InboxMessage): Promise<void> {
    this.messages.set(`${inbox.consumerName}:${inbox.messageId}`, inbox);
  }
}

class InMemoryUnitOfWork implements UnitOfWork {
  async transactional<T>(fn: () => Promise<T>): Promise<T> {
    return fn();
  }
}

describe('Use Cases Suite', () => {
  let walletRepo: InMemoryWalletRepository;
  let txRepo: InMemoryWagerTransactionRepository;
  let ledgerRepo: InMemoryWalletLedgerRepository;
  let outboxRepo: InMemoryOutboxRepository;
  let inboxRepo: InMemoryInboxRepository;
  let uow: InMemoryUnitOfWork;

  let createWalletUseCase: CreateWalletUseCase;
  let processWagerUseCase: ProcessWagerTransactionUseCase;
  let reconcileWalletUseCase: ReconcileWalletUseCase;

  beforeEach(() => {
    walletRepo = new InMemoryWalletRepository();
    txRepo = new InMemoryWagerTransactionRepository();
    ledgerRepo = new InMemoryWalletLedgerRepository();
    outboxRepo = new InMemoryOutboxRepository();
    inboxRepo = new InMemoryInboxRepository();
    uow = new InMemoryUnitOfWork();

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

  describe('CreateWalletUseCase', () => {
    it('should create wallet with initial balance, generating OPENING transaction and CREDIT ledger entry', async () => {
      const result = await createWalletUseCase.execute({
        playerId: 'player-1',
        initialBalance: { amount: '1000.00', currency: 'BRL' },
      });

      expect(result.playerId).toBe('player-1');
      expect(result.balance.amount).toBe('1000.00');
      expect(result.version).toBe(1);

      // Verificações de integridade
      expect(ledgerRepo.entries.length).toBe(1);
      expect(ledgerRepo.entries[0].direction).toBe(LedgerDirection.Credit);
      expect(ledgerRepo.entries[0].money.amount).toBe('1000.00');

      expect(outboxRepo.messages.length).toBe(1);
      expect(outboxRepo.messages[0].eventType).toBe('WalletBalanceChanged');
    });

    it('should throw WalletConflictError when creating duplicate wallet for same player and currency', async () => {
      await createWalletUseCase.execute({
        playerId: 'player-1',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      expect(
        createWalletUseCase.execute({
          playerId: 'player-1',
          initialBalance: { amount: '50.00', currency: 'BRL' },
        }),
      ).rejects.toThrow(WalletConflictError);
    });
  });

  describe('ProcessWagerTransactionUseCase', () => {
    let walletId: string;

    beforeEach(async () => {
      const w = await createWalletUseCase.execute({
        playerId: 'player-1',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });
      walletId = w.id;
    });

    it('should process BET with sufficient balance, deducting funds and creating ledger entry', async () => {
      const result = await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'tx-bet-1',
        idempotencyKey: 'prov-a:tx-bet-1',
        playerId: 'player-1',
        walletId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Bet,
        money: { amount: '35.00', currency: 'BRL' },
      });

      expect(result.status).toBe(WagerTransactionStatus.Processed);
      expect(result.balance.amount).toBe('65.00');
      expect(result.idempotentReplay).toBe(false);

      // Ledger: 1 OPENING (100.00) + 1 BET (35.00) = 2 entries
      expect(ledgerRepo.entries.length).toBe(2);
      expect(ledgerRepo.entries[1].direction).toBe(LedgerDirection.Debit);
      expect(ledgerRepo.entries[1].balanceAfter.amount).toBe('65.00');
    });

    it('should reject BET when balance is insufficient, leaving balance and ledger unchanged', async () => {
      const result = await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'tx-bet-2',
        idempotencyKey: 'prov-a:tx-bet-2',
        playerId: 'player-1',
        walletId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Bet,
        money: { amount: '150.00', currency: 'BRL' },
      });

      expect(result.status).toBe(WagerTransactionStatus.Rejected);
      expect(result.failureCode).toBe(FailureCode.INSUFFICIENT_FUNDS);
      expect(result.balance.amount).toBe('100.00');

      // Ledger must not contain any new entry
      expect(ledgerRepo.entries.length).toBe(1); // Only the initial OPENING
    });

    it('should return identical result with idempotentReplay: true on repeated request with same payload', async () => {
      const first = await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'tx-bet-3',
        idempotencyKey: 'prov-a:tx-bet-3',
        playerId: 'player-1',
        walletId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Bet,
        money: { amount: '20.00', currency: 'BRL' },
      });

      expect(first.idempotentReplay).toBe(false);
      expect(first.balance.amount).toBe('80.00');

      const replay = await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'tx-bet-3',
        idempotencyKey: 'prov-a:tx-bet-3',
        playerId: 'player-1',
        walletId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Bet,
        money: { amount: '20.00', currency: 'BRL' },
      });

      expect(replay.idempotentReplay).toBe(true);
      expect(replay.status).toBe(WagerTransactionStatus.Processed);
      expect(replay.balance.amount).toBe('80.00');
      // No duplicate debit!
      expect(ledgerRepo.entries.length).toBe(2);
    });

    it('should throw IdempotencyConflictError when same key is submitted with different payload', async () => {
      await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'tx-bet-4',
        idempotencyKey: 'prov-a:tx-bet-4',
        playerId: 'player-1',
        walletId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Bet,
        money: { amount: '20.00', currency: 'BRL' },
      });

      expect(
        processWagerUseCase.execute({
          providerId: 'prov-a',
          externalTransactionId: 'tx-bet-4',
          idempotencyKey: 'prov-a:tx-bet-4',
          playerId: 'player-1',
          walletId,
          roundId: 'round-1',
          gameId: 'game-1',
          kind: WagerTransactionKind.Bet,
          money: { amount: '25.00', currency: 'BRL' }, // Different amount!
        }),
      ).rejects.toThrow(IdempotencyConflictError);
    });

    it('should process WIN, crediting the wallet balance and adding CREDIT to ledger', async () => {
      const result = await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'tx-win-1',
        idempotencyKey: 'prov-a:tx-win-1',
        playerId: 'player-1',
        walletId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Win,
        money: { amount: '50.00', currency: 'BRL' },
      });

      expect(result.status).toBe(WagerTransactionStatus.Processed);
      expect(result.balance.amount).toBe('150.00');
    });

    it('should process LOSS without altering wallet balance or adding ledger entry', async () => {
      const result = await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'tx-loss-1',
        idempotencyKey: 'prov-a:tx-loss-1',
        playerId: 'player-1',
        walletId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Loss,
        money: { amount: '0.00', currency: 'BRL' },
      });

      expect(result.status).toBe(WagerTransactionStatus.Processed);
      expect(result.balance.amount).toBe('100.00');
      // No ledger entry added for LOSS
      expect(ledgerRepo.entries.length).toBe(1);
    });

    it('should process REFUND of a BET transaction', async () => {
      // 1. Process BET of 30.00
      await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'bet-ref-1',
        idempotencyKey: 'prov-a:bet-ref-1',
        playerId: 'player-1',
        walletId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Bet,
        money: { amount: '30.00', currency: 'BRL' },
      });

      // 2. Refund the BET
      const refundResult = await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'refund-1',
        idempotencyKey: 'prov-a:refund-1',
        playerId: 'player-1',
        walletId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Refund,
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: 'bet-ref-1',
      });

      expect(refundResult.status).toBe(WagerTransactionStatus.Processed);
      expect(refundResult.balance.amount).toBe('100.00'); // Back to 100.00
    });

    it('should mark REFUND as PENDING_REFERENCE if reference BET is not yet received', async () => {
      const result = await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'refund-out-of-order',
        idempotencyKey: 'prov-a:refund-out-of-order',
        playerId: 'player-1',
        walletId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Refund,
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: 'future-bet-tx',
      });

      expect(result.status).toBe(WagerTransactionStatus.PendingReference);
      // Balance remains 100.00 until reference is resolved
      expect(result.balance.amount).toBe('100.00');
    });
  });

  describe('ReconcileWalletUseCase', () => {
    it('should verify consistency when stored balance matches ledger sum exactly', async () => {
      const w = await createWalletUseCase.execute({
        playerId: 'player-1',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      await processWagerUseCase.execute({
        providerId: 'prov-a',
        externalTransactionId: 'bet-1',
        idempotencyKey: 'prov-a:bet-1',
        playerId: 'player-1',
        walletId: w.id,
        roundId: 'r-1',
        gameId: 'g-1',
        kind: WagerTransactionKind.Bet,
        money: { amount: '40.00', currency: 'BRL' },
      });

      const reconciliation = await reconcileWalletUseCase.execute(w.id);

      expect(reconciliation.consistent).toBe(true);
      expect(reconciliation.storedBalance.amount).toBe('60.00');
      expect(reconciliation.calculatedBalance.amount).toBe('60.00');
      expect(reconciliation.difference.amount).toBe('0.00');
      expect(reconciliation.checkedEntries).toBe(2);
    });
  });
});
