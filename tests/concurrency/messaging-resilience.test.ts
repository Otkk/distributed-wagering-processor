import { beforeEach, describe, expect, it } from 'bun:test';
import { CreateWalletUseCase } from '../../src/application/use-cases/create-wallet.use-case';
import { ProcessWagerTransactionUseCase } from '../../src/application/use-cases/process-wager-transaction.use-case';
import { ReconcileWalletUseCase } from '../../src/application/use-cases/reconcile-wallet.use-case';
import { FailureCode } from '../../src/domain/errors/domain.error';
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
import { AppLoggerService } from '../../src/infrastructure/logger/app-logger.service';
import { OutboxDispatcherWorker } from '../../src/infrastructure/messaging/outbox/outbox-dispatcher.worker';
import { PendingReferenceWorker } from '../../src/infrastructure/messaging/outbox/pending-reference.worker';
import { EventPublisherPort } from '../../src/infrastructure/messaging/sqs/sqs-event-publisher';
import {
  SqsConsumerClientPort,
  SqsRawMessage,
  WagerTransactionConsumer,
} from '../../src/infrastructure/messaging/sqs/wager-transaction.consumer';
import { MetricsService } from '../../src/infrastructure/metrics/metrics.service';

class SkipLockedOutboxRepository implements OutboxRepository {
  public messages = new Map<string, OutboxMessage>();
  private lockedIds = new Set<string>();

  async save(outbox: OutboxMessage): Promise<void> {
    this.messages.set(outbox.id, outbox);
  }

  // Simula exatamente o comportamento de FOR UPDATE SKIP LOCKED no PostgreSQL
  async findPendingToDispatch(limit: number, now: Date): Promise<OutboxMessage[]> {
    const selected: OutboxMessage[] = [];
    for (const msg of this.messages.values()) {
      if (selected.length >= limit) break;
      if (msg.isDue(now) && !this.lockedIds.has(msg.id)) {
        this.lockedIds.add(msg.id);
        selected.push(msg);
      }
    }
    return selected;
  }

  releaseLocks(): void {
    this.lockedIds.clear();
  }
}

class ResilienceTxRepository implements WagerTransactionRepository {
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

  async findByProviderAndExternalId(
    providerId: string,
    externalId: string,
  ): Promise<WagerTransaction | null> {
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

  async findProcessedReversal(
    referenceTransactionId: string,
    kind: string,
  ): Promise<WagerTransaction | null> {
    for (const tx of this.transactions.values()) {
      if (
        tx.referenceTransactionId === referenceTransactionId &&
        tx.kind === kind &&
        tx.status === WagerTransactionStatus.Processed
      ) {
        return tx;
      }
    }
    return null;
  }

  async save(transaction: WagerTransaction): Promise<void> {
    this.transactions.set(transaction.id, transaction);
  }
}

class ResilienceWalletRepository implements WalletRepository {
  public wallets = new Map<string, Wallet>();

  async findById(id: string): Promise<Wallet | null> {
    return this.wallets.get(id) || null;
  }

  async findByPlayerIdAndCurrency(playerId: string, currency: string): Promise<Wallet | null> {
    for (const w of this.wallets.values()) {
      if (w.playerId === playerId && w.currency === currency) return w;
    }
    return null;
  }

  async save(wallet: Wallet): Promise<void> {
    this.wallets.set(wallet.id, wallet);
  }
}

class ResilienceLedgerRepository implements WalletLedgerRepository {
  public entries: WalletLedgerEntry[] = [];

  async save(entry: WalletLedgerEntry): Promise<void> {
    this.entries.push(entry);
  }

  async findByWalletId(
    walletId: string,
    options?: { limit: number; cursor?: string },
  ): Promise<LedgerPaginationResult> {
    return {
      entries: this.entries.filter((e) => e.walletId === walletId).slice(0, options?.limit || 50),
    };
  }

  async calculateBalance(walletId: string): Promise<{ calculatedAmount: string; count: number }> {
    const list = this.entries.filter((e) => e.walletId === walletId);
    let total = Money.zero('BRL');
    for (const e of list) {
      total = e.direction === LedgerDirection.Credit ? total.add(e.money) : total.subtract(e.money);
    }
    return { calculatedAmount: total.amount, count: list.length };
  }
}

class ResilienceInboxRepository implements InboxRepository {
  public messages = new Map<string, InboxMessage>();

  async find(consumerName: string, messageId: string): Promise<InboxMessage | null> {
    return this.messages.get(`${consumerName}:${messageId}`) || null;
  }

  async save(inbox: InboxMessage): Promise<void> {
    this.messages.set(`${inbox.consumerName}:${inbox.messageId}`, inbox);
  }
}

class FakeSqsClient implements SqsConsumerClientPort {
  public deletedReceipts: string[] = [];
  public visibilityChanges: Array<{ receiptHandle: string; timeout: number }> = [];
  public dlqMessages: Array<{ body: string; groupId: string; dedupId: string }> = [];
  public failDeleteOnce = false;

  async receiveMessages(): Promise<SqsRawMessage[]> {
    return [];
  }

  async deleteMessage(_queueUrl: string, receiptHandle: string): Promise<void> {
    if (this.failDeleteOnce) {
      this.failDeleteOnce = false;
      throw new Error('Simulated worker crash before SQS ACK');
    }
    this.deletedReceipts.push(receiptHandle);
  }

  async changeMessageVisibility(
    _queueUrl: string,
    receiptHandle: string,
    visibilityTimeout: number,
  ): Promise<void> {
    this.visibilityChanges.push({ receiptHandle, timeout: visibilityTimeout });
  }

  async sendToDlq(
    _dlqUrl: string,
    body: string,
    groupId: string,
    dedupId: string,
  ): Promise<void> {
    this.dlqMessages.push({ body, groupId, dedupId });
  }
}

describe('Phase 5: Messaging, Outbox, Crash Recovery & Pending Reference Workers', () => {
  let walletRepo: ResilienceWalletRepository;
  let txRepo: ResilienceTxRepository;
  let ledgerRepo: ResilienceLedgerRepository;
  let outboxRepo: SkipLockedOutboxRepository;
  let inboxRepo: ResilienceInboxRepository;
  let uow: UnitOfWork;
  let logger: AppLoggerService;
  let metrics: MetricsService;
  let sqsClient: FakeSqsClient;

  let createWalletUseCase: CreateWalletUseCase;
  let processWagerUseCase: ProcessWagerTransactionUseCase;
  let reconcileUseCase: ReconcileWalletUseCase;

  beforeEach(() => {
    walletRepo = new ResilienceWalletRepository();
    txRepo = new ResilienceTxRepository();
    ledgerRepo = new ResilienceLedgerRepository();
    outboxRepo = new SkipLockedOutboxRepository();
    inboxRepo = new ResilienceInboxRepository();
    uow = {
      transactional: async <T>(fn: () => Promise<T>): Promise<T> => {
        try {
          return await fn();
        } finally {
          outboxRepo.releaseLocks();
        }
      },
    };

    logger = {
      log: () => {},
      warn: () => {},
      error: () => {},
    } as unknown as AppLoggerService;
    metrics = new MetricsService();
    sqsClient = new FakeSqsClient();

    createWalletUseCase = new CreateWalletUseCase(
      walletRepo,
      txRepo,
      ledgerRepo,
      outboxRepo,
      uow,
    );
    processWagerUseCase = new ProcessWagerTransactionUseCase(
      walletRepo,
      txRepo,
      ledgerRepo,
      outboxRepo,
      inboxRepo,
      uow,
    );
    reconcileUseCase = new ReconcileWalletUseCase(walletRepo, ledgerRepo);
  });

  describe('1. SQS Consumer & Crash Recovery (Commit antes do ACK)', () => {
    it('deve processar mensagem SQS, gravar no Inbox e fazer ACK somente após o commit', async () => {
      const wallet = await createWalletUseCase.execute({
        playerId: 'p-sqs-1',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      const consumer = new WagerTransactionConsumer(
        processWagerUseCase,
        sqsClient,
        logger,
        metrics,
      );

      const rawMsg: SqsRawMessage = {
        messageId: 'msg-001',
        receiptHandle: 'receipt-001',
        body: JSON.stringify({
          messageId: 'msg-001',
          type: 'WagerTransactionRequested',
          occurredAt: new Date().toISOString(),
          data: {
            providerId: 'prov-sqs',
            externalTransactionId: 'ext-sqs-1',
            idempotencyKey: 'prov-sqs:ext-sqs-1',
            playerId: 'p-sqs-1',
            walletId: wallet.id,
            roundId: 'r-sqs-1',
            gameId: 'slots',
            kind: WagerTransactionKind.Bet,
            money: { amount: '25.00', currency: 'BRL' },
          },
        }),
      };

      const decision = await consumer.handleMessage(rawMsg);
      expect(decision).toBe('ACK');
      expect(sqsClient.deletedReceipts).toContain('receipt-001');

      const inbox = await inboxRepo.find(WagerTransactionConsumer.CONSUMER_NAME, 'msg-001');
      expect(inbox).not.toBeNull();
      expect(inbox!.isProcessed()).toBe(true);

      const updatedWallet = await walletRepo.findById(wallet.id);
      expect(updatedWallet!.balance.amount).toBe('75.00');
    });

    it('Cenário 5 (Seção 13): Worker morre DEPOIS do commit e ANTES do ACK -> Redelivery não duplica débito', async () => {
      const wallet = await createWalletUseCase.execute({
        playerId: 'p-crash',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      const consumer1 = new WagerTransactionConsumer(
        processWagerUseCase,
        sqsClient,
        logger,
        metrics,
      );

      const payloadBody = JSON.stringify({
        messageId: 'msg-crash-99',
        type: 'WagerTransactionRequested',
        occurredAt: new Date().toISOString(),
        data: {
          providerId: 'prov-crash',
          externalTransactionId: 'ext-crash-99',
          idempotencyKey: 'prov-crash:ext-crash-99',
          playerId: 'p-crash',
          walletId: wallet.id,
          roundId: 'r-crash-1',
          gameId: 'crash-game',
          kind: WagerTransactionKind.Bet,
          money: { amount: '40.00', currency: 'BRL' },
        },
      });

      // Simula falha exatamente no momento do deleteMessage (após o commit do banco!)
      sqsClient.failDeleteOnce = true;

      const decisionAfterCrash = await consumer1.handleMessage({
        messageId: 'msg-crash-99',
        receiptHandle: 'receipt-attempt-1',
        body: payloadBody,
        attributes: { ApproximateReceiveCount: '1' },
      });

      // Como o ACK falhou, o worker tratou como erro transitório (RETRY), mas o commit já ocorreu!
      expect(decisionAfterCrash).toBe('RETRY');
      expect(sqsClient.deletedReceipts).not.toContain('receipt-attempt-1');

      // Verifica que o banco já comitou o débito de 40.00 -> saldo 60.00
      const walletAfterCommit = await walletRepo.findById(wallet.id);
      expect(walletAfterCommit!.balance.amount).toBe('60.00');

      // Agora o SQS reentrega a mesma mensagem para outra instância (Worker 2)
      const consumer2 = new WagerTransactionConsumer(
        processWagerUseCase,
        sqsClient,
        logger,
        metrics,
      );

      const decisionOnRedelivery = await consumer2.handleMessage({
        messageId: 'msg-crash-99',
        receiptHandle: 'receipt-attempt-2',
        body: payloadBody,
        attributes: { ApproximateReceiveCount: '2' },
      });

      expect(decisionOnRedelivery).toBe('ACK');
      expect(sqsClient.deletedReceipts).toContain('receipt-attempt-2');

      // O saldo permanece estritamente em 60.00 BRL (nenhum débito duplicado!)
      const finalWallet = await walletRepo.findById(wallet.id);
      expect(finalWallet!.balance.amount).toBe('60.00');
      expect(ledgerRepo.entries.length).toBe(2); // 1 OPENING + 1 único DEBIT

      const rec = await reconcileUseCase.execute(wallet.id);
      expect(rec.consistent).toBe(true);
    });

    it('deve encaminhar para DLQ mensagens malformadas ou que excederam limite de tentativas', async () => {
      const consumer = new WagerTransactionConsumer(
        processWagerUseCase,
        sqsClient,
        logger,
        metrics,
      );

      const decision = await consumer.handleMessage({
        messageId: 'msg-bad-json',
        receiptHandle: 'receipt-bad',
        body: '{ invalid json',
      });

      expect(decision).toBe('DLQ');
      expect(sqsClient.dlqMessages.length).toBe(1);
      expect(sqsClient.deletedReceipts).toContain('receipt-bad');
    });
  });

  describe('2. Transactional Outbox & Publishers Concorrentes (Seção 11 e 13)', () => {
    it('Cenário 6 (Seção 13): Dois publishers concorrentes sobre a mesma Outbox não colidem', async () => {
      const wallet = await createWalletUseCase.execute({
        playerId: 'p-outbox',
        initialBalance: { amount: '200.00', currency: 'BRL' },
      });

      // Gera várias transações para popular a Outbox
      for (let i = 1; i <= 5; i++) {
        await processWagerUseCase.execute({
          providerId: 'prov-ob',
          externalTransactionId: `tx-ob-${i}`,
          idempotencyKey: `prov-ob:tx-ob-${i}`,
          playerId: 'p-outbox',
          walletId: wallet.id,
          roundId: `r-${i}`,
          gameId: 'slots',
          kind: WagerTransactionKind.Bet,
          money: { amount: '10.00', currency: 'BRL' },
        });
      }

      // Total de mensagens na outbox: 1 (OPENING) + 5 * 2 (WalletBalanceChanged + WagerTransactionProcessed) = 11
      expect(outboxRepo.messages.size).toBe(11);

      const publishedEventIds: string[] = [];
      const publisher: EventPublisherPort = {
        publish: async (msg: OutboxMessage) => {
          await new Promise((r) => setTimeout(r, 2));
          publishedEventIds.push(msg.id);
        },
      };

      const worker1 = new OutboxDispatcherWorker(outboxRepo, uow, publisher, logger, metrics);
      const worker2 = new OutboxDispatcherWorker(outboxRepo, uow, publisher, logger, metrics);

      // Dispara os 2 workers concorrentemente
      const [batch1, batch2] = await Promise.all([
        worker1.dispatchBatch(10),
        worker2.dispatchBatch(10),
      ]);

      expect(batch1.published + batch2.published).toBe(11);
      // Nenhum evento foi publicado em duplicidade pelos dois workers!
      const uniquePublished = new Set(publishedEventIds);
      expect(uniquePublished.size).toBe(11);
    });

    it('deve agendar retry com backoff exponencial quando a publicação no SQS falha', async () => {
      await createWalletUseCase.execute({
        playerId: 'p-retry-ob',
        initialBalance: { amount: '50.00', currency: 'BRL' },
      });

      const failingPublisher: EventPublisherPort = {
        publish: async () => {
          throw new Error('SQS temporarily unavailable');
        },
      };

      const worker = new OutboxDispatcherWorker(
        outboxRepo,
        uow,
        failingPublisher,
        logger,
        metrics,
      );

      const now = new Date();
      const res = await worker.dispatchBatch(10, now);

      expect(res.published).toBe(0);
      expect(res.failed).toBe(1);

      const msg = Array.from(outboxRepo.messages.values())[0];
      expect(msg.isPending()).toBe(true);
      expect(msg.attempts).toBe(1);
      expect(msg.nextAttemptAt!.getTime()).toBeGreaterThan(now.getTime());
    });
  });

  describe('3. PendingReferenceWorker (Referências Fora de Ordem & Timeout — Seção 7.1)', () => {
    it('Cenário 7 (Seção 13): Deve resolver automaticamente REFUND/ROLLBACK após chegada da BET referenciada', async () => {
      const wallet = await createWalletUseCase.execute({
        playerId: 'p-ooo',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      // 1. REFUND chega ANTES da BET -> fica em PENDING_REFERENCE
      const earlyRefund = await processWagerUseCase.execute({
        providerId: 'prov-ooo',
        externalTransactionId: 'refund-ooo-1',
        idempotencyKey: 'prov-ooo:refund-ooo-1',
        playerId: 'p-ooo',
        walletId: wallet.id,
        roundId: 'round-ooo',
        gameId: 'blackjack',
        kind: WagerTransactionKind.Refund,
        money: { amount: '40.00', currency: 'BRL' },
        referenceExternalTransactionId: 'bet-ooo-1',
      });

      expect(earlyRefund.status).toBe(WagerTransactionStatus.PendingReference);

      // 2. BET chega depois -> debita 40.00 (Saldo vai para 60.00 BRL)
      const delayedBet = await processWagerUseCase.execute({
        providerId: 'prov-ooo',
        externalTransactionId: 'bet-ooo-1',
        idempotencyKey: 'prov-ooo:bet-ooo-1',
        playerId: 'p-ooo',
        walletId: wallet.id,
        roundId: 'round-ooo',
        gameId: 'blackjack',
        kind: WagerTransactionKind.Bet,
        money: { amount: '40.00', currency: 'BRL' },
      });
      expect(delayedBet.status).toBe(WagerTransactionStatus.Processed);
      expect(delayedBet.balance.amount).toBe('60.00');

      // 3. PendingReferenceWorker executa e resolve o REFUND pendente!
      const pendingWorker = new PendingReferenceWorker(
        txRepo,
        outboxRepo,
        uow,
        processWagerUseCase,
        logger,
        metrics,
      );

      const batchResult = await pendingWorker.processPendingBatch();
      expect(batchResult.resolved).toBe(1);

      // Verifica que a transação de REFUND agora está PROCESSED e o saldo voltou para 100.00 BRL
      const resolvedTx = await txRepo.findById(earlyRefund.transactionId);
      expect(resolvedTx!.status).toBe(WagerTransactionStatus.Processed);

      const finalWallet = await walletRepo.findById(wallet.id);
      expect(finalWallet!.balance.amount).toBe('100.00');

      const rec = await reconcileUseCase.execute(wallet.id);
      expect(rec.consistent).toBe(true);
      expect(rec.checkedEntries).toBe(3); // OPENING (+100) - BET (-40) + REFUND (+40)
    });

    it('deve rejeitar com REFERENCE_TIMEOUT quando a referência nunca chega e o limite de tentativas/TTL expira', async () => {
      const wallet = await createWalletUseCase.execute({
        playerId: 'p-timeout',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      const orphanRollback = await processWagerUseCase.execute({
        providerId: 'prov-timeout',
        externalTransactionId: 'rollback-orphan',
        idempotencyKey: 'prov-timeout:rollback-orphan',
        playerId: 'p-timeout',
        walletId: wallet.id,
        roundId: 'round-ghost',
        gameId: 'roulette',
        kind: WagerTransactionKind.Rollback,
        money: { amount: '25.00', currency: 'BRL' },
        referenceExternalTransactionId: 'ghost-bet-never-arrives',
      });

      expect(orphanRollback.status).toBe(WagerTransactionStatus.PendingReference);

      const pendingWorker = new PendingReferenceWorker(
        txRepo,
        outboxRepo,
        uow,
        processWagerUseCase,
        logger,
        metrics,
        { maxAttempts: 2, ttlMs: 1000, baseBackoffMs: 10 },
      );

      // Simula passagem do tempo além do TTL (2 segundos no futuro)
      const futureNow = new Date(Date.now() + 2000);
      const batchResult = await pendingWorker.processPendingBatch(50, futureNow);

      expect(batchResult.timedOut).toBe(1);

      const rejectedTx = await txRepo.findById(orphanRollback.transactionId);
      expect(rejectedTx!.status).toBe(WagerTransactionStatus.Rejected);
      expect(rejectedTx!.failureCode).toBe(FailureCode.REFERENCE_TIMEOUT);
    });

    it('Regra 4 (Seção 7): Deve rejeitar segunda tentativa de REFUND sobre a mesma BET com REFERENCE_ALREADY_REVERSED', async () => {
      const wallet = await createWalletUseCase.execute({
        playerId: 'p-double-rev',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      await processWagerUseCase.execute({
        providerId: 'prov-dr',
        externalTransactionId: 'bet-dr-1',
        idempotencyKey: 'prov-dr:bet-dr-1',
        playerId: 'p-double-rev',
        walletId: wallet.id,
        roundId: 'round-dr',
        gameId: 'slots',
        kind: WagerTransactionKind.Bet,
        money: { amount: '30.00', currency: 'BRL' },
      });

      // Primeiro REFUND -> PROCESSED
      const firstRefund = await processWagerUseCase.execute({
        providerId: 'prov-dr',
        externalTransactionId: 'refund-dr-1',
        idempotencyKey: 'prov-dr:refund-dr-1',
        playerId: 'p-double-rev',
        walletId: wallet.id,
        roundId: 'round-dr',
        gameId: 'slots',
        kind: WagerTransactionKind.Refund,
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: 'bet-dr-1',
      });
      expect(firstRefund.status).toBe(WagerTransactionStatus.Processed);

      // Segundo REFUND (com outro ID externo) tentando reverter a MESMA BET -> REJECTED!
      const secondRefund = await processWagerUseCase.execute({
        providerId: 'prov-dr',
        externalTransactionId: 'refund-dr-2',
        idempotencyKey: 'prov-dr:refund-dr-2',
        playerId: 'p-double-rev',
        walletId: wallet.id,
        roundId: 'round-dr',
        gameId: 'slots',
        kind: WagerTransactionKind.Refund,
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: 'bet-dr-1',
      });

      expect(secondRefund.status).toBe(WagerTransactionStatus.Rejected);
      expect(secondRefund.failureCode).toBe(FailureCode.REFERENCE_ALREADY_REVERSED);
      expect(secondRefund.balance.amount).toBe('100.00');
    });
  });
});
