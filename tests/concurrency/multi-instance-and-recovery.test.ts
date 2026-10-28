import { beforeEach, describe, expect, it } from 'bun:test';
import { CreateWalletUseCase } from '../../src/application/use-cases/create-wallet.use-case';
import { ProcessWagerTransactionUseCase } from '../../src/application/use-cases/process-wager-transaction.use-case';
import { ReconcileWalletUseCase } from '../../src/application/use-cases/reconcile-wallet.use-case';
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
  WagerTransactionConsumer,
} from '../../src/infrastructure/messaging/sqs/wager-transaction.consumer';
import { MetricsService } from '../../src/infrastructure/metrics/metrics.service';

class SilentLogger extends AppLoggerService {
  override log() {}
  override error() {}
  override warn() {}
  override debug() {}
}

/**
 * Gerenciador de Locks compartilhado entre múltiplas instâncias da aplicação,
 * reproduzindo fielmente o comportamento de SELECT ... FOR UPDATE (por walletId)
 * e FOR UPDATE SKIP LOCKED (no polling da outbox) do PostgreSQL.
 */
class SharedDatabaseState {
  public wallets = new Map<string, Wallet>();
  public transactions = new Map<string, WagerTransaction>();
  public ledgerEntries: WalletLedgerEntry[] = [];
  public outboxMessages = new Map<string, OutboxMessage>();
  public inboxMessages = new Map<string, InboxMessage>();

  private walletLocks = new Map<string, Promise<void>>();
  public outboxLockedIds = new Set<string>();
  public lockWaitTimestamps: Array<{ walletId: string; acquiredAt: number }> = [];

  async acquireWalletLock(walletId: string): Promise<() => void> {
    while (this.walletLocks.has(walletId)) {
      await this.walletLocks.get(walletId);
    }

    let release!: () => void;
    const promise = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.walletLocks.set(walletId, promise);
    this.lockWaitTimestamps.push({ walletId, acquiredAt: performance.now() });

    return () => {
      this.walletLocks.delete(walletId);
      release();
    };
  }
}

/**
 * Instância isolada da aplicação (simulando um pod/processo independente)
 * conectada ao mesmo banco compartilhado (SharedDatabaseState).
 */
class ApplicationNode {
  public readonly walletRepo: WalletRepository;
  public readonly txRepo: WagerTransactionRepository;
  public readonly ledgerRepo: WalletLedgerRepository;
  public readonly outboxRepo: OutboxRepository;
  public readonly inboxRepo: InboxRepository;
  public readonly uow: UnitOfWork;

  public readonly createWalletUseCase: CreateWalletUseCase;
  public readonly processWagerUseCase: ProcessWagerTransactionUseCase;
  public readonly reconcileWalletUseCase: ReconcileWalletUseCase;
  public readonly outboxWorker: OutboxDispatcherWorker;
  public readonly pendingRefWorker: PendingReferenceWorker;
  public readonly logger = new SilentLogger();
  public readonly metrics = new MetricsService();

  private activeReleases: Array<() => void> = [];

  constructor(
    public readonly instanceName: string,
    private readonly db: SharedDatabaseState,
    private readonly publisher: EventPublisherPort,
    private readonly simulatedIoDelayMs = 5,
  ) {
    this.walletRepo = {
      findById: async (id: string, lock?: 'pessimistic_write') => {
        if (lock === 'pessimistic_write') {
          const release = await this.db.acquireWalletLock(id);
          this.activeReleases.push(release);
        }
        if (this.simulatedIoDelayMs > 0) {
          await new Promise((r) => setTimeout(r, this.simulatedIoDelayMs));
        }
        return this.db.wallets.get(id) || null;
      },
      findByPlayerIdAndCurrency: async (playerId: string, currency: string) => {
        for (const w of this.db.wallets.values()) {
          if (w.playerId === playerId && w.currency === currency) {
            return w;
          }
        }
        return null;
      },
      save: async (wallet: Wallet) => {
        this.db.wallets.set(wallet.id, wallet);
      },
    };

    this.txRepo = {
      findById: async (id: string) => {
        return this.db.transactions.get(id) || null;
      },
      findByIdempotencyKey: async (key: string) => {
        for (const tx of this.db.transactions.values()) {
          if (tx.idempotencyKey === key) {
            return tx;
          }
        }
        return null;
      },
      findByProviderAndExternalId: async (providerId: string, externalId: string) => {
        for (const tx of this.db.transactions.values()) {
          if (tx.providerId === providerId && tx.externalTransactionId === externalId) {
            return tx;
          }
        }
        return null;
      },
      findProcessedReversal: async (refId: string) => {
        for (const tx of this.db.transactions.values()) {
          if (
            tx.referenceTransactionId === refId &&
            tx.status === WagerTransactionStatus.Processed &&
            (tx.kind === WagerTransactionKind.Refund ||
              tx.kind === WagerTransactionKind.Rollback)
          ) {
            return tx;
          }
        }
        return null;
      },
      findPendingReferences: async (limit: number) => {
        return Array.from(this.db.transactions.values())
          .filter((t) => t.status === WagerTransactionStatus.PendingReference)
          .slice(0, limit);
      },
      save: async (tx: WagerTransaction) => {
        this.db.transactions.set(tx.id, tx);
      },
    };

    this.ledgerRepo = {
      save: async (entry: WalletLedgerEntry) => {
        this.db.ledgerEntries.push(entry);
      },
      findByWalletId: async (
        walletId: string,
        options?: { limit: number; cursor?: string },
      ): Promise<LedgerPaginationResult> => {
        const entries = this.db.ledgerEntries
          .filter((e) => e.walletId === walletId)
          .slice(0, options?.limit ?? 50);
        return { entries };
      },
      calculateBalance: async (walletId: string) => {
        const entries = this.db.ledgerEntries.filter((e) => e.walletId === walletId);
        let total = Money.zero('BRL');
        for (const e of entries) {
          total =
            e.direction === LedgerDirection.Credit
              ? total.add(e.money)
              : total.subtract(e.money);
        }
        return { calculatedAmount: total.amount, count: entries.length };
      },
    };

    this.outboxRepo = {
      save: async (outbox: OutboxMessage) => {
        this.db.outboxMessages.set(outbox.id, outbox);
      },
      findPendingToDispatch: async (limit: number, now: Date) => {
        const claimed: OutboxMessage[] = [];
        for (const msg of this.db.outboxMessages.values()) {
          if (claimed.length >= limit) break;
          if (msg.isDue(now) && !this.db.outboxLockedIds.has(msg.id)) {
            this.db.outboxLockedIds.add(msg.id);
            claimed.push(msg);
          }
        }
        return claimed;
      },
    };

    this.inboxRepo = {
      find: async (consumerName: string, messageId: string) => {
        return this.db.inboxMessages.get(`${consumerName}:${messageId}`) || null;
      },
      save: async (inbox: InboxMessage) => {
        this.db.inboxMessages.set(`${inbox.consumerName}:${inbox.messageId}`, inbox);
      },
    };

    this.uow = {
      transactional: async <T>(fn: () => Promise<T>): Promise<T> => {
        try {
          return await fn();
        } finally {
          while (this.activeReleases.length > 0) {
            const release = this.activeReleases.pop();
            if (release) release();
          }
        }
      },
    };

    this.createWalletUseCase = new CreateWalletUseCase(
      this.walletRepo,
      this.txRepo,
      this.ledgerRepo,
      this.outboxRepo,
      this.uow,
    );

    this.processWagerUseCase = new ProcessWagerTransactionUseCase(
      this.walletRepo,
      this.txRepo,
      this.ledgerRepo,
      this.outboxRepo,
      this.inboxRepo,
      this.uow,
    );

    this.reconcileWalletUseCase = new ReconcileWalletUseCase(
      this.walletRepo,
      this.ledgerRepo,
    );

    this.outboxWorker = new OutboxDispatcherWorker(
      this.outboxRepo,
      this.uow,
      this.publisher,
      this.logger,
      this.metrics,
    );

    this.pendingRefWorker = new PendingReferenceWorker(
      this.txRepo,
      this.outboxRepo,
      this.uow,
      this.processWagerUseCase,
      this.logger,
      this.metrics,
    );
  }
}

describe('Section 13 Mandatory Scenarios: Multi-Instance, Distinct Wallets, SIGTERM & Crash Recovery', () => {
  let sharedDb: SharedDatabaseState;
  let publishedEventIds: string[];
  let mockPublisher: EventPublisherPort;

  beforeEach(() => {
    sharedDb = new SharedDatabaseState();
    publishedEventIds = [];
    mockPublisher = {
      publish: async (msg: OutboxMessage) => {
        await new Promise((r) => setTimeout(r, 2));
        publishedEventIds.push(msg.id);
      },
    };
  });

  describe('Cenário 3 (Seção 13): Wallets Distintas Processadas em Paralelo Sem Contenção Cruzada', () => {
    it('deve processar 20 wallets distintas simultaneamente em paralelo real sem bloquear umas às outras', async () => {
      const node = new ApplicationNode('node-1', sharedDb, mockPublisher, 15);

      // Cria 20 carteiras distintas com 100.00 BRL cada
      const wallets: { id: string; playerId: string }[] = [];
      for (let i = 1; i <= 20; i++) {
        const w = await node.createWalletUseCase.execute({
          playerId: `player-parallel-${i}`,
          initialBalance: { amount: '100.00', currency: 'BRL' },
        });
        wallets.push({ id: w.id, playerId: w.playerId });
      }

      // Se as 20 carteiras fossem serializadas globalmente (com 15ms de I/O cada), levariam >= 300ms.
      // Como o lock é por walletId, todas as 20 adquirem lock simultaneamente (~15-40ms total).
      const startTime = performance.now();
      const results = await Promise.all(
        wallets.map((w, idx) =>
          node.processWagerUseCase.execute({
            providerId: 'provider-parallel',
            externalTransactionId: `bet-wallet-${idx}`,
            idempotencyKey: `provider-parallel:bet-wallet-${idx}`,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `round-${idx}`,
            gameId: 'crash-x',
            kind: WagerTransactionKind.Bet,
            money: { amount: '35.50', currency: 'BRL' },
          }),
        ),
      );
      const elapsedMs = performance.now() - startTime;

      // Comprova paralelismo real (muito abaixo do tempo serial de 20 * 15ms = 300ms)
      expect(elapsedMs).toBeLessThan(200);

      // Verifica que todas as 20 carteiras terminaram com 64.50 BRL e reconciliação exata
      for (let i = 0; i < wallets.length; i++) {
        expect(results[i].status).toBe(WagerTransactionStatus.Processed);
        expect(results[i].balance.amount).toBe('64.50');

        const reconciliation = await node.reconcileWalletUseCase.execute(wallets[i].id);
        expect(reconciliation.consistent).toBe(true);
        expect(reconciliation.storedBalance.amount).toBe('64.50');
        expect(reconciliation.calculatedBalance.amount).toBe('64.50');
        expect(reconciliation.difference.amount).toBe('0.00');
      }
    });
  });

  describe('Cenário 4 (Seção 13): >= 3 Instâncias Concorrentes da Aplicação', () => {
    it('deve manter consistência absoluta com 4 instâncias simultâneas disputando a mesma carteira e a mesma outbox', async () => {
      const node1 = new ApplicationNode('instance-1', sharedDb, mockPublisher, 4);
      const node2 = new ApplicationNode('instance-2', sharedDb, mockPublisher, 4);
      const node3 = new ApplicationNode('instance-3', sharedDb, mockPublisher, 4);
      const node4 = new ApplicationNode('instance-4', sharedDb, mockPublisher, 4);
      const nodes = [node1, node2, node3, node4];

      // Cria carteira com 500.00 BRL através da instância 1
      const wallet = await node1.createWalletUseCase.execute({
        playerId: 'player-multi-node',
        initialBalance: { amount: '500.00', currency: 'BRL' },
      });

      // Dispara 20 apostas de 30.00 BRL (total 600.00 BRL para saldo de 500.00 BRL)
      // distribuídas pelas 4 instâncias em paralelo, além de 8 replays idênticos em instâncias diferentes!
      const betOperations = Array.from({ length: 20 }, (_, i) => {
        const targetNode = nodes[i % nodes.length];
        return targetNode.processWagerUseCase.execute({
          providerId: 'prov-cluster',
          externalTransactionId: `cluster-bet-${i}`,
          idempotencyKey: `prov-cluster:cluster-bet-${i}`,
          playerId: 'player-multi-node',
          walletId: wallet.id,
          roundId: `round-${i}`,
          gameId: 'mines',
          kind: WagerTransactionKind.Bet,
          money: { amount: '30.00', currency: 'BRL' },
        });
      });

      // Replays concorrentes das 4 primeiras apostas enviados para instâncias diferentes
      const replayOperations = Array.from({ length: 8 }, (_, i) => {
        const betIdx = i % 4;
        const targetNode = nodes[(i + 2) % nodes.length];
        return targetNode.processWagerUseCase.execute({
          providerId: 'prov-cluster',
          externalTransactionId: `cluster-bet-${betIdx}`,
          idempotencyKey: `prov-cluster:cluster-bet-${betIdx}`,
          playerId: 'player-multi-node',
          walletId: wallet.id,
          roundId: `round-${betIdx}`,
          gameId: 'mines',
          kind: WagerTransactionKind.Bet,
          money: { amount: '30.00', currency: 'BRL' },
        });
      });

      const allResults = await Promise.all([...betOperations, ...replayOperations]);

      // Com 500.00 BRL de saldo e apostas de 30.00 BRL:
      // No máximo 16 apostas únicas podem ser aprovadas (16 * 30.00 = 480.00 BRL), sobrando 20.00 BRL.
      // As outras 4 apostas únicas devem ser REJECTED por INSUFFICIENT_FUNDS.
      const uniqueProcessed = allResults.filter(
        (r) => r.status === WagerTransactionStatus.Processed && !r.idempotentReplay,
      );
      const uniqueRejected = allResults.filter(
        (r) => r.status === WagerTransactionStatus.Rejected && !r.idempotentReplay,
      );
      const replays = allResults.filter((r) => r.idempotentReplay);

      expect(uniqueProcessed.length).toBe(16);
      expect(uniqueRejected.length).toBe(4);
      expect(replays.length).toBe(8);

      // Agora os 4 OutboxDispatcherWorkers das 4 instâncias disputam a mesma tabela outbox simultaneamente
      const dispatchResults = await Promise.all(
        nodes.map(async (n) => {
          const res = await n.outboxWorker.dispatchBatch(50);
          return res.published;
        }),
      );
      sharedDb.outboxLockedIds.clear();

      const totalPublished = dispatchResults.reduce((acc, v) => acc + v, 0);
      expect(totalPublished).toBe(sharedDb.outboxMessages.size);
      expect(new Set(publishedEventIds).size).toBe(totalPublished);

      // Invariante contábil universal verificada a partir de qualquer instância
      const rec = await node3.reconcileWalletUseCase.execute(wallet.id);
      expect(rec.consistent).toBe(true);
      expect(rec.storedBalance.amount).toBe('20.00');
      expect(rec.calculatedBalance.amount).toBe('20.00');
      expect(rec.difference.amount).toBe('0.00');
      expect(rec.checkedEntries).toBe(17); // 1 OPENING + 16 BETs processadas
    });
  });

  describe('Cenário 5 (Seção 10 & 13): Graceful Shutdown (SIGTERM) Devolvendo Visibilidade ao SQS', () => {
    it('deve interromper polling no SIGTERM e devolver visibilidade (VisibilityTimeout=0) de mensagens não concluídas no drain timeout', async () => {
      const node = new ApplicationNode('node-sigterm', sharedDb, mockPublisher, 200);
      const wallet = await node.createWalletUseCase.execute({
        playerId: 'player-sigterm',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      const visibilityReturnedHandles: Array<{ handle: string; timeout: number }> = [];
      let slowUseCaseResolver!: () => void;

      // Use case bloqueado simulando operação longa quando o SIGTERM chega
      const slowUseCase = {
        execute: async () => {
          await new Promise<void>((resolve) => {
            slowUseCaseResolver = resolve;
          });
          return {
            transactionId: 'tx-1',
            status: WagerTransactionStatus.Processed,
            balance: { amount: '80.00', currency: 'BRL' },
            idempotentReplay: false,
          };
        },
      } as unknown as ProcessWagerTransactionUseCase;

      const mockQueueClient: SqsConsumerClientPort = {
        receiveMessages: async () => [],
        deleteMessage: async () => {},
        changeMessageVisibility: async (_q, receiptHandle, visibilityTimeout) => {
          visibilityReturnedHandles.push({
            handle: receiptHandle,
            timeout: visibilityTimeout,
          });
        },
        sendToDlq: async () => {},
      };

      const consumer = new WagerTransactionConsumer(
        slowUseCase,
        mockQueueClient,
        node.logger,
        node.metrics,
        { shutdownTimeoutMs: 30 },
      );

      // Inicia o processamento de uma mensagem SQS sem aguardar conclusão imediata
      const processingPromise = consumer.handleMessage({
        messageId: 'sqs-msg-inflight-1',
        receiptHandle: 'receipt-inflight-1',
        attributes: { ApproximateReceiveCount: '1' },
        body: JSON.stringify({
          messageId: 'msg-inflight-1',
          type: 'WagerTransactionRequested',
          occurredAt: new Date().toISOString(),
          data: {
            providerId: 'prov-sig',
            externalTransactionId: 'ext-sig-1',
            idempotencyKey: 'prov-sig:ext-sig-1',
            playerId: 'player-sigterm',
            walletId: wallet.id,
            roundId: 'r-sig',
            gameId: 'slots',
            kind: WagerTransactionKind.Bet,
            money: { amount: '20.00', currency: 'BRL' },
          },
        }),
      });

      // Aguarda a mensagem entrar em voo (in-flight)
      await new Promise((r) => setTimeout(r, 15));

      // Envia sinal SIGTERM (onApplicationShutdown) com shutdownTimeoutMs de 30ms
      await consumer.onApplicationShutdown('SIGTERM');

      // Como a mensagem ainda estava em andamento após o drainTimeout, o consumer DEVE ter devolvido
      // a visibilidade imediatamente ao SQS (VisibilityTimeout = 0) para outra instância assumir
      expect(visibilityReturnedHandles.length).toBe(1);
      expect(visibilityReturnedHandles[0]).toEqual({
        handle: 'receipt-inflight-1',
        timeout: 0,
      });

      // Libera a promise pendente para encerrar o teste de forma limpa
      slowUseCaseResolver();
      await processingPromise;
    });
  });

  describe('Cenário 8 (Seção 13): Reinício do Serviço com Comprovação da Consistência Final', () => {
    it('deve recuperar estado pendente (Outbox, PendingReference e SQS Redelivery) após queda da Instância 1 e garantir saldo == ledger na Instância 2', async () => {
      // --- FASE 1: Instância 1 está ativa e processa operações ---
      const instance1 = new ApplicationNode('instance-1-dying', sharedDb, mockPublisher, 2);

      const wallet = await instance1.createWalletUseCase.execute({
        playerId: 'player-restart',
        initialBalance: { amount: '300.00', currency: 'BRL' },
      });

      // 1. Aposta 1 de 100.00 BRL (Saldo -> 200.00 BRL)
      await instance1.processWagerUseCase.execute({
        providerId: 'prov-r',
        externalTransactionId: 'bet-r-1',
        idempotencyKey: 'prov-r:bet-r-1',
        playerId: 'player-restart',
        walletId: wallet.id,
        roundId: 'round-r-1',
        gameId: 'baccarat',
        kind: WagerTransactionKind.Bet,
        money: { amount: '100.00', currency: 'BRL' },
      });

      // 2. REFUND de uma segunda aposta ('bet-r-2') chega fora de ordem na Instância 1 -> fica em PENDING_REFERENCE
      const earlyRefund = await instance1.processWagerUseCase.execute({
        providerId: 'prov-r',
        externalTransactionId: 'refund-r-2',
        idempotencyKey: 'prov-r:refund-r-2',
        playerId: 'player-restart',
        walletId: wallet.id,
        roundId: 'round-r-2',
        gameId: 'baccarat',
        kind: WagerTransactionKind.Refund,
        money: { amount: '50.00', currency: 'BRL' },
        referenceExternalTransactionId: 'bet-r-2',
      });
      expect(earlyRefund.status).toBe(WagerTransactionStatus.PendingReference);

      // 3. A aposta 'bet-r-2' chega via SQS na Instância 1, que faz o commit no banco,
      // MAS SOFRE CRASH ANTES de publicar a Outbox e ANTES de rodar o PendingReferenceWorker!
      await instance1.processWagerUseCase.execute({
        providerId: 'prov-r',
        externalTransactionId: 'bet-r-2',
        idempotencyKey: 'prov-r:bet-r-2',
        playerId: 'player-restart',
        walletId: wallet.id,
        roundId: 'round-r-2',
        gameId: 'baccarat',
        kind: WagerTransactionKind.Bet,
        money: { amount: '50.00', currency: 'BRL' },
        inboxContext: {
          consumerName: WagerTransactionConsumer.CONSUMER_NAME,
          messageId: 'msg-bet-r-2',
        },
      });

      // Antes da recuperação, o saldo é 150.00 (300 - 100 - 50) e nenhuma outbox foi publicada
      expect(publishedEventIds.length).toBe(0);

      // --- FASE 2: Instância 1 morre. Instância 2 sobe a partir do estado persistido no banco ---
      const instance2 = new ApplicationNode('instance-2-recovered', sharedDb, mockPublisher, 2);

      // O SQS reentrega 'msg-bet-r-2' para a Instância 2 (pois a Instância 1 morreu antes do ACK)
      let ackedOnInstance2 = false;
      const consumer2 = new WagerTransactionConsumer(
        instance2.processWagerUseCase,
        {
          receiveMessages: async () => [],
          deleteMessage: async () => {
            ackedOnInstance2 = true;
          },
          changeMessageVisibility: async () => {},
          sendToDlq: async () => {},
        },
        instance2.logger,
        instance2.metrics,
      );

      const decision = await consumer2.handleMessage({
        messageId: 'msg-bet-r-2',
        receiptHandle: 'handle-redelivery-2',
        attributes: { ApproximateReceiveCount: '2' },
        body: JSON.stringify({
          messageId: 'msg-bet-r-2',
          type: 'WagerTransactionRequested',
          occurredAt: new Date().toISOString(),
          data: {
            providerId: 'prov-r',
            externalTransactionId: 'bet-r-2',
            idempotencyKey: 'prov-r:bet-r-2',
            playerId: 'player-restart',
            walletId: wallet.id,
            roundId: 'round-r-2',
            gameId: 'baccarat',
            kind: WagerTransactionKind.Bet,
            money: { amount: '50.00', currency: 'BRL' },
          },
        }),
      });
      // Redelivery deduplicado pelo Inbox e confirmado (ACK) sem cobrar duas vezes
      expect(decision).toBe('ACK');
      expect(ackedOnInstance2).toBe(true);

      // Instância 2 executa o PendingReferenceWorker e resolve o 'refund-r-2' que estava pendente!
      const pendingRes = await instance2.pendingRefWorker.processPendingBatch(
        50,
        new Date(Date.now() + 5000),
      );
      expect(pendingRes.resolved).toBe(1);

      // Instância 2 despacha todos os eventos acumulados na Outbox
      const outboxRes = await instance2.outboxWorker.dispatchBatch(50);
      expect(outboxRes.published).toBeGreaterThanOrEqual(7);
      expect(publishedEventIds.length).toBe(outboxRes.published);

      // Todas as mensagens na outbox agora estão publicadas
      for (const msg of sharedDb.outboxMessages.values()) {
        expect(msg.isPending()).toBe(false);
        expect(msg.publishedAt).toBeDefined();
      }

      // Saldo final esperado:
      // +300.00 (OPENING) - 100.00 (bet-r-1) - 50.00 (bet-r-2) + 50.00 (refund-r-2) = 200.00 BRL
      const reconciliation = await instance2.reconcileWalletUseCase.execute(wallet.id);
      expect(reconciliation.consistent).toBe(true);
      expect(reconciliation.storedBalance.amount).toBe('200.00');
      expect(reconciliation.calculatedBalance.amount).toBe('200.00');
      expect(reconciliation.difference.amount).toBe('0.00');
      expect(reconciliation.checkedEntries).toBe(4);
    });
  });
});
