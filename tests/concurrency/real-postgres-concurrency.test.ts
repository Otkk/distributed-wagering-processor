import 'reflect-metadata';
import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { MikroORM } from '@mikro-orm/postgresql';
import { CreateWalletUseCase } from '../../src/application/use-cases/create-wallet.use-case';
import { ProcessWagerTransactionUseCase } from '../../src/application/use-cases/process-wager-transaction.use-case';
import { ReconcileWalletUseCase } from '../../src/application/use-cases/reconcile-wallet.use-case';
import { OutboxMessage } from '../../src/domain/models/outbox-message';
import {
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/models/wager-transaction';
import { buildMikroOrmConfig } from '../../src/infrastructure/database/mikro-orm.config';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/database/mikro-orm-unit-of-work';
import { MikroOrmInboxRepository } from '../../src/infrastructure/database/repositories/mikro-orm-inbox.repository';
import { MikroOrmOutboxRepository } from '../../src/infrastructure/database/repositories/mikro-orm-outbox.repository';
import { MikroOrmWagerTransactionRepository } from '../../src/infrastructure/database/repositories/mikro-orm-wager-transaction.repository';
import { MikroOrmWalletLedgerRepository } from '../../src/infrastructure/database/repositories/mikro-orm-wallet-ledger.repository';
import { MikroOrmWalletRepository } from '../../src/infrastructure/database/repositories/mikro-orm-wallet.repository';
import { AppLoggerService } from '../../src/infrastructure/logger/app-logger.service';
import { OutboxDispatcherWorker } from '../../src/infrastructure/messaging/outbox/outbox-dispatcher.worker';
import { PendingReferenceWorker } from '../../src/infrastructure/messaging/outbox/pending-reference.worker';
import {
  SqsConsumerClientPort,
  SqsRawMessage,
  WagerTransactionConsumer,
} from '../../src/infrastructure/messaging/sqs/wager-transaction.consumer';
import { MetricsService } from '../../src/infrastructure/metrics/metrics.service';
import { ensureRealPostgres } from '../helpers/real-infra.helper';

interface AppNodeBundle {
  orm: MikroORM;
  createWallet: CreateWalletUseCase;
  processWager: ProcessWagerTransactionUseCase;
  reconcileWallet: ReconcileWalletUseCase;
  walletRepo: MikroOrmWalletRepository;
  txRepo: MikroOrmWagerTransactionRepository;
  ledgerRepo: MikroOrmWalletLedgerRepository;
  outboxRepo: MikroOrmOutboxRepository;
  inboxRepo: MikroOrmInboxRepository;
  uow: MikroOrmUnitOfWork;
  logger: AppLoggerService;
  metrics: MetricsService;
}

async function createPostgresNode(port: number): Promise<AppNodeBundle> {
  process.env.DB_PORT = String(port);
  process.env.LOG_LEVEL = 'silent';

  const orm = await MikroORM.init({
    ...buildMikroOrmConfig(),
    port,
  });

  const em = orm.em;
  const walletRepo = new MikroOrmWalletRepository(em);
  const txRepo = new MikroOrmWagerTransactionRepository(em);
  const ledgerRepo = new MikroOrmWalletLedgerRepository(em);
  const outboxRepo = new MikroOrmOutboxRepository(em);
  const inboxRepo = new MikroOrmInboxRepository(em);
  const uow = new MikroOrmUnitOfWork(em);
  const logger = new AppLoggerService();
  const metrics = new MetricsService();

  const createWallet = new CreateWalletUseCase(
    walletRepo,
    txRepo,
    ledgerRepo,
    outboxRepo,
    uow,
  );
  const processWager = new ProcessWagerTransactionUseCase(
    walletRepo,
    txRepo,
    ledgerRepo,
    outboxRepo,
    inboxRepo,
    uow,
    {
      onLockConflict: () => metrics.incrementLockConflict(),
    },
  );
  const reconcileWallet = new ReconcileWalletUseCase(walletRepo, ledgerRepo, {
    onDivergence: () => metrics.incrementReconciliationDivergence(),
  });

  return {
    orm,
    createWallet,
    processWager,
    reconcileWallet,
    walletRepo,
    txRepo,
    ledgerRepo,
    outboxRepo,
    inboxRepo,
    uow,
    logger,
    metrics,
  };
}

describe('Real PostgreSQL Concurrency & Chaos Suite (README §13 Mandatory Scenarios on Live Relational DB)', () => {
  let dbPort: number;
  let primaryNode: AppNodeBundle;
  const createdWalletIds: string[] = [];

  beforeAll(async () => {
    const { orm, port } = await ensureRealPostgres();
    dbPort = port;
    await orm.close(true);
    primaryNode = await createPostgresNode(dbPort);
  }, 30000);

  afterAll(async () => {
    if (primaryNode?.orm && (await primaryNode.orm.isConnected())) {
      await primaryNode.orm.close(true);
    }
  }, 30000);

  it('Cenário 1 (§13.1): Mesma aposta enviada 50 vezes em paralelo no PostgreSQL real -> único débito e 49 replays idempotentes', async () => {
    const tag = `pg-c1-${Date.now()}`;
    const playerId = `player-${tag}`;

    const wallet = await primaryNode.createWallet.execute({
      playerId,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    createdWalletIds.push(wallet.id);

    const results = await Promise.all(
      Array.from({ length: 50 }, () =>
        primaryNode.processWager.execute({
          providerId: 'prov-pg-c1',
          externalTransactionId: `bet-50x-${tag}`,
          idempotencyKey: `idem-50x-${tag}`,
          playerId,
          walletId: wallet.id,
          roundId: `round-${tag}`,
          gameId: 'fortune-tiger',
          kind: WagerTransactionKind.Bet,
          money: { amount: '30.00', currency: 'BRL' },
        }),
      ),
    );

    const firstExecutions = results.filter((r) => !r.idempotentReplay);
    const replays = results.filter((r) => r.idempotentReplay);

    expect(firstExecutions.length).toBe(1);
    expect(replays.length).toBe(49);

    const uniqueTxIds = new Set(results.map((r) => r.transactionId));
    expect(uniqueTxIds.size).toBe(1);

    const rec = await primaryNode.reconcileWallet.execute(wallet.id);
    expect(rec.consistent).toBe(true);
    expect(rec.storedBalance.amount).toBe('70.00');
    expect(rec.calculatedBalance.amount).toBe('70.00');
    expect(rec.difference.amount).toBe('0.00');
    expect(rec.checkedEntries).toBe(2); // OPENING (+100) + 1 BET (-30)
  });

  it('Cenário 2 (§13.2 & §8): Operações concorrentes (BET A 80, BET B 80, WIN C 30, ROLLBACK D de A 80) disputando saldo da mesma carteira no PostgreSQL real', async () => {
    const tag = `pg-c2-${Date.now()}`;
    const playerId = `player-${tag}`;

    const wallet = await primaryNode.createWallet.execute({
      playerId,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    createdWalletIds.push(wallet.id);

    const betAExternalId = `bet-a-${tag}`;
    const betBExternalId = `bet-b-${tag}`;
    const winCExternalId = `win-c-${tag}`;
    const rollbackDExternalId = `rollback-d-${tag}`;

    // Dispara as 4 operações concorrentes exatamente como descrito na Seção 8 do README
    const [resA, resB, resC, resD] = await Promise.all([
      primaryNode.processWager.execute({
        providerId: 'prov-pg-c2',
        externalTransactionId: betAExternalId,
        idempotencyKey: `idem-a-${tag}`,
        playerId,
        walletId: wallet.id,
        roundId: `round-${tag}`,
        gameId: 'aviator',
        kind: WagerTransactionKind.Bet,
        money: { amount: '80.00', currency: 'BRL' },
      }),
      primaryNode.processWager.execute({
        providerId: 'prov-pg-c2',
        externalTransactionId: betBExternalId,
        idempotencyKey: `idem-b-${tag}`,
        playerId,
        walletId: wallet.id,
        roundId: `round-${tag}`,
        gameId: 'aviator',
        kind: WagerTransactionKind.Bet,
        money: { amount: '80.00', currency: 'BRL' },
      }),
      primaryNode.processWager.execute({
        providerId: 'prov-pg-c2',
        externalTransactionId: winCExternalId,
        idempotencyKey: `idem-c-${tag}`,
        playerId,
        walletId: wallet.id,
        roundId: `round-${tag}`,
        gameId: 'aviator',
        kind: WagerTransactionKind.Win,
        money: { amount: '30.00', currency: 'BRL' },
      }),
      primaryNode.processWager.execute({
        providerId: 'prov-pg-c2',
        externalTransactionId: rollbackDExternalId,
        idempotencyKey: `idem-d-${tag}`,
        playerId,
        walletId: wallet.id,
        roundId: `round-${tag}`,
        gameId: 'aviator',
        kind: WagerTransactionKind.Rollback,
        money: { amount: '80.00', currency: 'BRL' },
        referenceExternalTransactionId: betAExternalId,
      }),
    ]);

    // Se o ROLLBACK D entrou no banco antes da BET A (PENDING_REFERENCE), o PendingReferenceWorker resolve
    if (resD.status === WagerTransactionStatus.PendingReference) {
      const pendingWorker = new PendingReferenceWorker(
        primaryNode.txRepo,
        primaryNode.outboxRepo,
        primaryNode.uow,
        primaryNode.processWager,
        primaryNode.logger,
        primaryNode.metrics,
      );
      await pendingWorker.processPendingBatch(20, new Date(Date.now() + 5000));
    }

    expect(resA.status).toBe(WagerTransactionStatus.Processed);
    expect(resC.status).toBe(WagerTransactionStatus.Processed);
    expect([
      WagerTransactionStatus.Processed,
      WagerTransactionStatus.Rejected,
    ]).toContain(resB.status);

    // Verifica que NENHUMA entrada intermediária do ledger ficou negativa no PostgreSQL
    const ledgerPage = await primaryNode.ledgerRepo.findByWalletId(wallet.id, { limit: 50 });
    for (const entry of ledgerPage.entries) {
      expect(Number(entry.balanceBefore.amount)).toBeGreaterThanOrEqual(0);
      expect(Number(entry.balanceAfter.amount)).toBeGreaterThanOrEqual(0);
    }

    // Reconciliação contábil exata correspondente à serialização ocorrida
    const rec = await primaryNode.reconcileWallet.execute(wallet.id);
    expect(rec.consistent).toBe(true);
    expect(rec.difference.amount).toBe('0.00');
    // Se BET B foi rejeitada por saldo insuficiente, saldo final = 100 - 80(A) + 30(C) + 80(D) = 130.00
    // Se BET B foi processada (porque WIN C ou ROLLBACK D creditaram antes), saldo final = 50.00
    expect(['130.00', '50.00']).toContain(rec.storedBalance.amount);
  });

  it('Cenário 3 (§13.3): 10 carteiras distintas processadas em paralelo no PostgreSQL real sem contenção cruzada', async () => {
    const tag = `pg-c3-${Date.now()}`;
    const isolatedNode = await createPostgresNode(dbPort);

    try {
      const wallets = await Promise.all(
        Array.from({ length: 10 }, (_, idx) =>
          isolatedNode.createWallet.execute({
            playerId: `player-${tag}-${idx}`,
            initialBalance: { amount: '200.00', currency: 'BRL' },
          }),
        ),
      );
      for (const w of wallets) createdWalletIds.push(w.id);

      // Processa 1 BET (-50) em cada uma das 10 carteiras simultaneamente
      await Promise.all(
        wallets.map((w, idx) =>
          isolatedNode.processWager.execute({
            providerId: 'prov-pg-c3',
            externalTransactionId: `bet-c3-${tag}-${idx}`,
            idempotencyKey: `idem-c3-${tag}-${idx}`,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `round-c3-${idx}`,
            gameId: 'spaceman',
            kind: WagerTransactionKind.Bet,
            money: { amount: '50.00', currency: 'BRL' },
          }),
        ),
      );

      // Como são 10 carteiras distintas, não deve haver nenhum conflito de lock entre elas
      const snapshot = isolatedNode.metrics.getSnapshot() as { lockConflictsCount: number };
      expect(snapshot.lockConflictsCount).toBe(0);

      for (const w of wallets) {
        const rec = await isolatedNode.reconcileWallet.execute(w.id);
        expect(rec.consistent).toBe(true);
        expect(rec.storedBalance.amount).toBe('150.00');
        expect(rec.calculatedBalance.amount).toBe('150.00');
      }
    } finally {
      await isolatedNode.orm.close(true);
    }
  });

  it('Cenário 4 (§13.4): 3 instâncias simultâneas independentes (3 pools MikroORM distintos) disputando a mesma carteira no PostgreSQL real', async () => {
    const tag = `pg-c4-${Date.now()}`;
    const node1 = await createPostgresNode(dbPort);
    const node2 = await createPostgresNode(dbPort);
    const node3 = await createPostgresNode(dbPort);

    try {
      const wallet = await node1.createWallet.execute({
        playerId: `player-${tag}`,
        initialBalance: { amount: '150.00', currency: 'BRL' },
      });
      createdWalletIds.push(wallet.id);

      const nodes = [node1, node2, node3];

      // 15 apostas concorrentes de 25.00 BRL distribuídas entre as 3 instâncias independentes (apenas 6 cabem no saldo de 150.00 BRL)
      const results = await Promise.all(
        Array.from({ length: 15 }, (_, idx) => {
          const targetNode = nodes[idx % 3];
          return targetNode.processWager.execute({
            providerId: 'prov-pg-c4',
            externalTransactionId: `bet-multi-node-${tag}-${idx}`,
            idempotencyKey: `idem-multi-node-${tag}-${idx}`,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: `round-${tag}-${idx}`,
            gameId: 'mines',
            kind: WagerTransactionKind.Bet,
            money: { amount: '25.00', currency: 'BRL' },
          });
        }),
      );

      const processed = results.filter((r) => r.status === WagerTransactionStatus.Processed);
      const rejected = results.filter((r) => r.status === WagerTransactionStatus.Rejected);

      expect(processed.length).toBe(6);
      expect(rejected.length).toBe(9);

      const rec = await node2.reconcileWallet.execute(wallet.id);
      expect(rec.consistent).toBe(true);
      expect(rec.storedBalance.amount).toBe('0.00');
      expect(rec.calculatedBalance.amount).toBe('0.00');
      expect(rec.difference.amount).toBe('0.00');
      expect(rec.checkedEntries).toBe(7); // 1 OPENING + 6 BETs
    } finally {
      await Promise.all([
        node1.orm.close(true),
        node2.orm.close(true),
        node3.orm.close(true),
      ]);
    }
  });

  it('Cenário 5 (§13.5): Worker morto após commit no PostgreSQL e antes do ACK no SQS -> reentrega deduplicada via inbox_messages real', async () => {
    const tag = `pg-c5-${Date.now()}`;
    const nodeA = await createPostgresNode(dbPort);
    const nodeB = await createPostgresNode(dbPort);

    try {
      const wallet = await nodeA.createWallet.execute({
        playerId: `player-${tag}`,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });
      createdWalletIds.push(wallet.id);

      const rawMessage: SqsRawMessage = {
        messageId: `sqs-msg-crash-${tag}`,
        receiptHandle: `rh-attempt-1-${tag}`,
        body: JSON.stringify({
          messageId: `sqs-msg-crash-${tag}`,
          type: 'WagerTransactionRequested',
          occurredAt: new Date().toISOString(),
          data: {
            providerId: 'prov-pg-c5',
            externalTransactionId: `ext-crash-${tag}`,
            idempotencyKey: `idem-crash-${tag}`,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: `round-${tag}`,
            gameId: 'crash',
            kind: WagerTransactionKind.Bet,
            money: { amount: '40.00', currency: 'BRL' },
          },
        }),
        attributes: { ApproximateReceiveCount: '1' },
      };

      // Simula que o Worker A faz commit no PostgreSQL mas a chamada deleteMessage (ACK no SQS) falha porque o worker morreu
      let ackCalls = 0;
      const crashingSqsClient: SqsConsumerClientPort = {
        receiveMessages: async () => [rawMessage],
        deleteMessage: async () => {
          ackCalls += 1;
          throw new Error('Simulated Worker Crash right after PostgreSQL commit and before SQS ACK');
        },
        changeMessageVisibility: async () => {},
        sendToDlq: async () => {},
      };

      const consumerA = new WagerTransactionConsumer(
        nodeA.processWager,
        crashingSqsClient,
        nodeA.logger,
        nodeA.metrics,
      );

      // O handleMessage no Worker A tenta dar ACK após o commit, falha no ACK e agenda retry
      const decisionA = await consumerA.handleMessage(rawMessage);
      expect(ackCalls).toBe(1);
      expect(decisionA).toBe('RETRY');

      // Verifica no PostgreSQL real que o commit JÁ ocorreu (saldo 60.00 BRL e inbox_messages gravado)
      const inboxRecord = await nodeB.inboxRepo.find(
        WagerTransactionConsumer.CONSUMER_NAME,
        rawMessage.messageId,
      );
      expect(inboxRecord).not.toBeNull();
      expect(inboxRecord!.isProcessed()).toBe(true);

      // Agora o SQS reentrega a mesma mensagem para o Worker B (outra instância conectada ao mesmo PostgreSQL)
      let workerBAcked = false;
      const healthySqsClient: SqsConsumerClientPort = {
        receiveMessages: async () => [{ ...rawMessage, receiptHandle: `rh-attempt-2-${tag}` }],
        deleteMessage: async () => {
          workerBAcked = true;
        },
        changeMessageVisibility: async () => {},
        sendToDlq: async () => {},
      };

      const consumerB = new WagerTransactionConsumer(
        nodeB.processWager,
        healthySqsClient,
        nodeB.logger,
        nodeB.metrics,
      );

      const decisionsB = await consumerB.pollOnce();
      expect(decisionsB).toEqual(['ACK']);
      expect(workerBAcked).toBe(true);

      // Verifica que a reentrega foi detectada como duplicata e que o saldo NÃO foi debitado duas vezes
      const snapshotB = nodeB.metrics.getSnapshot() as { duplicatesDetected: number };
      expect(snapshotB.duplicatesDetected).toBe(1);

      const rec = await nodeB.reconcileWallet.execute(wallet.id);
      expect(rec.consistent).toBe(true);
      expect(rec.storedBalance.amount).toBe('60.00');
      expect(rec.calculatedBalance.amount).toBe('60.00');
      expect(rec.checkedEntries).toBe(2); // 1 OPENING + 1 BET (nenhum débito duplicado!)
    } finally {
      await Promise.all([nodeA.orm.close(true), nodeB.orm.close(true)]);
    }
  });

  it('Cenário 6 (§13.6): Dois publicadores concorrentes sobre a mesma outbox_messages no PostgreSQL real (FOR UPDATE SKIP LOCKED)', async () => {
    const tag = `pg-c6-${Date.now()}`;
    const nodePub1 = await createPostgresNode(dbPort);
    const nodePub2 = await createPostgresNode(dbPort);

    try {
      // Drena qualquer evento anterior pendente na tabela outbox_messages
      const drainWorker = new OutboxDispatcherWorker(
        primaryNode.outboxRepo,
        primaryNode.uow,
        { publish: async () => {} },
        primaryNode.logger,
        primaryNode.metrics,
      );
      while ((await drainWorker.dispatchBatch(100)).published > 0) {
        // draining
      }

      // Gera 10 novas carteiras (10 eventos na outbox_messages real)
      for (let i = 0; i < 10; i++) {
        const w = await primaryNode.createWallet.execute({
          playerId: `player-${tag}-${i}`,
          initialBalance: { amount: '50.00', currency: 'BRL' },
        });
        createdWalletIds.push(w.id);
      }

      const publishedByWorker1: string[] = [];
      const publishedByWorker2: string[] = [];

      const worker1 = new OutboxDispatcherWorker(
        nodePub1.outboxRepo,
        nodePub1.uow,
        {
          publish: async (msg: OutboxMessage) => {
            await new Promise((r) => setTimeout(r, 15));
            publishedByWorker1.push(msg.id);
          },
        },
        nodePub1.logger,
        nodePub1.metrics,
      );

      const worker2 = new OutboxDispatcherWorker(
        nodePub2.outboxRepo,
        nodePub2.uow,
        {
          publish: async (msg: OutboxMessage) => {
            await new Promise((r) => setTimeout(r, 15));
            publishedByWorker2.push(msg.id);
          },
        },
        nodePub2.logger,
        nodePub2.metrics,
      );

      // Dispara os 2 workers concorrentemente com batchSize=5 sobre a tabela outbox_messages real
      const [batch1, batch2] = await Promise.all([
        worker1.dispatchBatch(5),
        worker2.dispatchBatch(5),
      ]);

      expect(batch1.published + batch2.published).toBe(10);

      // Verifica que nenhum evento foi publicado em duplicidade pelos dois workers (SKIP LOCKED + lease)
      const overlap = publishedByWorker1.filter((id) => publishedByWorker2.includes(id));
      expect(overlap.length).toBe(0);
    } finally {
      await Promise.all([nodePub1.orm.close(true), nodePub2.orm.close(true)]);
    }
  });

  it('Cenário 7 (§13.7): ROLLBACK e REFUND entregues antes da transação referenciada no PostgreSQL real', async () => {
    const tag = `pg-c7-${Date.now()}`;
    const wallet = await primaryNode.createWallet.execute({
      playerId: `player-${tag}`,
      initialBalance: { amount: '200.00', currency: 'BRL' },
    });
    createdWalletIds.push(wallet.id);

    // 1. ROLLBACK (50.00) e REFUND (30.00) chegam antes das BETs referenciadas
    const [earlyRollback, earlyRefund] = await Promise.all([
      primaryNode.processWager.execute({
        providerId: 'prov-pg-c7',
        externalTransactionId: `rb-early-${tag}`,
        idempotencyKey: `idem-rb-early-${tag}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `round-1-${tag}`,
        gameId: 'roulette',
        kind: WagerTransactionKind.Rollback,
        money: { amount: '50.00', currency: 'BRL' },
        referenceExternalTransactionId: `bet-1-${tag}`,
      }),
      primaryNode.processWager.execute({
        providerId: 'prov-pg-c7',
        externalTransactionId: `rf-early-${tag}`,
        idempotencyKey: `idem-rf-early-${tag}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `round-2-${tag}`,
        gameId: 'roulette',
        kind: WagerTransactionKind.Refund,
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: `bet-2-${tag}`,
      }),
    ]);

    expect(earlyRollback.status).toBe(WagerTransactionStatus.PendingReference);
    expect(earlyRefund.status).toBe(WagerTransactionStatus.PendingReference);

    // 2. As BETs originais chegam (200 - 50 - 30 = 120.00 BRL)
    await primaryNode.processWager.execute({
      providerId: 'prov-pg-c7',
      externalTransactionId: `bet-1-${tag}`,
      idempotencyKey: `idem-bet-1-${tag}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `round-1-${tag}`,
      gameId: 'roulette',
      kind: WagerTransactionKind.Bet,
      money: { amount: '50.00', currency: 'BRL' },
    });
    await primaryNode.processWager.execute({
      providerId: 'prov-pg-c7',
      externalTransactionId: `bet-2-${tag}`,
      idempotencyKey: `idem-bet-2-${tag}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `round-2-${tag}`,
      gameId: 'roulette',
      kind: WagerTransactionKind.Bet,
      money: { amount: '30.00', currency: 'BRL' },
    });

    // 3. PendingReferenceWorker resolve ambas no PostgreSQL real -> saldo volta para 200.00 BRL
    const pendingWorker = new PendingReferenceWorker(
      primaryNode.txRepo,
      primaryNode.outboxRepo,
      primaryNode.uow,
      primaryNode.processWager,
      primaryNode.logger,
      primaryNode.metrics,
    );
    const stats = await pendingWorker.processPendingBatch(50, new Date(Date.now() + 5000));
    expect(stats.resolved).toBeGreaterThanOrEqual(2);

    const rec = await primaryNode.reconcileWallet.execute(wallet.id);
    expect(rec.consistent).toBe(true);
    expect(rec.storedBalance.amount).toBe('200.00');
    expect(rec.calculatedBalance.amount).toBe('200.00');
    expect(rec.checkedEntries).toBe(5); // OPENING + 2 BETs + 1 ROLLBACK + 1 REFUND
  });

  it('Cenário 8 (§13.8): Recuperação após reinício de serviço e validação da invariante universal wallet.balance == sum(ledger) em todas as carteiras no PostgreSQL real', async () => {
    const tag = `pg-c8-${Date.now()}`;

    // 1. Sobe uma instância temporária (Node Crashable), cria carteira e deixa um ROLLBACK pendente
    const crashableNode = await createPostgresNode(dbPort);
    const wallet = await crashableNode.createWallet.execute({
      playerId: `player-${tag}`,
      initialBalance: { amount: '120.00', currency: 'BRL' },
    });
    createdWalletIds.push(wallet.id);

    await crashableNode.processWager.execute({
      providerId: 'prov-pg-c8',
      externalTransactionId: `rb-restart-${tag}`,
      idempotencyKey: `idem-rb-restart-${tag}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `round-${tag}`,
      gameId: 'blackjack',
      kind: WagerTransactionKind.Rollback,
      money: { amount: '45.00', currency: 'BRL' },
      referenceExternalTransactionId: `bet-restart-${tag}`,
    });

    // Encerra abruptamente a conexão do Node Crashable
    await crashableNode.orm.close(true);

    // 2. Sobe uma nova instância (Recovery Node) conectada ao mesmo PostgreSQL real
    const recoveryNode = await createPostgresNode(dbPort);
    try {
      // A BET chega na nova instância
      await recoveryNode.processWager.execute({
        providerId: 'prov-pg-c8',
        externalTransactionId: `bet-restart-${tag}`,
        idempotencyKey: `idem-bet-restart-${tag}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `round-${tag}`,
        gameId: 'blackjack',
        kind: WagerTransactionKind.Bet,
        money: { amount: '45.00', currency: 'BRL' },
      });

      const pendingWorker = new PendingReferenceWorker(
        recoveryNode.txRepo,
        recoveryNode.outboxRepo,
        recoveryNode.uow,
        recoveryNode.processWager,
        recoveryNode.logger,
        recoveryNode.metrics,
      );
      await pendingWorker.processPendingBatch(50, new Date(Date.now() + 5000));

      const outboxWorker = new OutboxDispatcherWorker(
        recoveryNode.outboxRepo,
        recoveryNode.uow,
        { publish: async () => {} },
        recoveryNode.logger,
        recoveryNode.metrics,
      );
      while ((await outboxWorker.dispatchBatch(100)).published > 0) {
        // dispatch remaining outbox events
      }

      // 3. Invariante Universal Contábil (§2 & §13): verifica TODAS as carteiras criadas na suíte no PostgreSQL real
      expect(createdWalletIds.length).toBeGreaterThanOrEqual(24);
      for (const wId of createdWalletIds) {
        const rec = await recoveryNode.reconcileWallet.execute(wId);
        expect(rec.consistent).toBe(true);
        expect(rec.difference.amount).toBe('0.00');
        expect(Number(rec.storedBalance.amount)).toBeGreaterThanOrEqual(0);
      }
    } finally {
      await recoveryNode.orm.close(true);
    }
  });
});
