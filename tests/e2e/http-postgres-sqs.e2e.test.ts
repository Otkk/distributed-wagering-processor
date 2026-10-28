import 'reflect-metadata';
import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { EntityManager } from '@mikro-orm/postgresql';
import { AppModule } from '../../src/app.module';
import { WagerTransactionKind } from '../../src/domain/models/wager-transaction';
import { GlobalHttpExceptionFilter } from '../../src/infrastructure/http/filters/http-exception.filter';
import { AppLoggerService } from '../../src/infrastructure/logger/app-logger.service';
import { OutboxDispatcherWorker } from '../../src/infrastructure/messaging/outbox/outbox-dispatcher.worker';
import { PendingReferenceWorker } from '../../src/infrastructure/messaging/outbox/pending-reference.worker';
import { WagerTransactionConsumer } from '../../src/infrastructure/messaging/sqs/wager-transaction.consumer';
import {
  ensureRealPostgres,
  ensureSqsEndpoint,
  SqsEndpointHandle,
} from '../helpers/real-infra.helper';

describe('E2E Suite: Real NestJS HTTP Server + Real PostgreSQL + Real AWS SQS SDK over HTTP', () => {
  let app: INestApplication;
  let baseUrl: string;
  let sqsHandle: SqsEndpointHandle;
  let sqsEndpoint: string;
  let outboxWorker: OutboxDispatcherWorker;
  let pendingRefWorker: PendingReferenceWorker;
  let sqsConsumer: WagerTransactionConsumer;

  beforeAll(async () => {
    // 1. Garante PostgreSQL real acessível e com migrations aplicadas
    const { orm, port: dbPort } = await ensureRealPostgres();
    await orm.close(true);

    // 2. Conecta ao LocalStack real (se ativo na 4566) ou inicia servidor HTTP SQS real
    sqsHandle = await ensureSqsEndpoint();
    sqsEndpoint = sqsHandle.endpoint;

    process.env.DB_PORT = String(dbPort);
    process.env.DISABLE_WORKERS = 'true';
    process.env.AWS_REGION = 'us-east-1';
    process.env.AWS_ENDPOINT = sqsEndpoint;
    process.env.AWS_ACCESS_KEY_ID = 'test';
    process.env.AWS_SECRET_ACCESS_KEY = 'test';
    process.env.SQS_WAGER_QUEUE_URL = `${sqsEndpoint}/000000000000/wager-transactions.fifo`;
    process.env.SQS_WAGER_DLQ_URL = `${sqsEndpoint}/000000000000/wager-transactions-dlq.fifo`;
    process.env.SQS_OUTBOX_QUEUE_URL = `${sqsEndpoint}/000000000000/wagering-events.fifo`;
    process.env.SQS_EVENTS_QUEUE_URL = `${sqsEndpoint}/000000000000/wagering-events.fifo`;

    // 3. Sobe a aplicação NestJS real escutando em porta TCP real
    app = await NestFactory.create(AppModule, { logger: false });
    const logger = app.get(AppLoggerService);
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        transform: true,
        forbidNonWhitelisted: true,
      }),
    );
    app.useGlobalFilters(new GlobalHttpExceptionFilter(logger));

    await app.listen(0, '127.0.0.1');
    const httpServer = app.getHttpServer();
    const addr = httpServer.address();
    const port = typeof addr === 'object' && addr ? addr.port : 3000;
    baseUrl = `http://127.0.0.1:${port}`;

    outboxWorker = app.get(OutboxDispatcherWorker);
    pendingRefWorker = app.get(PendingReferenceWorker);
    sqsConsumer = app.get(WagerTransactionConsumer);
  });

  afterAll(async () => {
    if (app) {
      await app.close();
    }
    if (sqsHandle) {
      await sqsHandle.stop();
    }
  });

  it('1. Health & Readiness: /health/live, /health/ready (PostgreSQL + SQS reais) e /health/metrics', async () => {
    const liveRes = await fetch(`${baseUrl}/health/live`, {
      headers: { 'X-Correlation-Id': 'e2e-health-corr-1' },
    });
    expect(liveRes.status).toBe(200);
    expect(liveRes.headers.get('x-correlation-id')).toBe('e2e-health-corr-1');
    const liveBody = (await liveRes.json()) as { status: string };
    expect(liveBody.status).toBe('ok');

    const readyRes = await fetch(`${baseUrl}/health/ready`);
    expect(readyRes.status).toBe(200);
    const readyBody = (await readyRes.json()) as {
      status: string;
      services: { database: string; sqs: string };
    };
    expect(readyBody.status).toBe('ok');
    expect(readyBody.services.database).toBe('ok');
    expect(readyBody.services.sqs).toBe('ok');

    const metricsRes = await fetch(`${baseUrl}/health/metrics`);
    expect(metricsRes.status).toBe(200);
    const metricsBody = (await metricsRes.json()) as {
      transactionsByStatus: Record<string, number>;
    };
    expect(typeof metricsBody.transactionsByStatus.PROCESSED).toBe('number');
  });

  it('2. Fluxo HTTP Completo: criação de carteira, BET, replay idempotente, conflito 409, saldo insuficiente 422, WIN, LOSS, REFUND e reconciliação no PostgreSQL', async () => {
    const runId = `${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const playerId = `e2e-player-${runId}`;

    // 2.1 Cria carteira com 100.00 BRL
    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      }),
    });
    expect(createRes.status).toBe(201);
    const wallet = (await createRes.json()) as {
      id: string;
      playerId: string;
      balance: { amount: string; currency: string };
      version: number;
    };
    expect(wallet.balance.amount).toBe('100.00');
    expect(wallet.version).toBe(1);

    // 2.2 Duplicidade (playerId + currency) -> 409 Conflict
    const dupRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '50.00', currency: 'BRL' },
      }),
    });
    expect(dupRes.status).toBe(409);

    // 2.3 BET de 30.00 BRL -> 200 OK (saldo 70.00 BRL)
    const betPayload = {
      providerId: 'e2e-prov',
      externalTransactionId: `bet-1-${runId}`,
      playerId,
      walletId: wallet.id,
      roundId: `round-1-${runId}`,
      gameId: 'spaceman',
      kind: WagerTransactionKind.Bet,
      money: { amount: '30.00', currency: 'BRL' },
    };
    const betRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-bet-1-${runId}`,
      },
      body: JSON.stringify(betPayload),
    });
    expect(betRes.status).toBe(200);
    const betBody = (await betRes.json()) as {
      transactionId: string;
      status: string;
      balance: { amount: string; currency: string };
      idempotentReplay: boolean;
    };
    expect(betBody.status).toBe('PROCESSED');
    expect(betBody.balance.amount).toBe('70.00');
    expect(betBody.idempotentReplay).toBe(false);

    // 2.4 Replay idempotente da mesma BET -> 200 OK (idempotentReplay: true, saldo 70.00 BRL)
    const replayRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-bet-1-${runId}`,
      },
      body: JSON.stringify(betPayload),
    });
    expect(replayRes.status).toBe(200);
    const replayBody = (await replayRes.json()) as typeof betBody;
    expect(replayBody.transactionId).toBe(betBody.transactionId);
    expect(replayBody.balance.amount).toBe('70.00');
    expect(replayBody.idempotentReplay).toBe(true);

    // 2.5 Mesma Idempotency-Key com valor adulterado -> 409 Conflict
    const conflictRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-bet-1-${runId}`,
      },
      body: JSON.stringify({
        ...betPayload,
        money: { amount: '35.00', currency: 'BRL' },
      }),
    });
    expect(conflictRes.status).toBe(409);

    // 2.6 BET acima do saldo (500.00 BRL > 70.00 BRL) -> 422 Unprocessable Entity (INSUFFICIENT_FUNDS)
    const overBetRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-overbet-${runId}`,
      },
      body: JSON.stringify({
        ...betPayload,
        externalTransactionId: `bet-over-${runId}`,
        money: { amount: '500.00', currency: 'BRL' },
      }),
    });
    expect(overBetRes.status).toBe(422);
    const overBetBody = (await overBetRes.json()) as { failureCode: string; status: string };
    expect(overBetBody.failureCode).toBe('INSUFFICIENT_FUNDS');

    // 2.7 WIN de 50.00 BRL -> 200 OK (saldo 120.00 BRL)
    const winRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-win-1-${runId}`,
      },
      body: JSON.stringify({
        providerId: 'e2e-prov',
        externalTransactionId: `win-1-${runId}`,
        playerId,
        walletId: wallet.id,
        roundId: `round-1-${runId}`,
        gameId: 'spaceman',
        kind: WagerTransactionKind.Win,
        money: { amount: '50.00', currency: 'BRL' },
      }),
    });
    expect(winRes.status).toBe(200);
    const winBody = (await winRes.json()) as typeof betBody;
    expect(winBody.balance.amount).toBe('120.00');

    // 2.8 LOSS de 30.00 BRL -> 200 OK (não altera saldo: permanece 120.00 BRL)
    const lossRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-loss-1-${runId}`,
      },
      body: JSON.stringify({
        providerId: 'e2e-prov',
        externalTransactionId: `loss-1-${runId}`,
        playerId,
        walletId: wallet.id,
        roundId: `round-1-${runId}`,
        gameId: 'spaceman',
        kind: WagerTransactionKind.Loss,
        money: { amount: '30.00', currency: 'BRL' },
      }),
    });
    expect(lossRes.status).toBe(200);
    const lossBody = (await lossRes.json()) as typeof betBody;
    expect(lossBody.balance.amount).toBe('120.00');

    // 2.9 REFUND de 30.00 BRL referenciando bet-1 -> 200 OK (saldo 150.00 BRL)
    const refundRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-refund-1-${runId}`,
      },
      body: JSON.stringify({
        providerId: 'e2e-prov',
        externalTransactionId: `refund-1-${runId}`,
        playerId,
        walletId: wallet.id,
        roundId: `round-1-${runId}`,
        gameId: 'spaceman',
        kind: WagerTransactionKind.Refund,
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: `bet-1-${runId}`,
      }),
    });
    expect(refundRes.status).toBe(200);
    const refundBody = (await refundRes.json()) as typeof betBody;
    expect(refundBody.balance.amount).toBe('150.00');

    // 2.10 Reconciliação contábil e consulta paginada ao ledger via HTTP
    const recRes = await fetch(`${baseUrl}/wallets/${wallet.id}/reconcile`);
    expect(recRes.status).toBe(200);
    const recBody = (await recRes.json()) as {
      consistent: boolean;
      storedBalance: { amount: string };
      calculatedBalance: { amount: string };
      difference: { amount: string };
      checkedEntries: number;
    };
    expect(recBody.consistent).toBe(true);
    expect(recBody.storedBalance.amount).toBe('150.00');
    expect(recBody.calculatedBalance.amount).toBe('150.00');
    expect(recBody.difference.amount).toBe('0.00');
    expect(recBody.checkedEntries).toBe(4); // OPENING (+100) + BET (-30) + WIN (+50) + REFUND (+30)

    const ledgerRes = await fetch(`${baseUrl}/wallets/${wallet.id}/ledger?limit=2`);
    expect(ledgerRes.status).toBe(200);
    const ledgerPage1 = (await ledgerRes.json()) as {
      entries: Array<{ id: string }>;
      nextCursor?: string;
    };
    expect(ledgerPage1.entries.length).toBe(2);
    expect(ledgerPage1.nextCursor).toBeDefined();

    const ledgerRes2 = await fetch(
      `${baseUrl}/wallets/${wallet.id}/ledger?limit=2&cursor=${encodeURIComponent(ledgerPage1.nextCursor!)}`,
    );
    expect(ledgerRes2.status).toBe(200);
    const ledgerPage2 = (await ledgerRes2.json()) as typeof ledgerPage1;
    expect(ledgerPage2.entries.length).toBe(2);
  });

  it('3. Referência Fora de Ordem (202 Accepted PENDING_REFERENCE) e Resolução pelo PendingReferenceWorker no PostgreSQL', async () => {
    const runId = `ooo-${Date.now()}`;
    const playerId = `player-${runId}`;

    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      }),
    });
    const wallet = (await createRes.json()) as { id: string };

    // 3.1 ROLLBACK chega ANTES da BET referenciada -> 202 Accepted (PENDING_REFERENCE)
    const rollbackRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-rb-${runId}`,
      },
      body: JSON.stringify({
        providerId: 'e2e-prov',
        externalTransactionId: `rb-${runId}`,
        playerId,
        walletId: wallet.id,
        roundId: `round-${runId}`,
        gameId: 'aviator',
        kind: WagerTransactionKind.Rollback,
        money: { amount: '40.00', currency: 'BRL' },
        referenceExternalTransactionId: `late-bet-${runId}`,
      }),
    });
    expect(rollbackRes.status).toBe(202);
    const rollbackBody = (await rollbackRes.json()) as {
      status: string;
      balance: { amount: string };
    };
    expect(rollbackBody.status).toBe('PENDING_REFERENCE');
    expect(rollbackBody.balance.amount).toBe('100.00');

    // 3.2 A BET atrasada chega -> 200 OK (saldo cai para 60.00 BRL)
    const lateBetRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-late-bet-${runId}`,
      },
      body: JSON.stringify({
        providerId: 'e2e-prov',
        externalTransactionId: `late-bet-${runId}`,
        playerId,
        walletId: wallet.id,
        roundId: `round-${runId}`,
        gameId: 'aviator',
        kind: WagerTransactionKind.Bet,
        money: { amount: '40.00', currency: 'BRL' },
      }),
    });
    expect(lateBetRes.status).toBe(200);

    // 3.3 PendingReferenceWorker processa no PostgreSQL real e resolve o ROLLBACK -> saldo volta a 100.00 BRL
    const workerStats = await pendingRefWorker.processPendingBatch(
      50,
      new Date(Date.now() + 5000),
    );
    expect(workerStats.resolved).toBeGreaterThanOrEqual(1);

    const recRes = await fetch(`${baseUrl}/wallets/${wallet.id}/reconcile`);
    const recBody = (await recRes.json()) as {
      consistent: boolean;
      storedBalance: { amount: string };
      calculatedBalance: { amount: string };
    };
    expect(recBody.consistent).toBe(true);
    expect(recBody.storedBalance.amount).toBe('100.00');
    expect(recBody.calculatedBalance.amount).toBe('100.00');
  });

  it('4. Concorrência Real HTTP + PostgreSQL (SELECT ... FOR UPDATE): 20 BETs simultâneas de 10.00 BRL em carteira com 100.00 BRL', async () => {
    const runId = `conc-${Date.now()}`;
    const playerId = `player-${runId}`;

    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      }),
    });
    expect(createRes.status).toBe(201);
    const wallet = (await createRes.json()) as { id: string };

    // Dispara 20 requisições HTTP POST concorrentes contra a mesma carteira
    const responses = await Promise.all(
      Array.from({ length: 20 }, (_, idx) =>
        fetch(`${baseUrl}/wagering/transactions`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': `idem-conc-${runId}-${idx}`,
          },
          body: JSON.stringify({
            providerId: 'e2e-prov',
            externalTransactionId: `ext-conc-${runId}-${idx}`,
            playerId,
            walletId: wallet.id,
            roundId: `round-${runId}-${idx}`,
            gameId: 'mines',
            kind: WagerTransactionKind.Bet,
            money: { amount: '10.00', currency: 'BRL' },
          }),
        }),
      ),
    );

    const statusCodes = responses.map((r) => r.status);
    const count200 = statusCodes.filter((s) => s === 200).length;
    const count422 = statusCodes.filter((s) => s === 422).length;
    const count5xx = statusCodes.filter((s) => s >= 500).length;

    expect(count5xx).toBe(0);
    expect(count200).toBe(10);
    expect(count422).toBe(10);

    // Reconciliação final: saldo deve ser exatamente 0.00 BRL e consistente com as 11 entradas do ledger
    const recRes = await fetch(`${baseUrl}/wallets/${wallet.id}/reconcile`);
    const recBody = (await recRes.json()) as {
      consistent: boolean;
      storedBalance: { amount: string };
      calculatedBalance: { amount: string };
      difference: { amount: string };
      checkedEntries: number;
    };
    expect(recBody.consistent).toBe(true);
    expect(recBody.storedBalance.amount).toBe('0.00');
    expect(recBody.calculatedBalance.amount).toBe('0.00');
    expect(recBody.difference.amount).toBe('0.00');
    expect(recBody.checkedEntries).toBe(11);

    // Verifica que a contenção de lock por carteira foi contabilizada nas métricas (§12)
    const metricsRes = await fetch(`${baseUrl}/health/metrics`);
    const metricsBody = (await metricsRes.json()) as { lockConflictsCount: number };
    expect(metricsBody.lockConflictsCount).toBeGreaterThan(0);
  });

  it('5. Outbox Dispatcher (wagering-events.fifo) + SQS FIFO Consumer (wager-transactions.fifo) + Persistent Inbox sem colisão de filas', async () => {
    const runId = `sqs-${Date.now()}`;
    const playerId = `player-${runId}`;

    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '80.00', currency: 'BRL' },
      }),
    });
    const wallet = (await createRes.json()) as { id: string };

    // 5.1 Despacha eventos pendentes da tabela outbox_messages para a fila de eventos wagering-events.fifo
    const dispatchRes = await outboxWorker.dispatchBatch(100);
    expect(dispatchRes.published).toBeGreaterThanOrEqual(1);
    expect(dispatchRes.failed).toBe(0);

    const sqsClient = new SQSClient({
      region: 'us-east-1',
      endpoint: sqsEndpoint,
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    });

    // Verifica que os eventos de integração foram publicados em wagering-events.fifo (e NÃO em wager-transactions.fifo!)
    const eventsCheck = await sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: `${sqsEndpoint}/000000000000/wagering-events.fifo`,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 0,
      }),
    );
    expect((eventsCheck.Messages || []).length).toBeGreaterThanOrEqual(1);

    // 5.2 Publica mensagem FIFO de comando de aposta em wager-transactions.fifo (sem precisar limpar fila!)
    const wagerEnvelope = {
      messageId: `msg-${runId}`,
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: {
        providerId: 'sqs-e2e-prov',
        externalTransactionId: `sqs-bet-${runId}`,
        idempotencyKey: `sqs-idem-${runId}`,
        playerId,
        walletId: wallet.id,
        roundId: `round-${runId}`,
        gameId: 'plinko',
        kind: WagerTransactionKind.Bet,
        money: { amount: '25.00', currency: 'BRL' },
      },
    };

    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: `${sqsEndpoint}/000000000000/wager-transactions.fifo`,
        MessageBody: JSON.stringify(wagerEnvelope),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: wagerEnvelope.data.idempotencyKey,
      }),
    );

    // 5.3 WagerTransactionConsumer consome via HTTP SQS, persiste no PostgreSQL e remove da fila
    const pollRes = await sqsConsumer.pollOnce();
    expect(pollRes).toEqual(['ACK']);

    // 5.4 Verifica saldo atualizado (80.00 - 25.00 = 55.00 BRL) e reconciliação no PostgreSQL
    const recRes = await fetch(`${baseUrl}/wallets/${wallet.id}/reconcile`);
    const recBody = (await recRes.json()) as {
      consistent: boolean;
      storedBalance: { amount: string };
      calculatedBalance: { amount: string };
    };
    expect(recBody.consistent).toBe(true);
    expect(recBody.storedBalance.amount).toBe('55.00');
    expect(recBody.calculatedBalance.amount).toBe('55.00');
  });

  it('6. Detecção de Divergência Contábil (§9): sinaliza na resposta, loga erro estruturado, incrementa métrica e não corrige silenciosamente', async () => {
    const runId = `div-${Date.now()}`;
    const playerId = `player-${runId}`;

    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      }),
    });
    const wallet = (await createRes.json()) as { id: string };

    // Adultera diretamente o saldo armazenado no PostgreSQL para simular corrupção externa
    const em = app.get(EntityManager);
    await em
      .getConnection()
      .execute(`UPDATE wallets SET balance_amount = 145.00 WHERE id = ?`, [wallet.id]);

    const recRes = await fetch(`${baseUrl}/wallets/${wallet.id}/reconciliation`, {
      method: 'POST',
    });
    expect(recRes.status).toBe(200);
    const recBody = (await recRes.json()) as {
      consistent: boolean;
      storedBalance: { amount: string };
      calculatedBalance: { amount: string };
      difference: { amount: string };
    };
    expect(recBody.consistent).toBe(false);
    expect(recBody.storedBalance.amount).toBe('145.00');
    expect(recBody.calculatedBalance.amount).toBe('100.00');
    expect(recBody.difference.amount).toBe('45.00');

    // Confirma que a divergência NÃO foi sobrescrita silenciosamente e que a métrica foi incrementada
    const getWalletRes = await fetch(`${baseUrl}/wallets/${wallet.id}`);
    const getWalletBody = (await getWalletRes.json()) as { balance: { amount: string } };
    expect(getWalletBody.balance.amount).toBe('145.00');

    const metricsRes = await fetch(`${baseUrl}/health/metrics`);
    const metricsBody = (await metricsRes.json()) as {
      reconciliationDivergencesCount: number;
    };
    expect(metricsBody.reconciliationDivergencesCount).toBeGreaterThanOrEqual(1);
  });
});
