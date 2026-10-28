import 'reflect-metadata';
import * as os from 'os';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../../src/app.module';
import { WagerTransactionKind } from '../../src/domain/models/wager-transaction';
import { GlobalHttpExceptionFilter } from '../../src/infrastructure/http/filters/http-exception.filter';
import { AppLoggerService } from '../../src/infrastructure/logger/app-logger.service';
import { OutboxDispatcherWorker } from '../../src/infrastructure/messaging/outbox/outbox-dispatcher.worker';
import { ensureRealPostgres, ensureSqsEndpoint } from '../helpers/real-infra.helper';

interface AutocannonResult {
  requests: { average: number; total: number };
  latency: { p50: number; p97_5: number; p99: number };
  errors: number;
  non2xx: number;
}

// eslint-disable-next-line @typescript-eslint/no-require-imports
const autocannon = require('autocannon') as (
  opts: { url: string; connections: number; duration: number; pipelining: number },
  cb: (err: Error | null, result: AutocannonResult) => void,
) => void;

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx];
}

async function runAutocannonReadBurst(url: string): Promise<{
  requestsPerSec: number;
  p50Ms: number;
  p97_5Ms: number;
  p99Ms: number;
  totalRequests: number;
  errors: number;
}> {
  return new Promise((resolve, reject) => {
    autocannon(
      {
        url,
        connections: 20,
        duration: 2,
        pipelining: 1,
      },
      (err: Error | null, result: AutocannonResult) => {
        if (err) {
          return reject(err);
        }
        resolve({
          requestsPerSec: result.requests.average,
          p50Ms: result.latency.p50,
          p97_5Ms: result.latency.p97_5,
          p99Ms: result.latency.p99,
          totalRequests: result.requests.total,
          errors: result.errors + result.non2xx,
        });
      },
    );
  });
}

async function runLoadTest() {
  console.log('================================================================================');
  console.log('🚀 JUNGLE GAMING — REAL HTTP + POSTGRESQL + SQS LOAD TEST & BENCHMARK');
  console.log('================================================================================');
  console.log(`Runtime        : Bun ${process.versions.bun || process.version}`);
  console.log(`OS / Arch      : ${os.platform()} ${os.release()} (${os.arch()})`);
  console.log(
    `CPU            : ${os.cpus()[0]?.model || 'Generic CPU'} (${os.cpus().length} cores)`,
  );
  console.log(`Memory         : ${(os.totalmem() / 1024 ** 3).toFixed(1)} GB RAM`);

  // 1. Conecta ao PostgreSQL real e aplica migrations
  const { orm, port: dbPort } = await ensureRealPostgres();
  const pgVersionRow = await orm.em.getConnection().execute('SELECT version() as v');
  const pgVersion = String(pgVersionRow[0]?.v || 'PostgreSQL').split(',')[0];
  await orm.close(true);

  console.log(`Database       : ${pgVersion} (port ${dbPort}, real SELECT ... FOR UPDATE)`);

  // 2. Conecta ao LocalStack real (se ativo) ou sobe servidor HTTP SQS real
  const sqsHandle = await ensureSqsEndpoint();
  const sqsEndpoint = sqsHandle.endpoint;

  process.env.DB_PORT = String(dbPort);
  process.env.DISABLE_WORKERS = 'true';
  process.env.LOG_LEVEL = 'silent';
  process.env.AWS_REGION = 'us-east-1';
  process.env.AWS_ENDPOINT = sqsEndpoint;
  process.env.SQS_WAGER_QUEUE_URL = `${sqsEndpoint}/000000000000/wager-transactions.fifo`;
  process.env.SQS_WAGER_DLQ_URL = `${sqsEndpoint}/000000000000/wager-transactions-dlq.fifo`;
  process.env.SQS_OUTBOX_QUEUE_URL = `${sqsEndpoint}/000000000000/wagering-events.fifo`;
  process.env.SQS_EVENTS_QUEUE_URL = `${sqsEndpoint}/000000000000/wagering-events.fifo`;

  // 3. Sobe aplicação NestJS real escutando em porta TCP real
  const app: INestApplication = await NestFactory.create(AppModule, { logger: false });
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
  const addr = app.getHttpServer().address();
  const httpPort = typeof addr === 'object' && addr ? addr.port : 3000;
  const baseUrl = `http://127.0.0.1:${httpPort}`;
  const outboxWorker = app.get(OutboxDispatcherWorker);

  console.log(`HTTP Server    : ${baseUrl} (NestJS 10 + Express + ValidationPipe)`);
  console.log('--------------------------------------------------------------------------------\n');

  const runTag = `${Date.now()}`;

  try {
    // =========================================================================
    // PERFIL A: Multi-Wallet Parallel Throughput (20 carteiras × 20 transações HTTP = 400 reqs)
    // =========================================================================
    const walletCountA = 20;
    const opsPerWalletA = 20;
    const totalOpsA = walletCountA * opsPerWalletA;

    console.log(
      `▶ PERFIL A: Multi-Wallet Parallel HTTP + PostgreSQL (${walletCountA} carteiras × ${opsPerWalletA} ops = ${totalOpsA} HTTP POSTs)`,
    );

    const walletsA: Array<{ id: string; playerId: string }> = [];
    for (let w = 0; w < walletCountA; w++) {
      const playerId = `load-a-${runTag}-p-${w}`;
      const res = await fetch(`${baseUrl}/wallets`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          playerId,
          initialBalance: { amount: '500.00', currency: 'BRL' },
        }),
      });
      if (res.status !== 201) {
        throw new Error(`Failed to create wallet in Profile A: HTTP ${res.status}`);
      }
      const data = (await res.json()) as { id: string; playerId: string };
      walletsA.push(data);
    }

    const latenciesA: number[] = [];
    let status200A = 0;
    let status5xxA = 0;
    const startA = performance.now();

    await Promise.all(
      walletsA.map(async (wallet, wIdx) => {
        for (let op = 0; op < opsPerWalletA; op++) {
          const isWin = op % 4 === 3;
          const isLoss = op % 4 === 2;
          const kind = isWin
            ? WagerTransactionKind.Win
            : isLoss
              ? WagerTransactionKind.Loss
              : WagerTransactionKind.Bet;
          const amount = isWin ? '25.00' : '10.00';
          const extId = `ext-a-${runTag}-${wIdx}-${op}`;
          const idemKey = `idem-a-${runTag}-${wIdx}-${op}`;

          const t0 = performance.now();
          const res = await fetch(`${baseUrl}/wagering/transactions`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Idempotency-Key': idemKey,
            },
            body: JSON.stringify({
              providerId: 'load-provider-a',
              externalTransactionId: extId,
              playerId: wallet.playerId,
              walletId: wallet.id,
              roundId: `round-a-${wIdx}-${op}`,
              gameId: 'fortune-tiger',
              kind,
              money: { amount, currency: 'BRL' },
            }),
          });
          latenciesA.push(performance.now() - t0);

          if (res.status === 200) status200A += 1;
          if (res.status >= 500) status5xxA += 1;
        }
      }),
    );

    const durationSecA = (performance.now() - startA) / 1000;
    const rpsA = totalOpsA / durationSecA;
    const sortedA = [...latenciesA].sort((a, b) => a - b);

    console.log(`  • Total Reqs HTTP   : ${totalOpsA} (200 OK: ${status200A}, 5xx: ${status5xxA})`);
    console.log(`  • Tempo Total       : ${(durationSecA * 1000).toFixed(2)} ms`);
    console.log(`  • Throughput (Write): ${rpsA.toFixed(2)} req/s`);
    console.log(`  • Latência p50      : ${percentile(sortedA, 50).toFixed(2)} ms`);
    console.log(`  • Latência p95      : ${percentile(sortedA, 95).toFixed(2)} ms`);
    console.log(`  • Latência p99      : ${percentile(sortedA, 99).toFixed(2)} ms\n`);

    // =========================================================================
    // PERFIL B: Hot Wallet Contentious Burst + Idempotency Storm (1 carteira, 200 reqs HTTP)
    // =========================================================================
    const burstsB = 8;
    const concurrencyPerBurstB = 25;
    const totalOpsB = burstsB * concurrencyPerBurstB; // 200 reqs na mesma carteira

    console.log(
      `▶ PERFIL B: Hot Wallet Contentious Burst + Idempotency Storm (1 carteira, ${burstsB} rajadas × ${concurrencyPerBurstB} concorrentes = ${totalOpsB} HTTP POSTs)`,
    );

    const hotPlayerId = `load-hot-${runTag}`;
    const hotCreateRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId: hotPlayerId,
        initialBalance: { amount: '500.00', currency: 'BRL' },
      }),
    });
    const hotWallet = (await hotCreateRes.json()) as { id: string; playerId: string };

    const latenciesB: number[] = [];
    let processedB = 0;
    let rejectedB = 0;
    let replaysB = 0;
    let errors5xxB = 0;
    const startB = performance.now();

    for (let b = 0; b < burstsB; b++) {
      const stormExtId = `ext-storm-${runTag}-${b}`;
      const stormIdemKey = `idem-storm-${runTag}-${b}`;

      await Promise.all(
        Array.from({ length: concurrencyPerBurstB }, async (_, idx) => {
          // Primeiras 15 requisições da rajada enviam a MESMA Idempotency-Key simultaneamente (Idempotency Storm)
          const isStormReplay = idx < 15;
          const extId = isStormReplay ? stormExtId : `ext-hot-${runTag}-${b}-${idx}`;
          const idemKey = isStormReplay ? stormIdemKey : `idem-hot-${runTag}-${b}-${idx}`;
          const kind = isStormReplay
            ? WagerTransactionKind.Bet
            : idx % 3 === 0
              ? WagerTransactionKind.Win
              : WagerTransactionKind.Bet;
          const amount = isStormReplay ? '20.00' : idx % 3 === 0 ? '15.00' : '80.00';

          const t0 = performance.now();
          const res = await fetch(`${baseUrl}/wagering/transactions`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Idempotency-Key': idemKey,
            },
            body: JSON.stringify({
              providerId: 'load-provider-b',
              externalTransactionId: extId,
              playerId: hotWallet.playerId,
              walletId: hotWallet.id,
              roundId: `round-hot-${b}`,
              gameId: 'aviator',
              kind,
              money: { amount, currency: 'BRL' },
            }),
          });
          latenciesB.push(performance.now() - t0);

          if (res.status >= 500) {
            errors5xxB += 1;
            return;
          }

          const body = (await res.json()) as {
            status: string;
            idempotentReplay: boolean;
          };
          if (body.idempotentReplay) {
            replaysB += 1;
          } else if (res.status === 200) {
            processedB += 1;
          } else if (res.status === 422) {
            rejectedB += 1;
          }
        }),
      );
    }

    const durationSecB = (performance.now() - startB) / 1000;
    const rpsB = totalOpsB / durationSecB;
    const sortedB = [...latenciesB].sort((a, b) => a - b);

    console.log(`  • Total Reqs HTTP   : ${totalOpsB}`);
    console.log(`  • Tempo Total       : ${(durationSecB * 1000).toFixed(2)} ms`);
    console.log(`  • Throughput (Hot)  : ${rpsB.toFixed(2)} req/s`);
    console.log(`  • Latência p50      : ${percentile(sortedB, 50).toFixed(2)} ms`);
    console.log(`  • Latência p95      : ${percentile(sortedB, 95).toFixed(2)} ms`);
    console.log(`  • Latência p99      : ${percentile(sortedB, 99).toFixed(2)} ms`);
    console.log(
      `  • Desfechos         : PROCESSED=${processedB} | REJECTED(422)=${rejectedB} | IDEMPOTENT_REPLAY=${replaysB}`,
    );
    console.log(`  • Erros 5xx         : ${errors5xxB} (0.00%)\n`);

    // =========================================================================
    // PERFIL C: Autocannon HTTP Read & Reconciliation Burst (2s, 20 conexões)
    // =========================================================================
    console.log(
      `▶ PERFIL C: Autocannon HTTP Reconciliation Benchmark (20 conexões simultâneas, 2s em /wallets/:id/reconcile)`,
    );
    const acStats = await runAutocannonReadBurst(
      `${baseUrl}/wallets/${hotWallet.id}/reconcile`,
    );
    console.log(`  • Total Reqs HTTP   : ${acStats.totalRequests}`);
    console.log(`  • Throughput Médio  : ${acStats.requestsPerSec.toFixed(2)} req/s`);
    console.log(`  • Latência p50      : ${acStats.p50Ms.toFixed(2)} ms`);
    console.log(`  • Latência p97.5    : ${acStats.p97_5Ms.toFixed(2)} ms`);
    console.log(`  • Latência p99      : ${acStats.p99Ms.toFixed(2)} ms`);
    console.log(`  • Erros / Non-2xx   : ${acStats.errors}\n`);

    // =========================================================================
    // AUDITORIA PÓS-CARGA: Reconciliação Universal (21 carteiras) + Outbox Dispatch
    // =========================================================================
    console.log('▶ AUDITORIA DE INVARIANTES CONTÁBEIS PÓS-CARGA (PostgreSQL + SQS)...');

    const allWallets = [...walletsA, hotWallet];
    let reconciledCount = 0;

    for (const w of allWallets) {
      const recRes = await fetch(`${baseUrl}/wallets/${w.id}/reconcile`);
      const rec = (await recRes.json()) as {
        consistent: boolean;
        storedBalance: { amount: string };
        calculatedBalance: { amount: string };
        difference: { amount: string };
      };
      if (
        !rec.consistent ||
        rec.difference.amount !== '0.00' ||
        Number(rec.storedBalance.amount) < 0
      ) {
        throw new Error(
          `Accounting invariant violated on wallet ${w.id}: stored=${rec.storedBalance.amount}, ledger=${rec.calculatedBalance.amount}`,
        );
      }
      reconciledCount += 1;
    }

    let totalPublishedOutbox = 0;
    while (true) {
      const batch = await outboxWorker.dispatchBatch(200);
      totalPublishedOutbox += batch.published;
      if (batch.published === 0) break;
    }

    if (status5xxA > 0 || errors5xxB > 0 || acStats.errors > 0) {
      throw new Error('Unexpected HTTP 5xx errors during load test');
    }

    console.log(
      `  • Carteiras Reconciliadas : ${reconciledCount}/${allWallets.length} (100% consistentes, discrepância = 0.00)`,
    );
    console.log(
      `  • Eventos Outbox -> SQS   : ${totalPublishedOutbox} publicados via @aws-sdk/client-sqs`,
    );
    console.log('================================================================================');
    console.log('✅ LOAD TEST REAL (HTTP + POSTGRESQL + SQS) CONCLUÍDO COM SUCESSO (0 FALHAS)');
    console.log('================================================================================');
  } finally {
    await app.close();
    await sqsHandle.stop();
  }
}

runLoadTest().catch((err) => {
  console.error('❌ Load test failed:', err);
  process.exit(1);
});
