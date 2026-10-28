import * as os from 'os';
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
import { EventPublisherPort } from '../../src/infrastructure/messaging/sqs/sqs-event-publisher';
import { MetricsService } from '../../src/infrastructure/metrics/metrics.service';

/**
 * Gerenciador de Locks Pessimistas por linha (simulando SELECT ... FOR UPDATE no PostgreSQL)
 * com contabilização de contenção de lock (quantas vezes uma transação precisou aguardar fila de lock).
 */
class LoadTestLockManager {
  private locks = new Map<string, Promise<void>>();
  public lockContentionCount = 0;

  async acquire(walletId: string): Promise<() => void> {
    if (this.locks.has(walletId)) {
      this.lockContentionCount += 1;
    }
    while (this.locks.has(walletId)) {
      await this.locks.get(walletId);
    }

    let release!: () => void;
    const p = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.locks.set(walletId, p);

    return () => {
      this.locks.delete(walletId);
      release();
    };
  }
}

class LoadTestEngine {
  public readonly lockManager = new LoadTestLockManager();
  public readonly wallets = new Map<string, Wallet>();
  public readonly transactions = new Map<string, WagerTransaction>();
  public readonly txByIdempotency = new Map<string, WagerTransaction>();
  public readonly txByExt = new Map<string, WagerTransaction>();
  public readonly ledgerByWallet = new Map<string, WalletLedgerEntry[]>();
  public readonly outboxMessages = new Map<string, OutboxMessage>();
  public readonly inboxMessages = new Map<string, InboxMessage>();
  public readonly metrics = new MetricsService();

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

  private activeReleases: Array<() => void> = [];
  public publishedEventsCount = 0;
  private readonly simulatedDbIoMs = Number(process.env.LOAD_TEST_IO_MS ?? 1);

  constructor() {
    this.walletRepo = {
      findById: async (id: string, lock?: 'pessimistic_write') => {
        if (lock === 'pessimistic_write') {
          const release = await this.lockManager.acquire(id);
          this.activeReleases.push(release);
          if (this.simulatedDbIoMs > 0) {
            await new Promise((r) => setTimeout(r, this.simulatedDbIoMs));
          }
        }
        return this.wallets.get(id) || null;
      },
      findByPlayerIdAndCurrency: async (playerId: string, currency: string) => {
        for (const w of this.wallets.values()) {
          if (w.playerId === playerId && w.currency === currency) return w;
        }
        return null;
      },
      save: async (w: Wallet) => {
        this.wallets.set(w.id, w);
      },
    };

    this.txRepo = {
      findById: async (id: string) => this.transactions.get(id) || null,
      findByIdempotencyKey: async (key: string) => this.txByIdempotency.get(key) || null,
      findByProviderAndExternalId: async (p: string, ext: string) =>
        this.txByExt.get(`${p}:${ext}`) || null,
      findProcessedReversal: async (refId: string) => {
        for (const tx of this.transactions.values()) {
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
      findPendingReferences: async (limit: number) =>
        Array.from(this.transactions.values())
          .filter((t) => t.status === WagerTransactionStatus.PendingReference)
          .slice(0, limit),
      save: async (tx: WagerTransaction) => {
        this.transactions.set(tx.id, tx);
        this.txByIdempotency.set(tx.idempotencyKey, tx);
        this.txByExt.set(`${tx.providerId}:${tx.externalTransactionId}`, tx);
      },
    };

    this.ledgerRepo = {
      save: async (entry: WalletLedgerEntry) => {
        const list = this.ledgerByWallet.get(entry.walletId) || [];
        list.push(entry);
        this.ledgerByWallet.set(entry.walletId, list);
      },
      findByWalletId: async (
        walletId: string,
        options?: { limit: number; cursor?: string },
      ): Promise<LedgerPaginationResult> => {
        const list = this.ledgerByWallet.get(walletId) || [];
        return { entries: list.slice(0, options?.limit ?? 50) };
      },
      calculateBalance: async (walletId: string) => {
        const list = this.ledgerByWallet.get(walletId) || [];
        let total = Money.zero('BRL');
        for (const e of list) {
          total =
            e.direction === LedgerDirection.Credit
              ? total.add(e.money)
              : total.subtract(e.money);
        }
        return { calculatedAmount: total.amount, count: list.length };
      },
    };

    this.outboxRepo = {
      save: async (msg: OutboxMessage) => {
        this.outboxMessages.set(msg.id, msg);
      },
      findPendingToDispatch: async (limit: number, now: Date) =>
        Array.from(this.outboxMessages.values())
          .filter((m) => m.isDue(now))
          .slice(0, limit),
    };

    this.inboxRepo = {
      find: async (c: string, m: string) => this.inboxMessages.get(`${c}:${m}`) || null,
      save: async (i: InboxMessage) => {
        this.inboxMessages.set(`${i.consumerName}:${i.messageId}`, i);
      },
    };

    this.uow = {
      transactional: async <T>(fn: () => Promise<T>): Promise<T> => {
        try {
          return await fn();
        } finally {
          while (this.activeReleases.length > 0) {
            const rel = this.activeReleases.pop();
            if (rel) rel();
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

    const publisher: EventPublisherPort = {
      publish: async () => {
        this.publishedEventsCount += 1;
      },
    };

    const silentLogger = {
      log: () => {},
      warn: () => {},
      error: () => {},
      debug: () => {},
    } as unknown as AppLoggerService;

    this.outboxWorker = new OutboxDispatcherWorker(
      this.outboxRepo,
      this.uow,
      publisher,
      silentLogger,
      this.metrics,
    );
  }
}

interface BenchmarkStats {
  profileName: string;
  totalRequests: number;
  durationMs: number;
  rps: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  minMs: number;
  maxMs: number;
  avgMs: number;
  processedCount: number;
  rejectedCount: number;
  replaysCount: number;
  unexpectedErrors: number;
  lockContentions: number;
}

function computePercentiles(latencies: number[]) {
  const sorted = [...latencies].sort((a, b) => a - b);
  const len = sorted.length;
  if (len === 0) {
    return { p50: 0, p95: 0, p99: 0, min: 0, max: 0, avg: 0 };
  }
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    p50: Number(sorted[Math.floor(len * 0.5)].toFixed(3)),
    p95: Number(sorted[Math.floor(len * 0.95)].toFixed(3)),
    p99: Number(sorted[Math.floor(len * 0.99)].toFixed(3)),
    min: Number(sorted[0].toFixed(3)),
    max: Number(sorted[len - 1].toFixed(3)),
    avg: Number((sum / len).toFixed(3)),
  };
}

async function runProfileA_MultiWallet(engine: LoadTestEngine): Promise<BenchmarkStats> {
  const walletCount = 50;
  const opsPerWallet = 30; // 1.500 transações concorrentes distribuídas em 50 carteiras
  const wallets: Array<{ id: string; playerId: string }> = [];

  for (let i = 1; i <= walletCount; i++) {
    const w = await engine.createWalletUseCase.execute({
      playerId: `load-player-multi-${i}`,
      initialBalance: { amount: '500.00', currency: 'BRL' },
    });
    wallets.push({ id: w.id, playerId: w.playerId });
  }

  const latencies: number[] = [];
  let processedCount = 0;
  let rejectedCount = 0;
  let replaysCount = 0;
  let unexpectedErrors = 0;
  const contentionsBefore = engine.lockManager.lockContentionCount;

  const startTime = performance.now();

  // Executa 30 rodadas por carteira com paralelismo entre as 50 carteiras
  await Promise.all(
    wallets.map(async (wallet, wIdx) => {
      for (let op = 1; op <= opsPerWallet; op++) {
        const opStart = performance.now();
        try {
          const isWin = op % 3 === 0;
          const isLoss = op % 5 === 0 && !isWin;
          const kind = isWin
            ? WagerTransactionKind.Win
            : isLoss
              ? WagerTransactionKind.Loss
              : WagerTransactionKind.Bet;
          const amount = isLoss ? '0.00' : isWin ? '40.00' : '25.00';

          const res = await engine.processWagerUseCase.execute({
            providerId: 'load-provider-a',
            externalTransactionId: `mw-${wIdx}-op-${op}`,
            idempotencyKey: `load-provider-a:mw-${wIdx}-op-${op}`,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: `round-${wIdx}-${op}`,
            gameId: 'fortune-chimp',
            kind,
            money: { amount, currency: 'BRL' },
          });

          if (res.idempotentReplay) replaysCount++;
          else if (res.status === WagerTransactionStatus.Processed) processedCount++;
          else if (res.status === WagerTransactionStatus.Rejected) rejectedCount++;
        } catch {
          unexpectedErrors++;
        } finally {
          latencies.push(performance.now() - opStart);
        }
      }
    }),
  );

  const durationMs = performance.now() - startTime;
  const totalRequests = walletCount * opsPerWallet;
  const pct = computePercentiles(latencies);

  return {
    profileName: 'Perfil A: Multi-Wallet (50 carteiras paralelas, baixa contenção)',
    totalRequests,
    durationMs: Number(durationMs.toFixed(2)),
    rps: Number(((totalRequests / durationMs) * 1000).toFixed(2)),
    p50Ms: pct.p50,
    p95Ms: pct.p95,
    p99Ms: pct.p99,
    minMs: pct.min,
    maxMs: pct.max,
    avgMs: pct.avg,
    processedCount,
    rejectedCount,
    replaysCount,
    unexpectedErrors,
    lockContentions: engine.lockManager.lockContentionCount - contentionsBefore,
  };
}

async function runProfileB_HotWallet(engine: LoadTestEngine): Promise<BenchmarkStats> {
  const hotWallet = await engine.createWalletUseCase.execute({
    playerId: 'load-player-hot-wallet',
    initialBalance: { amount: '1000.00', currency: 'BRL' },
  });

  const totalRequests = 500;
  const latencies: number[] = [];
  let processedCount = 0;
  let rejectedCount = 0;
  let replaysCount = 0;
  let unexpectedErrors = 0;
  const contentionsBefore = engine.lockManager.lockContentionCount;

  const startTime = performance.now();

  // Dispara 500 requisições concorrentes sobre A MESMA carteira (100 únicas + 400 replays em rajadas de 50)
  const batchSize = 50;
  for (let b = 0; b < totalRequests / batchSize; b++) {
    await Promise.all(
      Array.from({ length: batchSize }, async (_, idx) => {
        const opStart = performance.now();
        // Primeiros 10 do lote são transações únicas; os outros 40 são replays das primeiras 10
        const uniqueSlot = idx % 10;
        const txId = `hot-tx-batch-${b}-slot-${uniqueSlot}`;
        try {
          const res = await engine.processWagerUseCase.execute({
            providerId: 'load-provider-hot',
            externalTransactionId: txId,
            idempotencyKey: `load-provider-hot:${txId}`,
            playerId: 'load-player-hot-wallet',
            walletId: hotWallet.id,
            roundId: `hot-round-${b}`,
            gameId: 'aviator-hot',
            kind: WagerTransactionKind.Bet,
            money: { amount: '15.00', currency: 'BRL' },
          });

          if (res.idempotentReplay) replaysCount++;
          else if (res.status === WagerTransactionStatus.Processed) processedCount++;
          else if (res.status === WagerTransactionStatus.Rejected) rejectedCount++;
        } catch {
          unexpectedErrors++;
        } finally {
          latencies.push(performance.now() - opStart);
        }
      }),
    );
  }

  const durationMs = performance.now() - startTime;
  const pct = computePercentiles(latencies);

  return {
    profileName: 'Perfil B: Hot Wallet + Tempestade de Idempotência (Alta Contenção em 1 Wallet)',
    totalRequests,
    durationMs: Number(durationMs.toFixed(2)),
    rps: Number(((totalRequests / durationMs) * 1000).toFixed(2)),
    p50Ms: pct.p50,
    p95Ms: pct.p95,
    p99Ms: pct.p99,
    minMs: pct.min,
    maxMs: pct.max,
    avgMs: pct.avg,
    processedCount,
    rejectedCount,
    replaysCount,
    unexpectedErrors,
    lockContentions: engine.lockManager.lockContentionCount - contentionsBefore,
  };
}

async function main() {
  console.log('================================================================================');
  console.log(' 🚀 JUNGLE GAMING — DISTRIBUTED WAGERING PROCESSOR LOAD & STRESS TEST');
  console.log('================================================================================');
  console.log(`Ambiente:`);
  console.log(`  • OS / Arch      : ${os.platform()} ${os.release()} (${os.arch()})`);
  console.log(`  • CPU            : ${os.cpus()[0]?.model || 'Unknown'} (${os.cpus().length} logical cores)`);
  console.log(`  • RAM Total      : ${(os.totalmem() / 1024 / 1024 / 1024).toFixed(2)} GB`);
  console.log(`  • Runtime        : Bun ${Bun.version}`);
  console.log('--------------------------------------------------------------------------------\n');

  const engine = new LoadTestEngine();

  // 1. Executa Perfil A (Multi-Wallet)
  const statsA = await runProfileA_MultiWallet(engine);

  // 2. Executa Perfil B (Hot Wallet & Idempotency Storm)
  const statsB = await runProfileB_HotWallet(engine);

  // 3. Drena a Transactional Outbox medindo Outbox Lag e Throughput de Publicação
  const outboxDrainStart = performance.now();
  let totalOutboxPublished = 0;
  while (true) {
    const res = await engine.outboxWorker.dispatchBatch(500);
    totalOutboxPublished += res.published;
    if (res.published === 0) break;
  }
  const outboxDrainDurationMs = performance.now() - outboxDrainStart;
  const metricsSnap = engine.metrics.getSnapshot();

  // 4. Auditoria Universal: Reconcilia TODAS as 51 carteiras (wallet.balance == sum(ledger))
  let consistentWallets = 0;
  let totalLedgerEntriesChecked = 0;
  for (const w of engine.wallets.values()) {
    const report = await engine.reconcileWalletUseCase.execute(w.id);
    if (!report.consistent || report.difference.amount !== '0.00') {
      throw new Error(
        `CRITICAL INVARIANT VIOLATION on wallet ${w.id}: stored=${report.storedBalance.amount}, ledger=${report.calculatedBalance.amount}`,
      );
    }
    consistentWallets += 1;
    totalLedgerEntriesChecked += report.checkedEntries;
  }

  for (const s of [statsA, statsB]) {
    console.log(`📊 ${s.profileName}`);
    console.log(`   • Requisições Totais   : ${s.totalRequests}`);
    console.log(`   • Tempo Total          : ${s.durationMs} ms`);
    console.log(`   • Throughput           : ${s.rps} req/s`);
    console.log(`   • Latência (ms)        : p50=${s.p50Ms}ms | p95=${s.p95Ms}ms | p99=${s.p99Ms}ms | avg=${s.avgMs}ms | max=${s.maxMs}ms`);
    console.log(`   • Resultados           : PROCESSED=${s.processedCount} | REJECTED=${s.rejectedCount} | REPLAYS=${s.replaysCount}`);
    console.log(`   • Conflitos de Lock    : ${s.lockContentions} aquisições enfileiradas em lock pessimista`);
    console.log(`   • Taxa de Erro (5xx)   : ${((s.unexpectedErrors / s.totalRequests) * 100).toFixed(2)}%\n`);
  }

  console.log('📦 Transactional Outbox & Consistência Contábil:');
  console.log(`   • Eventos Publicados   : ${totalOutboxPublished} eventos em ${outboxDrainDurationMs.toFixed(2)} ms`);
  console.log(`   • Outbox Lag Máximo    : ${metricsSnap.outboxLagMs} ms`);
  console.log(`   • Carteiras Auditadas  : ${consistentWallets}/${engine.wallets.size} (100% consistentes)`);
  console.log(`   • Entradas no Ledger   : ${totalLedgerEntriesChecked} lançamentos verificados`);
  console.log(`   • Invariante Final     : wallet.balance == sum(ledger) VERIFICADA COM SUCESSO ✅`);
  console.log('================================================================================');
}

main().catch((err) => {
  console.error('Load test failed:', err);
  process.exit(1);
});
