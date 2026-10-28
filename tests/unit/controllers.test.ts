import { beforeEach, describe, expect, it } from 'bun:test';
import {
  ArgumentsHost,
  BadRequestException,
  ExecutionContext,
  HttpStatus,
  NotFoundException,
} from '@nestjs/common';
import type { Response } from 'express';
import {
  CreateWalletUseCase,
  WalletConflictError,
} from '../../src/application/use-cases/create-wallet.use-case';
import { GetWalletLedgerUseCase } from '../../src/application/use-cases/get-wallet-ledger.use-case';
import {
  ProcessWagerTransactionUseCase,
  WalletNotFoundError,
} from '../../src/application/use-cases/process-wager-transaction.use-case';
import { ReconcileWalletUseCase } from '../../src/application/use-cases/reconcile-wallet.use-case';
import {
  CurrencyMismatchError,
  FailureCode,
  IdempotencyConflictError,
  InsufficientBalanceError,
  InvalidLedgerEntryError,
  InvalidMoneyError,
  InvalidTransactionReferenceError,
} from '../../src/domain/errors/domain.error';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/models/wager-transaction';
import { Wallet } from '../../src/domain/models/wallet';
import { WalletLedgerEntry } from '../../src/domain/models/wallet-ledger-entry';
import { HealthController } from '../../src/infrastructure/http/controllers/health.controller';
import { WageringController } from '../../src/infrastructure/http/controllers/wagering.controller';
import { WalletsController } from '../../src/infrastructure/http/controllers/wallets.controller';
import { GlobalHttpExceptionFilter } from '../../src/infrastructure/http/filters/http-exception.filter';
import {
  AuthGuard,
  NoopProviderIdentityPort,
} from '../../src/infrastructure/http/guards/auth.guard';
import {
  CorrelationIdMiddleware,
  RequestWithCorrelation,
} from '../../src/infrastructure/http/middlewares/correlation-id.middleware';
import { AppLoggerService } from '../../src/infrastructure/logger/app-logger.service';
import { MetricsService } from '../../src/infrastructure/metrics/metrics.service';

// Mock Response object for Express
function createMockResponse(): Response {
  const res: Partial<Response> = {};
  res.statusCode = 200;
  res.status = (code: number) => {
    res.statusCode = code;
    return res as Response;
  };
  res.json = (data: unknown) => {
    (res as Record<string, unknown>).data = data;
    return res as Response;
  };
  res.setHeader = () => res as Response;
  return res as Response;
}

describe('HTTP Controllers, Filters, Guards & Middlewares Unit Tests', () => {
  let mockWalletRepo: any;
  let mockTxRepo: any;
  let mockLedgerRepo: any;
  let mockOutboxRepo: any;
  let mockInboxRepo: any;
  let mockUow: any;
  let metricsService: MetricsService;

  let createWalletUseCase: CreateWalletUseCase;
  let getWalletLedgerUseCase: GetWalletLedgerUseCase;
  let reconcileWalletUseCase: ReconcileWalletUseCase;
  let processWagerUseCase: ProcessWagerTransactionUseCase;

  let walletsController: WalletsController;
  let wageringController: WageringController;
  let healthController: HealthController;

  beforeEach(() => {
    const wallets = new Map<string, Wallet>();
    const transactions = new Map<string, WagerTransaction>();
    const ledgerEntries: WalletLedgerEntry[] = [];

    mockWalletRepo = {
      findById: async (id: string) => wallets.get(id) || null,
      findByPlayerIdAndCurrency: async () => null,
      save: async (w: Wallet) => wallets.set(w.id, w),
    };

    mockTxRepo = {
      findById: async (id: string) => transactions.get(id) || null,
      findByIdempotencyKey: async (key: string) => {
        for (const t of transactions.values()) {
          if (t.idempotencyKey === key) return t;
        }
        return null;
      },
      findByProviderAndExternalId: async (pId: string, extId: string) => {
        for (const t of transactions.values()) {
          if (t.providerId === pId && t.externalTransactionId === extId) return t;
        }
        return null;
      },
      findProcessedReversal: async () => null,
      save: async (t: WagerTransaction) => transactions.set(t.id, t),
    };

    mockLedgerRepo = {
      save: async (e: WalletLedgerEntry) => {
        ledgerEntries.push(e);
      },
      findByWalletId: async (wId: string, opts?: { limit: number; cursor?: string }) => {
        const filtered = ledgerEntries.filter((e) => e.walletId === wId);
        const limit = opts?.limit ?? 50;
        const slice = filtered.slice(0, limit);
        const nextCursor = filtered.length > limit ? 'cursor-token-next' : undefined;
        return { entries: slice, nextCursor };
      },
      calculateBalance: async () => ({ calculatedAmount: '100.00', count: 1 }),
    };

    mockOutboxRepo = { save: async () => {} };
    mockInboxRepo = { find: async () => null, save: async () => {} };
    mockUow = { transactional: async (fn: any) => fn() };
    metricsService = new MetricsService();

    createWalletUseCase = new CreateWalletUseCase(
      mockWalletRepo,
      mockTxRepo,
      mockLedgerRepo,
      mockOutboxRepo,
      mockUow,
    );
    getWalletLedgerUseCase = new GetWalletLedgerUseCase(mockWalletRepo, mockLedgerRepo);
    reconcileWalletUseCase = new ReconcileWalletUseCase(mockWalletRepo, mockLedgerRepo);
    processWagerUseCase = new ProcessWagerTransactionUseCase(
      mockWalletRepo,
      mockTxRepo,
      mockLedgerRepo,
      mockOutboxRepo,
      mockInboxRepo,
      mockUow,
    );

    walletsController = new WalletsController(
      createWalletUseCase,
      getWalletLedgerUseCase,
      reconcileWalletUseCase,
      mockWalletRepo,
    );
    wageringController = new WageringController(
      processWagerUseCase,
      mockTxRepo,
      metricsService,
    );
    healthController = new HealthController(
      {
        getConnection: () => ({
          execute: async () => [{ '?column?': 1 }],
        }),
      } as any,
      metricsService,
    );
  });

  describe('WalletsController', () => {
    it('POST /wallets - should create wallet and return 201 response', async () => {
      const response = await walletsController.createWallet({
        playerId: 'p-1',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      expect(response.playerId).toBe('p-1');
      expect(response.balance.amount).toBe('100.00');
    });

    it('GET /wallets/:walletId - should return wallet or throw WalletNotFoundError', async () => {
      const created = await walletsController.createWallet({
        playerId: 'p-2',
        initialBalance: { amount: '50.00', currency: 'BRL' },
      });

      const found = await walletsController.getWallet(created.id);
      expect(found.id).toBe(created.id);
      expect(found.balance.amount).toBe('50.00');

      expect(walletsController.getWallet('non-existing')).rejects.toThrow(
        WalletNotFoundError,
      );
    });

    it('GET /wallets/:walletId/ledger - should return paginated ledger entries with cursor', async () => {
      const created = await walletsController.createWallet({
        playerId: 'p-ledger',
        initialBalance: { amount: '200.00', currency: 'BRL' },
      });

      const mockRes = createMockResponse();
      await wageringController.submitTransaction(
        'prov:tx-ledger-1',
        {
          providerId: 'prov',
          externalTransactionId: 'tx-ledger-1',
          playerId: 'p-ledger',
          walletId: created.id,
          roundId: 'r-1',
          gameId: 'g-1',
          kind: WagerTransactionKind.Bet,
          money: { amount: '50.00', currency: 'BRL' },
        },
        mockRes,
      );

      const page1 = await walletsController.getWalletLedger(created.id, undefined, '1');
      expect(page1.entries.length).toBe(1);
      expect(page1.nextCursor).toBe('cursor-token-next');

      expect(walletsController.getWalletLedger('unknown-wallet')).rejects.toThrow(
        WalletNotFoundError,
      );
    });

    it('POST /wallets/:walletId/reconciliation - should execute wallet reconciliation', async () => {
      const created = await walletsController.createWallet({
        playerId: 'p-3',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      const report = await walletsController.reconcileWallet(created.id);
      expect(report.consistent).toBe(true);
      expect(report.walletId).toBe(created.id);
    });
  });

  describe('WageringController', () => {
    it('POST /wagering/transactions - should throw BadRequestException if Idempotency-Key header is missing', async () => {
      const mockRes = createMockResponse();

      expect(
        wageringController.submitTransaction(
          undefined,
          {
            providerId: 'prov-a',
            externalTransactionId: 'tx-1',
            playerId: 'p-1',
            walletId: 'w-1',
            roundId: 'r-1',
            gameId: 'g-1',
            kind: WagerTransactionKind.Bet,
            money: { amount: '10.00', currency: 'BRL' },
          },
          mockRes,
        ),
      ).rejects.toThrow(BadRequestException);
    });

    it('POST /wagering/transactions - should return 200 OK for processed transaction and query it via GET endpoints', async () => {
      const wallet = await walletsController.createWallet({
        playerId: 'p-1',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      const mockRes = createMockResponse();
      await wageringController.submitTransaction(
        'prov-a:tx-100',
        {
          providerId: 'prov-a',
          externalTransactionId: 'tx-100',
          playerId: 'p-1',
          walletId: wallet.id,
          roundId: 'r-1',
          gameId: 'g-1',
          kind: WagerTransactionKind.Bet,
          money: { amount: '25.00', currency: 'BRL' },
        },
        mockRes,
      );

      expect(mockRes.statusCode).toBe(200);
      const data = (mockRes as any).data;
      expect(data.status).toBe(WagerTransactionStatus.Processed);
      expect(data.balance.amount).toBe('75.00');

      // Consulta por transactionId (GET /wagering/transactions/:transactionId)
      const byId = await wageringController.getTransactionById(data.transactionId);
      expect(byId.id).toBe(data.transactionId);
      expect(byId.externalTransactionId).toBe('tx-100');
      expect(byId.money.amount).toBe('25.00');

      // Consulta por providerId + externalTransactionId
      const byExternal = await wageringController.getTransactionByExternalId('prov-a', 'tx-100');
      expect(byExternal.id).toBe(data.transactionId);
      expect(byExternal.providerId).toBe('prov-a');

      // 404 para IDs inexistentes
      expect(wageringController.getTransactionById('non-existent-id')).rejects.toThrow(
        NotFoundException,
      );
      expect(
        wageringController.getTransactionByExternalId('prov-a', 'non-existent-ext'),
      ).rejects.toThrow(NotFoundException);
    });

    it('POST /wagering/transactions - should return 422 Unprocessable Entity when transaction is rejected', async () => {
      const wallet = await walletsController.createWallet({
        playerId: 'p-1',
        initialBalance: { amount: '10.00', currency: 'BRL' },
      });

      const mockRes = createMockResponse();
      await wageringController.submitTransaction(
        'prov-a:tx-101',
        {
          providerId: 'prov-a',
          externalTransactionId: 'tx-101',
          playerId: 'p-1',
          walletId: wallet.id,
          roundId: 'r-1',
          gameId: 'g-1',
          kind: WagerTransactionKind.Bet,
          money: { amount: '20.00', currency: 'BRL' },
        },
        mockRes,
      );

      expect(mockRes.statusCode).toBe(422);
      const data = (mockRes as any).data;
      expect(data.status).toBe(WagerTransactionStatus.Rejected);
      expect(data.failureCode).toBe('INSUFFICIENT_FUNDS');
    });

    it('POST /wagering/transactions - should return 202 Accepted when reference is missing (PENDING_REFERENCE)', async () => {
      const wallet = await walletsController.createWallet({
        playerId: 'p-1',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      const mockRes = createMockResponse();
      await wageringController.submitTransaction(
        'prov-a:refund-1',
        {
          providerId: 'prov-a',
          externalTransactionId: 'refund-1',
          playerId: 'p-1',
          walletId: wallet.id,
          roundId: 'r-1',
          gameId: 'g-1',
          kind: WagerTransactionKind.Refund,
          money: { amount: '20.00', currency: 'BRL' },
          referenceExternalTransactionId: 'non-existing-bet',
        },
        mockRes,
      );

      expect(mockRes.statusCode).toBe(202);
      const data = (mockRes as any).data;
      expect(data.status).toBe(WagerTransactionStatus.PendingReference);
    });
  });

  describe('HealthController', () => {
    it('GET /health/live - should return status ok and uptime', () => {
      const live = healthController.getLive();
      expect(live.status).toBe('ok');
      expect(typeof live.uptime).toBe('number');
    });

    it('GET /health/ready - should return 200 OK when DB and SQS are reachable, or 503 when degraded', async () => {
      // Injeta mock no sqsClient interno para simular ambiente saudável
      (healthController as any).sqsClient = {
        send: async () => ({ QueueUrls: [] }),
      };

      const resOk = createMockResponse();
      await healthController.getReady(resOk);
      expect(resOk.statusCode).toBe(HttpStatus.OK);
      expect((resOk as any).data.status).toBe('ok');

      // Simula queda do SQS -> deve retornar 503 SERVICE_UNAVAILABLE
      (healthController as any).sqsClient = {
        send: async () => {
          throw new Error('SQS connection refused');
        },
      };
      const resDegraded = createMockResponse();
      await healthController.getReady(resDegraded);
      expect(resDegraded.statusCode).toBe(HttpStatus.SERVICE_UNAVAILABLE);
      expect((resDegraded as any).data.status).toBe('degraded');
    });

    it('GET /health/metrics - should return metrics snapshot', () => {
      metricsService.incrementLockConflict();
      metricsService.incrementDlq();
      metricsService.incrementRetry();
      metricsService.setOutboxLag(42);

      const metrics = healthController.getMetrics();
      expect(metrics).toHaveProperty('transactionsByStatus');
      expect(metrics.lockConflictsCount).toBe(1);
      expect(metrics.dlqMessagesCount).toBe(1);
      expect(metrics.retriesCount).toBe(1);
      expect(metrics.outboxLagMs).toBe(42);
    });
  });

  describe('GlobalHttpExceptionFilter, AuthGuard & CorrelationIdMiddleware', () => {
    it('GlobalHttpExceptionFilter should map domain errors to semantic HTTP status codes', () => {
      const silentLogger = {
        error: () => {},
      } as unknown as AppLoggerService;
      const filter = new GlobalHttpExceptionFilter(silentLogger);

      const createHost = (res: Response): ArgumentsHost =>
        ({
          switchToHttp: () => ({
            getResponse: () => res,
            getRequest: () => ({
              correlationId: 'corr-123',
              url: '/wagering/transactions',
              method: 'POST',
            }),
          }),
        }) as unknown as ArgumentsHost;

      // 1. IdempotencyConflictError -> 409 CONFLICT
      const res409 = createMockResponse();
      filter.catch(new IdempotencyConflictError('key-1'), createHost(res409));
      expect(res409.statusCode).toBe(HttpStatus.CONFLICT);

      // 2. WalletConflictError -> 409 CONFLICT
      const resWallet409 = createMockResponse();
      filter.catch(new WalletConflictError('p-1', 'BRL'), createHost(resWallet409));
      expect(resWallet409.statusCode).toBe(HttpStatus.CONFLICT);

      // 3. WalletNotFoundError -> 404 NOT_FOUND
      const res404 = createMockResponse();
      filter.catch(new WalletNotFoundError('w-404'), createHost(res404));
      expect(res404.statusCode).toBe(HttpStatus.NOT_FOUND);

      // 4. InvalidMoneyError / CurrencyMismatchError -> 400 BAD_REQUEST
      const res400 = createMockResponse();
      filter.catch(new InvalidMoneyError('Bad amount'), createHost(res400));
      expect(res400.statusCode).toBe(HttpStatus.BAD_REQUEST);

      const resCurr400 = createMockResponse();
      filter.catch(new CurrencyMismatchError('BRL', 'USD'), createHost(resCurr400));
      expect(resCurr400.statusCode).toBe(HttpStatus.BAD_REQUEST);

      // 5. InsufficientBalanceError / InvalidTransactionReferenceError -> 422 UNPROCESSABLE_ENTITY
      const res422 = createMockResponse();
      filter.catch(
        new InsufficientBalanceError('w-1', '10.00', '50.00'),
        createHost(res422),
      );
      expect(res422.statusCode).toBe(HttpStatus.UNPROCESSABLE_ENTITY);
      expect((res422 as any).data.failureCode).toBe('INSUFFICIENT_FUNDS');

      const resRef422 = createMockResponse();
      filter.catch(
        new InvalidTransactionReferenceError(
          FailureCode.REFERENCE_ALREADY_REVERSED,
          'Already reversed',
        ),
        createHost(resRef422),
      );
      expect(resRef422.statusCode).toBe(HttpStatus.UNPROCESSABLE_ENTITY);
      expect((resRef422 as any).data.failureCode).toBe(FailureCode.REFERENCE_ALREADY_REVERSED);

      const resLedger422 = createMockResponse();
      filter.catch(new InvalidLedgerEntryError('Ledger mismatch'), createHost(resLedger422));
      expect(resLedger422.statusCode).toBe(HttpStatus.UNPROCESSABLE_ENTITY);

      // 6. NestJS HttpException -> preserves status
      const resBadReq = createMockResponse();
      filter.catch(new BadRequestException('Invalid DTO'), createHost(resBadReq));
      expect(resBadReq.statusCode).toBe(HttpStatus.BAD_REQUEST);
    });

    it('AuthGuard & CorrelationIdMiddleware should process headers and provider identity', async () => {
      const identityPort = new NoopProviderIdentityPort();
      const guard = new AuthGuard(identityPort);

      const healthCtx = {
        switchToHttp: () => ({
          getRequest: () => ({ path: '/health/live', headers: {} }),
        }),
      } as unknown as ExecutionContext;
      expect(await guard.canActivate(healthCtx)).toBe(true);

      const apiCtx = {
        switchToHttp: () => ({
          getRequest: () => ({
            path: '/wagering/transactions',
            headers: { 'x-provider-id': 'prov-a', authorization: 'Bearer tok-123' },
          }),
        }),
      } as unknown as ExecutionContext;
      expect(await guard.canActivate(apiCtx)).toBe(true);

      // CorrelationIdMiddleware
      const middleware = new CorrelationIdMiddleware();
      const req = { headers: {} } as unknown as RequestWithCorrelation;
      const res = createMockResponse();
      let nextCalled = false;
      middleware.use(req, res, () => {
        nextCalled = true;
      });
      expect(nextCalled).toBe(true);
      expect(typeof req.correlationId).toBe('string');
      expect(req.correlationId!.length).toBeGreaterThan(10);
    });
  });
});
