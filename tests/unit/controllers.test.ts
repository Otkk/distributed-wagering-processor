import { beforeEach, describe, expect, it } from 'bun:test';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import type { Response } from 'express';
import {
  CreateWalletUseCase,
} from '../../src/application/use-cases/create-wallet.use-case';
import { GetWalletLedgerUseCase } from '../../src/application/use-cases/get-wallet-ledger.use-case';
import {
  ProcessWagerTransactionUseCase,
  WalletNotFoundError,
} from '../../src/application/use-cases/process-wager-transaction.use-case';
import { ReconcileWalletUseCase } from '../../src/application/use-cases/reconcile-wallet.use-case';
import { Money } from '../../src/domain/models/money';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/models/wager-transaction';
import { Wallet } from '../../src/domain/models/wallet';
import { HealthController } from '../../src/infrastructure/http/controllers/health.controller';
import { WageringController } from '../../src/infrastructure/http/controllers/wagering.controller';
import { WalletsController } from '../../src/infrastructure/http/controllers/wallets.controller';
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
  return res as Response;
}

describe('HTTP Controllers Unit Tests', () => {
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
      save: async (t: WagerTransaction) => transactions.set(t.id, t),
    };

    mockLedgerRepo = {
      save: async () => {},
      findByWalletId: async () => ({ entries: [] }),
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
    healthController = new HealthController({} as any, metricsService);
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
          undefined, // Missing header!
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

    it('POST /wagering/transactions - should return 200 OK for processed transaction', async () => {
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
          money: { amount: '20.00', currency: 'BRL' }, // Exceeds balance!
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

    it('GET /health/metrics - should return metrics snapshot', () => {
      const metrics = healthController.getMetrics();
      expect(metrics).toHaveProperty('transactionsByStatus');
      expect(metrics).toHaveProperty('latency');
    });
  });
});
