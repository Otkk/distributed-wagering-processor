import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { CreateWalletUseCase } from './application/use-cases/create-wallet.use-case';
import { GetWalletLedgerUseCase } from './application/use-cases/get-wallet-ledger.use-case';
import { ProcessWagerTransactionUseCase } from './application/use-cases/process-wager-transaction.use-case';
import { ReconcileWalletUseCase } from './application/use-cases/reconcile-wallet.use-case';
import {
  InboxRepository,
  OutboxRepository,
  UnitOfWork,
  WagerTransactionRepository,
  WalletLedgerRepository,
  WalletRepository,
} from './domain/ports/repositories.port';
import { DatabaseModule } from './infrastructure/database/database.module';
import { HealthController } from './infrastructure/http/controllers/health.controller';
import { WageringController } from './infrastructure/http/controllers/wagering.controller';
import { WalletsController } from './infrastructure/http/controllers/wallets.controller';
import { AuthGuard, NoopProviderIdentityPort } from './infrastructure/http/guards/auth.guard';
import { CorrelationIdMiddleware } from './infrastructure/http/middlewares/correlation-id.middleware';
import { AppLoggerService } from './infrastructure/logger/app-logger.service';
import { OutboxDispatcherWorker } from './infrastructure/messaging/outbox/outbox-dispatcher.worker';
import { PendingReferenceWorker } from './infrastructure/messaging/outbox/pending-reference.worker';
import { SqsEventPublisher } from './infrastructure/messaging/sqs/sqs-event-publisher';
import {
  AwsSqsConsumerClient,
  WagerTransactionConsumer,
} from './infrastructure/messaging/sqs/wager-transaction.consumer';
import { MetricsService } from './infrastructure/metrics/metrics.service';

@Module({
  imports: [DatabaseModule],
  controllers: [WalletsController, WageringController, HealthController],
  providers: [
    AppLoggerService,
    MetricsService,
    AuthGuard,
    NoopProviderIdentityPort,
    {
      provide: APP_GUARD,
      useExisting: AuthGuard,
    },
    {
      provide: 'ProviderIdentityPort',
      useExisting: NoopProviderIdentityPort,
    },
    {
      provide: 'EventPublisherPort',
      useClass: SqsEventPublisher,
    },
    {
      provide: 'SqsConsumerClientPort',
      useClass: AwsSqsConsumerClient,
    },
    {
      provide: CreateWalletUseCase,
      useFactory: (
        wRepo: WalletRepository,
        txRepo: WagerTransactionRepository,
        lRepo: WalletLedgerRepository,
        oRepo: OutboxRepository,
        uow: UnitOfWork,
      ) => new CreateWalletUseCase(wRepo, txRepo, lRepo, oRepo, uow),
      inject: [
        'WalletRepository',
        'WagerTransactionRepository',
        'WalletLedgerRepository',
        'OutboxRepository',
        'UnitOfWork',
      ],
    },
    {
      provide: GetWalletLedgerUseCase,
      useFactory: (wRepo: WalletRepository, lRepo: WalletLedgerRepository) =>
        new GetWalletLedgerUseCase(wRepo, lRepo),
      inject: ['WalletRepository', 'WalletLedgerRepository'],
    },
    {
      provide: ReconcileWalletUseCase,
      useFactory: (
        wRepo: WalletRepository,
        lRepo: WalletLedgerRepository,
        logger: AppLoggerService,
        metrics: MetricsService,
      ) =>
        new ReconcileWalletUseCase(wRepo, lRepo, {
          onDivergence: (details) => {
            metrics.incrementReconciliationDivergence();
            logger.error(
              `Accounting reconciliation divergence detected for wallet ${details.walletId}`,
              undefined,
              details,
            );
          },
        }),
      inject: ['WalletRepository', 'WalletLedgerRepository', AppLoggerService, MetricsService],
    },
    {
      provide: ProcessWagerTransactionUseCase,
      useFactory: (
        wRepo: WalletRepository,
        txRepo: WagerTransactionRepository,
        lRepo: WalletLedgerRepository,
        oRepo: OutboxRepository,
        iRepo: InboxRepository,
        uow: UnitOfWork,
        metrics: MetricsService,
      ) =>
        new ProcessWagerTransactionUseCase(wRepo, txRepo, lRepo, oRepo, iRepo, uow, {
          onLockConflict: () => {
            metrics.incrementLockConflict();
          },
        }),
      inject: [
        'WalletRepository',
        'WagerTransactionRepository',
        'WalletLedgerRepository',
        'OutboxRepository',
        'InboxRepository',
        'UnitOfWork',
        MetricsService,
      ],
    },
    OutboxDispatcherWorker,
    PendingReferenceWorker,
    WagerTransactionConsumer,
  ],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(CorrelationIdMiddleware).forRoutes('*');
  }
}
