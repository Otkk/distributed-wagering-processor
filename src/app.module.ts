import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
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
      useFactory: (wRepo: WalletRepository, lRepo: WalletLedgerRepository) =>
        new ReconcileWalletUseCase(wRepo, lRepo),
      inject: ['WalletRepository', 'WalletLedgerRepository'],
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
      ) => new ProcessWagerTransactionUseCase(wRepo, txRepo, lRepo, oRepo, iRepo, uow),
      inject: [
        'WalletRepository',
        'WagerTransactionRepository',
        'WalletLedgerRepository',
        'OutboxRepository',
        'InboxRepository',
        'UnitOfWork',
      ],
    },
    {
      provide: 'WalletRepository',
      useFactory: (wRepo: WalletRepository) => wRepo,
      inject: ['WalletRepository'],
    },
    {
      provide: 'WagerTransactionRepository',
      useFactory: (txRepo: WagerTransactionRepository) => txRepo,
      inject: ['WagerTransactionRepository'],
    },
  ],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(CorrelationIdMiddleware).forRoutes('*');
  }
}
