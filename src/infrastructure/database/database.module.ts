import { Module, Global, Inject, OnApplicationShutdown } from '@nestjs/common';
import { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { buildMikroOrmConfig } from './mikro-orm.config';
import { MikroOrmWalletRepository } from './repositories/mikro-orm-wallet.repository';
import { MikroOrmWagerTransactionRepository } from './repositories/mikro-orm-wager-transaction.repository';
import { MikroOrmWalletLedgerRepository } from './repositories/mikro-orm-wallet-ledger.repository';
import { MikroOrmInboxRepository } from './repositories/mikro-orm-inbox.repository';
import { MikroOrmOutboxRepository } from './repositories/mikro-orm-outbox.repository';
import { MikroOrmUnitOfWork } from './mikro-orm-unit-of-work';

@Global()
@Module({
  providers: [
    {
      provide: 'MIKRO_ORM',
      useFactory: async () => {
        const orm = await MikroORM.init(buildMikroOrmConfig());
        if (process.env.AUTO_MIGRATE === 'true') {
          await orm.getMigrator().up();
        }
        return orm;
      },
    },
    {
      provide: EntityManager,
      // Mantém useContext=true para que TransactionContext/RequestContext sejam respeitados
      useFactory: (orm: MikroORM) => orm.em,
      inject: ['MIKRO_ORM'],
    },
    {
      provide: 'WalletRepository',
      useFactory: (em: EntityManager) => new MikroOrmWalletRepository(em),
      inject: [EntityManager],
    },
    {
      provide: 'WagerTransactionRepository',
      useFactory: (em: EntityManager) => new MikroOrmWagerTransactionRepository(em),
      inject: [EntityManager],
    },
    {
      provide: 'WalletLedgerRepository',
      useFactory: (em: EntityManager) => new MikroOrmWalletLedgerRepository(em),
      inject: [EntityManager],
    },
    {
      provide: 'InboxRepository',
      useFactory: (em: EntityManager) => new MikroOrmInboxRepository(em),
      inject: [EntityManager],
    },
    {
      provide: 'OutboxRepository',
      useFactory: (em: EntityManager) => new MikroOrmOutboxRepository(em),
      inject: [EntityManager],
    },
    {
      provide: 'UnitOfWork',
      useFactory: (em: EntityManager) => new MikroOrmUnitOfWork(em),
      inject: [EntityManager],
    },
  ],
  exports: [
    'MIKRO_ORM',
    EntityManager,
    'WalletRepository',
    'WagerTransactionRepository',
    'WalletLedgerRepository',
    'InboxRepository',
    'OutboxRepository',
    'UnitOfWork',
  ],
})
export class DatabaseModule implements OnApplicationShutdown {
  constructor(@Inject('MIKRO_ORM') private readonly orm: MikroORM) {}

  async onApplicationShutdown(): Promise<void> {
    if (this.orm && (await this.orm.isConnected())) {
      await this.orm.close(true);
    }
  }
}
