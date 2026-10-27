import { Module, Global } from '@nestjs/common';
import { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import mikroOrmConfig from './mikro-orm.config';
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
        return await MikroORM.init(mikroOrmConfig);
      },
    },
    {
      provide: EntityManager,
      useFactory: (orm: MikroORM) => orm.em.fork(),
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
export class DatabaseModule {}
