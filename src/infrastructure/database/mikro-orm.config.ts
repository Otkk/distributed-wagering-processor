import { Migrator } from '@mikro-orm/migrations';
import { defineConfig, Options } from '@mikro-orm/postgresql';
import { InboxMessageEntity } from './entities/inbox-message.entity';
import { OutboxMessageEntity } from './entities/outbox-message.entity';
import { WagerTransactionEntity } from './entities/wager-transaction.entity';
import { WalletLedgerEntryEntity } from './entities/wallet-ledger-entry.entity';
import { WalletEntity } from './entities/wallet.entity';
import { Migration20261027000001_InitialSchema } from './migrations/Migration20261027000001_InitialSchema';

export function buildMikroOrmConfig(overrides?: Partial<Options>): Options {
  return defineConfig({
    host: process.env.DB_HOST || 'localhost',
    port: Number(process.env.DB_PORT) || 5432,
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
    dbName: process.env.DB_NAME || 'wagering_db',
    pool: {
      min: Number(process.env.DB_POOL_MIN) || 2,
      max: Number(process.env.DB_POOL_MAX) || 25,
    },
    entities: [
      WalletEntity,
      WagerTransactionEntity,
      WalletLedgerEntryEntity,
      InboxMessageEntity,
      OutboxMessageEntity,
    ],
    migrations: {
      path: './src/infrastructure/database/migrations',
      pathTs: './src/infrastructure/database/migrations',
      migrationsList: [
        {
          name: 'Migration20261027000001_InitialSchema',
          class: Migration20261027000001_InitialSchema,
        },
      ],
      tableName: 'mikro_orm_migrations',
      transactional: true,
      allOrNothing: true,
    },
    extensions: [Migrator],
    debug: process.env.DB_DEBUG === 'true',
    ...overrides,
  });
}

export default buildMikroOrmConfig();
