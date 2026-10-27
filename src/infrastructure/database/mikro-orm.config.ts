import { defineConfig } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';

export default defineConfig({
  host: process.env.DB_HOST || 'localhost',
  port: Number(process.env.DB_PORT) || 5432,
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  dbName: process.env.DB_NAME || 'wagering_db',
  entities: ['./dist/src/infrastructure/database/entities', './src/infrastructure/database/entities'],
  entitiesTs: ['./src/infrastructure/database/entities'],
  migrations: {
    path: './dist/src/infrastructure/database/migrations',
    pathTs: './src/infrastructure/database/migrations',
    tableName: 'mikro_orm_migrations',
    transactional: true,
    allOrNothing: true,
  },
  extensions: [Migrator],
  debug: process.env.NODE_ENV === 'development',
});
