import { MikroORM } from '@mikro-orm/postgresql';
import { buildMikroOrmConfig } from '../src/infrastructure/database/mikro-orm.config';

async function run() {
  const action = process.argv[2] || 'up';
  const orm = await MikroORM.init(buildMikroOrmConfig());
  const migrator = orm.getMigrator();

  try {
    if (action === 'up') {
      const executed = await migrator.up();
      console.log(
        JSON.stringify({
          level: 'info',
          action: 'migration:up',
          executed: executed.map((m) => m.name),
          status: 'success',
        }),
      );
    } else if (action === 'down') {
      const reverted = await migrator.down();
      console.log(
        JSON.stringify({
          level: 'info',
          action: 'migration:down',
          reverted: reverted.map((m) => m.name),
          status: 'success',
        }),
      );
    } else if (action === 'fresh') {
      await orm.schema.dropSchema();
      const executed = await migrator.up();
      console.log(
        JSON.stringify({
          level: 'info',
          action: 'migration:fresh',
          executed: executed.map((m) => m.name),
          status: 'success',
        }),
      );
    } else {
      throw new Error(`Unknown migration action: ${action}. Expected 'up', 'down', or 'fresh'.`);
    }
  } finally {
    await orm.close(true);
  }
}

run().catch((err) => {
  console.error(
    JSON.stringify({
      level: 'error',
      message: (err as Error).message,
      stack: (err as Error).stack,
    }),
  );
  process.exit(1);
});
