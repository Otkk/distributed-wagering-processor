import { EntityManager } from '@mikro-orm/postgresql';
import { UnitOfWork } from '../../domain/ports/repositories.port';

export class MikroOrmUnitOfWork implements UnitOfWork {
  constructor(private readonly em: EntityManager) {}

  public async transactional<T>(fn: () => Promise<T>): Promise<T> {
    return this.em.transactional(
      async () => {
        return fn();
      },
      { clear: true },
    );
  }
}
