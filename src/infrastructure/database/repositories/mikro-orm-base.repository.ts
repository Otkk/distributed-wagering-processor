import { EntityManager, RequestContext, TransactionContext } from '@mikro-orm/postgresql';

export abstract class MikroOrmBaseRepository {
  constructor(protected readonly em: EntityManager) {}

  /**
   * Resolve o EntityManager ativo no escopo transacional (TransactionContext),
   * no escopo da requisição (RequestContext) ou cria um fork limpo para leituras isoladas.
   */
  protected getEm(): EntityManager {
    const txEm = TransactionContext.getEntityManager() as EntityManager | undefined;
    if (txEm) {
      return txEm;
    }

    if (typeof this.em.getTransactionContext === 'function' && this.em.getTransactionContext()) {
      return this.em;
    }

    const reqEm = RequestContext.getEntityManager() as EntityManager | undefined;
    if (reqEm) {
      return reqEm;
    }

    return typeof this.em.fork === 'function' ? this.em.fork({ clear: true }) : this.em;
  }
}
