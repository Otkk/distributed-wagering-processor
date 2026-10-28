import { EntityManager } from '@mikro-orm/postgresql';
import { OutboxRepository } from '../../../domain/ports/repositories.port';
import { OutboxMessage } from '../../../domain/models/outbox-message';
import { OutboxMessageEntity } from '../entities/outbox-message.entity';
import { MikroOrmBaseRepository } from './mikro-orm-base.repository';

export class MikroOrmOutboxRepository extends MikroOrmBaseRepository implements OutboxRepository {
  constructor(em: EntityManager) {
    super(em);
  }

  public async save(outbox: OutboxMessage): Promise<void> {
    const em = this.getEm();
    const entity = OutboxMessageEntity.fromDomain(outbox);
    await em.upsert(OutboxMessageEntity, entity);
  }

  public async findPendingToDispatch(limit: number, now: Date): Promise<OutboxMessage[]> {
    const em = this.getEm();
    const knex = em.getKnex();

    // Query nativa com FOR UPDATE SKIP LOCKED vinculada à transação ativa (se existir)
    const query = knex('outbox_messages')
      .whereNull('published_at')
      .andWhere((builder) => {
        builder.whereNull('next_attempt_at').orWhere('next_attempt_at', '<=', now);
      })
      .orderBy('occurred_at', 'asc')
      .limit(limit)
      .forUpdate()
      .skipLocked();

    const txCtx = typeof em.getTransactionContext === 'function' ? em.getTransactionContext() : undefined;
    if (txCtx && typeof (query as unknown as { transacting?: (trx: unknown) => void }).transacting === 'function') {
      (query as unknown as { transacting: (trx: unknown) => void }).transacting(txCtx);
    }

    const results = await query;

    return results.map((row) =>
      OutboxMessage.rehydrate({
        id: row.id,
        aggregateId: row.aggregate_id,
        eventType: row.event_type,
        payload: row.payload,
        occurredAt: new Date(row.occurred_at),
        attempts: Number(row.attempts),
        nextAttemptAt: row.next_attempt_at ? new Date(row.next_attempt_at) : undefined,
        publishedAt: row.published_at ? new Date(row.published_at) : undefined,
      }),
    );
  }
}
