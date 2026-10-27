import { EntityManager, LockMode } from '@mikro-orm/postgresql';
import { OutboxRepository } from '../../../domain/ports/repositories.port';
import { OutboxMessage } from '../../../domain/models/outbox-message';
import { OutboxMessageEntity } from '../entities/outbox-message.entity';

export class MikroOrmOutboxRepository implements OutboxRepository {
  constructor(private readonly em: EntityManager) {}

  public async save(outbox: OutboxMessage): Promise<void> {
    const entity = OutboxMessageEntity.fromDomain(outbox);
    await this.em.upsert(OutboxMessageEntity, entity);
  }

  public async findPendingToDispatch(limit: number, now: Date): Promise<OutboxMessage[]> {
    const qb = this.em.createQueryBuilder(OutboxMessageEntity, 'o');

    qb.where({
      publishedAt: null,
      $or: [{ nextAttemptAt: null }, { nextAttemptAt: { $lte: now } }],
    });
    qb.orderBy({ occurredAt: 'ASC' });
    qb.limit(limit);
    qb.setLockMode(LockMode.PESSIMISTIC_WRITE_OR_FAIL); // or SKIP LOCKED via custom raw query

    const knex = this.em.getKnex();
    // Native query with FOR UPDATE SKIP LOCKED to ensure multiple concurrent workers don't collide
    const results = await knex('outbox_messages')
      .whereNull('published_at')
      .andWhere((builder) => {
        builder.whereNull('next_attempt_at').orWhere('next_attempt_at', '<=', now);
      })
      .orderBy('occurred_at', 'asc')
      .limit(limit)
      .forUpdate()
      .skipLocked();

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
