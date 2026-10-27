import { Entity, Index, PrimaryKey, Property } from '@mikro-orm/core';
import { OutboxMessage } from '../../../domain/models/outbox-message';

@Entity({ tableName: 'outbox_messages' })
@Index({ properties: ['publishedAt', 'nextAttemptAt'] })
export class OutboxMessageEntity {
  @PrimaryKey({ type: 'uuid' })
  id!: string;

  @Property({ type: 'varchar', length: 255 })
  @Index()
  aggregateId!: string;

  @Property({ type: 'varchar', length: 100 })
  eventType!: string;

  @Property({ type: 'jsonb' })
  payload!: Record<string, unknown>;

  @Property({ type: 'integer', default: 0 })
  attempts: number = 0;

  @Property({ type: 'timestamptz', nullable: true })
  nextAttemptAt?: Date;

  @Property({ type: 'timestamptz', nullable: true })
  publishedAt?: Date;

  @Property({ type: 'timestamptz' })
  occurredAt: Date = new Date();

  public toDomain(): OutboxMessage {
    return OutboxMessage.rehydrate({
      id: this.id,
      aggregateId: this.aggregateId,
      eventType: this.eventType,
      payload: Object.freeze(this.payload),
      occurredAt: this.occurredAt,
      attempts: this.attempts,
      nextAttemptAt: this.nextAttemptAt,
      publishedAt: this.publishedAt,
    });
  }

  public static fromDomain(outbox: OutboxMessage): OutboxMessageEntity {
    const entity = new OutboxMessageEntity();
    entity.id = outbox.id;
    entity.aggregateId = outbox.aggregateId;
    entity.eventType = outbox.eventType;
    entity.payload = { ...outbox.payload };
    entity.occurredAt = outbox.occurredAt;
    entity.attempts = outbox.attempts;
    entity.nextAttemptAt = outbox.nextAttemptAt;
    entity.publishedAt = outbox.publishedAt;
    return entity;
  }
}
