import { EntityManager } from '@mikro-orm/postgresql';
import { InboxRepository } from '../../../domain/ports/repositories.port';
import { InboxMessage } from '../../../domain/models/inbox-message';
import { InboxMessageEntity } from '../entities/inbox-message.entity';

export class MikroOrmInboxRepository implements InboxRepository {
  constructor(private readonly em: EntityManager) {}

  public async find(consumerName: string, messageId: string): Promise<InboxMessage | null> {
    const entity = await this.em.findOne(InboxMessageEntity, { consumerName, messageId });
    return entity ? entity.toDomain() : null;
  }

  public async save(inbox: InboxMessage): Promise<void> {
    const entity = InboxMessageEntity.fromDomain(inbox);
    await this.em.upsert(InboxMessageEntity, entity);
  }
}
