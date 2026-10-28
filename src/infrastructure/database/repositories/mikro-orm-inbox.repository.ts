import { EntityManager } from '@mikro-orm/postgresql';
import { InboxRepository } from '../../../domain/ports/repositories.port';
import { InboxMessage } from '../../../domain/models/inbox-message';
import { InboxMessageEntity } from '../entities/inbox-message.entity';
import { MikroOrmBaseRepository } from './mikro-orm-base.repository';

export class MikroOrmInboxRepository extends MikroOrmBaseRepository implements InboxRepository {
  constructor(em: EntityManager) {
    super(em);
  }

  public async find(consumerName: string, messageId: string): Promise<InboxMessage | null> {
    const em = this.getEm();
    const entity = await em.findOne(InboxMessageEntity, {
      consumerName,
      messageId,
    });
    return entity ? entity.toDomain() : null;
  }

  public async save(inbox: InboxMessage): Promise<void> {
    const em = this.getEm();
    const entity = InboxMessageEntity.fromDomain(inbox);
    await em.upsert(InboxMessageEntity, entity);
  }
}
