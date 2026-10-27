import { Entity, PrimaryKeyProp, Property } from '@mikro-orm/core';
import { InboxMessage } from '../../../domain/models/inbox-message';

@Entity({ tableName: 'inbox_messages' })
export class InboxMessageEntity {
  [PrimaryKeyProp]?: ['consumerName', 'messageId'];

  @Property({ primary: true, type: 'varchar', length: 100 })
  consumerName!: string;

  @Property({ primary: true, type: 'varchar', length: 255 })
  messageId!: string;

  @Property({ type: 'varchar', length: 64 })
  payloadHash!: string;

  @Property({ type: 'timestamptz' })
  receivedAt: Date = new Date();

  @Property({ type: 'timestamptz', nullable: true })
  processedAt?: Date;

  public toDomain(): InboxMessage {
    return InboxMessage.rehydrate({
      messageId: this.messageId,
      consumerName: this.consumerName,
      payloadHash: this.payloadHash,
      receivedAt: this.receivedAt,
      processedAt: this.processedAt,
    });
  }

  public static fromDomain(inbox: InboxMessage): InboxMessageEntity {
    const entity = new InboxMessageEntity();
    entity.consumerName = inbox.consumerName;
    entity.messageId = inbox.messageId;
    entity.payloadHash = inbox.payloadHash;
    entity.receivedAt = inbox.receivedAt;
    entity.processedAt = inbox.processedAt;
    return entity;
  }
}
