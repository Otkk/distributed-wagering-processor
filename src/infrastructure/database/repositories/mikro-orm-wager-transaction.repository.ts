import { EntityManager } from '@mikro-orm/postgresql';
import { WagerTransactionRepository } from '../../../domain/ports/repositories.port';
import { WagerTransaction, WagerTransactionStatus } from '../../../domain/models/wager-transaction';
import { WagerTransactionEntity } from '../entities/wager-transaction.entity';

export class MikroOrmWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly em: EntityManager) {}

  public async findById(id: string): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, { id });
    return entity ? entity.toDomain() : null;
  }

  public async findByIdempotencyKey(key: string): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, { idempotencyKey: key });
    return entity ? entity.toDomain() : null;
  }

  public async findByProviderAndExternalId(
    providerId: string,
    externalId: string,
  ): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, {
      providerId,
      externalTransactionId: externalId,
    });
    return entity ? entity.toDomain() : null;
  }

  public async findPendingReferences(limit: number): Promise<WagerTransaction[]> {
    const entities = await this.em.find(
      WagerTransactionEntity,
      { status: WagerTransactionStatus.PendingReference },
      { limit, orderBy: { createdAt: 'ASC' } },
    );
    return entities.map((e) => e.toDomain());
  }

  public async findProcessedReversal(
    referenceTransactionId: string,
    kind: string,
  ): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, {
      referenceTransactionId,
      kind,
      status: WagerTransactionStatus.Processed,
    });
    return entity ? entity.toDomain() : null;
  }

  public async save(transaction: WagerTransaction): Promise<void> {
    const entity = WagerTransactionEntity.fromDomain(transaction);
    await this.em.upsert(WagerTransactionEntity, entity);
  }
}
