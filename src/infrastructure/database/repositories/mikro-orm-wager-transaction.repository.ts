import { EntityManager } from '@mikro-orm/postgresql';
import { WagerTransactionRepository } from '../../../domain/ports/repositories.port';
import { WagerTransaction, WagerTransactionStatus } from '../../../domain/models/wager-transaction';
import { WagerTransactionEntity } from '../entities/wager-transaction.entity';
import { MikroOrmBaseRepository } from './mikro-orm-base.repository';

export class MikroOrmWagerTransactionRepository
  extends MikroOrmBaseRepository
  implements WagerTransactionRepository
{
  constructor(em: EntityManager) {
    super(em);
  }

  public async findById(id: string): Promise<WagerTransaction | null> {
    const em = this.getEm();
    const entity = await em.findOne(WagerTransactionEntity, { id });
    return entity ? entity.toDomain() : null;
  }

  public async findByIdempotencyKey(key: string): Promise<WagerTransaction | null> {
    const em = this.getEm();
    const entity = await em.findOne(WagerTransactionEntity, { idempotencyKey: key });
    return entity ? entity.toDomain() : null;
  }

  public async findByProviderAndExternalId(
    providerId: string,
    externalId: string,
  ): Promise<WagerTransaction | null> {
    const em = this.getEm();
    const entity = await em.findOne(WagerTransactionEntity, {
      providerId,
      externalTransactionId: externalId,
    });
    return entity ? entity.toDomain() : null;
  }

  public async findPendingReferences(limit: number): Promise<WagerTransaction[]> {
    const em = this.getEm();
    const entities = await em.find(
      WagerTransactionEntity,
      { status: WagerTransactionStatus.PendingReference },
      { limit, orderBy: { createdAt: 'ASC' } },
    );
    return entities.map((e) => e.toDomain());
  }

  public async findProcessedReversal(
    referenceTransactionId: string,
    kind?: string,
  ): Promise<WagerTransaction | null> {
    const em = this.getEm();
    const where: Record<string, unknown> = {
      referenceTransactionId,
      status: WagerTransactionStatus.Processed,
    };
    if (kind) {
      where.kind = kind;
    }
    const entity = await em.findOne(WagerTransactionEntity, where);
    return entity ? entity.toDomain() : null;
  }

  public async save(transaction: WagerTransaction): Promise<void> {
    const em = this.getEm();
    const entity = WagerTransactionEntity.fromDomain(transaction);
    await em.upsert(WagerTransactionEntity, entity);
  }
}
