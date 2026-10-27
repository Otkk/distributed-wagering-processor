import { EntityManager, LockMode } from '@mikro-orm/postgresql';
import { WalletRepository } from '../../../domain/ports/repositories.port';
import { Wallet } from '../../../domain/models/wallet';
import { WalletEntity } from '../entities/wallet.entity';

export class MikroOrmWalletRepository implements WalletRepository {
  constructor(private readonly em: EntityManager) {}

  public async findById(id: string, lock?: 'pessimistic_write'): Promise<Wallet | null> {
    const lockMode = lock === 'pessimistic_write' ? LockMode.PESSIMISTIC_WRITE : undefined;
    const entity = await this.em.findOne(
      WalletEntity,
      { id },
      { lockMode },
    );

    return entity ? entity.toDomain() : null;
  }

  public async findByPlayerIdAndCurrency(playerId: string, currency: string): Promise<Wallet | null> {
    const entity = await this.em.findOne(WalletEntity, { playerId, currency });
    return entity ? entity.toDomain() : null;
  }

  public async save(wallet: Wallet): Promise<void> {
    const entity = WalletEntity.fromDomain(wallet);
    await this.em.upsert(WalletEntity, entity);
  }
}
