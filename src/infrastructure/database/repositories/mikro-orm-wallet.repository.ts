import { EntityManager, LockMode } from '@mikro-orm/postgresql';
import { WalletRepository } from '../../../domain/ports/repositories.port';
import { Wallet } from '../../../domain/models/wallet';
import { WalletEntity } from '../entities/wallet.entity';
import { MikroOrmBaseRepository } from './mikro-orm-base.repository';

export class MikroOrmWalletRepository extends MikroOrmBaseRepository implements WalletRepository {
  constructor(em: EntityManager) {
    super(em);
  }

  public async findById(id: string, lock?: 'pessimistic_write'): Promise<Wallet | null> {
    const em = this.getEm();
    const lockMode = lock === 'pessimistic_write' ? LockMode.PESSIMISTIC_WRITE : undefined;
    const entity = await em.findOne(
      WalletEntity,
      { id },
      lockMode ? { lockMode, refresh: true } : undefined,
    );

    return entity ? entity.toDomain() : null;
  }

  public async findByPlayerIdAndCurrency(playerId: string, currency: string): Promise<Wallet | null> {
    const em = this.getEm();
    const entity = await em.findOne(WalletEntity, { playerId, currency });
    return entity ? entity.toDomain() : null;
  }

  public async save(wallet: Wallet): Promise<void> {
    const em = this.getEm();
    const entity = WalletEntity.fromDomain(wallet);
    await em.upsert(WalletEntity, entity);
  }
}
