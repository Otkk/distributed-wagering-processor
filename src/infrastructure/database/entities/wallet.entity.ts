import { Entity, Index, PrimaryKey, Property, Unique } from '@mikro-orm/core';
import { Money } from '../../../domain/models/money';
import { Wallet } from '../../../domain/models/wallet';

@Entity({ tableName: 'wallets' })
@Unique({ properties: ['playerId', 'currency'] })
export class WalletEntity {
  @PrimaryKey({ type: 'uuid' })
  id!: string;

  @Property({ type: 'varchar', length: 255 })
  @Index()
  playerId!: string;

  @Property({ type: 'varchar', length: 3 })
  currency!: string;

  @Property({ type: 'numeric', precision: 18, scale: 2 })
  balanceAmount!: string;

  @Property({ type: 'bigint' })
  version!: string; // Stored as bigint, mapped as string/number

  @Property({ type: 'timestamptz' })
  createdAt: Date = new Date();

  @Property({ type: 'timestamptz', onUpdate: () => new Date() })
  updatedAt: Date = new Date();

  public toDomain(): Wallet {
    return Wallet.rehydrate({
      id: this.id,
      playerId: this.playerId,
      currency: this.currency,
      balance: Money.from({ amount: this.balanceAmount, currency: this.currency }),
      version: Number(this.version),
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
    });
  }

  public static fromDomain(wallet: Wallet): WalletEntity {
    const entity = new WalletEntity();
    entity.id = wallet.id;
    entity.playerId = wallet.playerId;
    entity.currency = wallet.currency;
    entity.balanceAmount = wallet.balance.amount;
    entity.version = String(wallet.version);
    entity.createdAt = wallet.createdAt;
    entity.updatedAt = wallet.updatedAt;
    return entity;
  }
}
