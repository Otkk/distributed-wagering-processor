import { Entity, Index, PrimaryKey, Property } from '@mikro-orm/core';
import { LedgerDirection } from '../../../domain/models/ledger-direction';
import { Money } from '../../../domain/models/money';
import { WalletLedgerEntry } from '../../../domain/models/wallet-ledger-entry';

@Entity({ tableName: 'wallet_ledger_entries' })
@Index({ properties: ['walletId', 'createdAt', 'id'] })
export class WalletLedgerEntryEntity {
  @PrimaryKey({ type: 'uuid' })
  id!: string;

  @Property({ type: 'uuid' })
  @Index()
  walletId!: string;

  @Property({ type: 'uuid' })
  @Index()
  transactionId!: string;

  @Property({ type: 'varchar', length: 10 })
  direction!: string;

  @Property({ type: 'numeric', precision: 18, scale: 2 })
  amount!: string;

  @Property({ type: 'varchar', length: 3 })
  currency!: string;

  @Property({ type: 'numeric', precision: 18, scale: 2 })
  balanceBefore!: string;

  @Property({ type: 'numeric', precision: 18, scale: 2 })
  balanceAfter!: string;

  @Property({ type: 'timestamptz' })
  createdAt: Date = new Date();

  public toDomain(): WalletLedgerEntry {
    return WalletLedgerEntry.rehydrate({
      id: this.id,
      walletId: this.walletId,
      transactionId: this.transactionId,
      direction: this.direction as LedgerDirection,
      money: Money.from({ amount: this.amount, currency: this.currency }),
      balanceBefore: Money.from({ amount: this.balanceBefore, currency: this.currency }),
      balanceAfter: Money.from({ amount: this.balanceAfter, currency: this.currency }),
      createdAt: this.createdAt,
    });
  }

  public static fromDomain(entry: WalletLedgerEntry): WalletLedgerEntryEntity {
    const entity = new WalletLedgerEntryEntity();
    entity.id = entry.id;
    entity.walletId = entry.walletId;
    entity.transactionId = entry.transactionId;
    entity.direction = entry.direction;
    entity.amount = entry.money.amount;
    entity.currency = entry.money.currency;
    entity.balanceBefore = entry.balanceBefore.amount;
    entity.balanceAfter = entry.balanceAfter.amount;
    entity.createdAt = entry.createdAt;
    return entity;
  }
}
