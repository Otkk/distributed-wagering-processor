import { Entity, Index, PrimaryKey, Property, Unique } from '@mikro-orm/core';
import { FailureCode } from '../../../domain/errors/domain.error';
import { Money } from '../../../domain/models/money';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../../domain/models/wager-transaction';

@Entity({ tableName: 'wager_transactions' })
@Unique({ properties: ['idempotencyKey'] })
@Unique({ properties: ['providerId', 'externalTransactionId'] })
export class WagerTransactionEntity {
  @PrimaryKey({ type: 'uuid' })
  id!: string;

  @Property({ type: 'varchar', length: 100 })
  @Index()
  providerId!: string;

  @Property({ type: 'varchar', length: 255 })
  externalTransactionId!: string;

  @Property({ type: 'varchar', length: 255 })
  idempotencyKey!: string;

  @Property({ type: 'varchar', length: 64 })
  payloadHash!: string;

  @Property({ type: 'uuid' })
  @Index()
  walletId!: string;

  @Property({ type: 'varchar', length: 255 })
  playerId!: string;

  @Property({ type: 'varchar', length: 255 })
  roundId!: string;

  @Property({ type: 'varchar', length: 255 })
  gameId!: string;

  @Property({ type: 'varchar', length: 20 })
  kind!: string;

  @Property({ type: 'numeric', precision: 18, scale: 2 })
  amount!: string;

  @Property({ type: 'varchar', length: 3 })
  currency!: string;

  @Property({ type: 'varchar', length: 20 })
  @Index()
  status!: string;

  @Property({ type: 'varchar', length: 255, nullable: true })
  @Index()
  referenceExternalTransactionId?: string;

  @Property({ type: 'uuid', nullable: true })
  referenceTransactionId?: string;

  @Property({ type: 'varchar', length: 50, nullable: true })
  failureCode?: string;

  @Property({ type: 'timestamptz', nullable: true })
  processedAt?: Date;

  @Property({ type: 'timestamptz' })
  createdAt: Date = new Date();

  public toDomain(): WagerTransaction {
    return WagerTransaction.rehydrate({
      id: this.id,
      providerId: this.providerId,
      externalTransactionId: this.externalTransactionId,
      idempotencyKey: this.idempotencyKey,
      payloadHash: this.payloadHash,
      walletId: this.walletId,
      playerId: this.playerId,
      roundId: this.roundId,
      gameId: this.gameId,
      kind: this.kind as WagerTransactionKind,
      money: Money.from({ amount: this.amount, currency: this.currency }),
      referenceExternalTransactionId: this.referenceExternalTransactionId,
      createdAt: this.createdAt,
      status: this.status as WagerTransactionStatus,
      referenceTransactionId: this.referenceTransactionId,
      failureCode: this.failureCode as FailureCode | undefined,
      processedAt: this.processedAt,
    });
  }

  public static fromDomain(tx: WagerTransaction): WagerTransactionEntity {
    const entity = new WagerTransactionEntity();
    entity.id = tx.id;
    entity.providerId = tx.providerId;
    entity.externalTransactionId = tx.externalTransactionId;
    entity.idempotencyKey = tx.idempotencyKey;
    entity.payloadHash = tx.payloadHash;
    entity.walletId = tx.walletId;
    entity.playerId = tx.playerId;
    entity.roundId = tx.roundId;
    entity.gameId = tx.gameId;
    entity.kind = tx.kind;
    entity.amount = tx.money.amount;
    entity.currency = tx.money.currency;
    entity.status = tx.status;
    entity.referenceExternalTransactionId = tx.referenceExternalTransactionId;
    entity.referenceTransactionId = tx.referenceTransactionId;
    entity.failureCode = tx.failureCode;
    entity.processedAt = tx.processedAt;
    entity.createdAt = tx.createdAt;
    return entity;
  }
}
