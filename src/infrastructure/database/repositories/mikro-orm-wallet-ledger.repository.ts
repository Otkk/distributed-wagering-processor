import Decimal from 'decimal.js';
import { EntityManager } from '@mikro-orm/postgresql';
import {
  LedgerPaginationResult,
  WalletLedgerRepository,
} from '../../../domain/ports/repositories.port';
import { WalletLedgerEntry } from '../../../domain/models/wallet-ledger-entry';
import { WalletLedgerEntryEntity } from '../entities/wallet-ledger-entry.entity';

interface CursorData {
  createdAt: string;
  id: string;
}

export class MikroOrmWalletLedgerRepository implements WalletLedgerRepository {
  constructor(private readonly em: EntityManager) {}

  public async save(entry: WalletLedgerEntry): Promise<void> {
    const entity = WalletLedgerEntryEntity.fromDomain(entry);
    await this.em.persistAndFlush(entity);
  }

  public async findByWalletId(
    walletId: string,
    options?: { limit: number; cursor?: string },
  ): Promise<LedgerPaginationResult> {
    const limit = options?.limit || 50;
    const qb = this.em.createQueryBuilder(WalletLedgerEntryEntity, 'l');

    qb.where({ walletId });

    if (options?.cursor) {
      try {
        const decoded = Buffer.from(options.cursor, 'base64').toString('utf8');
        const cursorData: CursorData = JSON.parse(decoded);
        qb.andWhere(
          '(l.created_at < ? or (l.created_at = ? and l.id < ?))',
          [new Date(cursorData.createdAt), new Date(cursorData.createdAt), cursorData.id],
        );
      } catch {
        // Ignora cursor malformado e busca do início
      }
    }

    qb.orderBy({ createdAt: 'DESC', id: 'DESC' });
    qb.limit(limit + 1);

    const entities = await qb.getResultList();
    const hasMore = entities.length > limit;
    const sliced = hasMore ? entities.slice(0, limit) : entities;

    let nextCursor: string | undefined;
    if (hasMore && sliced.length > 0) {
      const last = sliced[sliced.length - 1];
      const cursorPayload: CursorData = {
        createdAt: last.createdAt.toISOString(),
        id: last.id,
      };
      nextCursor = Buffer.from(JSON.stringify(cursorPayload)).toString('base64');
    }

    return {
      entries: sliced.map((e) => e.toDomain()),
      nextCursor,
    };
  }

  public async calculateBalance(walletId: string): Promise<{ calculatedAmount: string; count: number }> {
    const knex = this.em.getKnex();
    const result = await knex('wallet_ledger_entries')
      .where('wallet_id', walletId)
      .select(
        knex.raw(
          "COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0.00) as calculated_amount",
        ),
        knex.raw('COUNT(*) as count'),
      )
      .first();

    const rawAmount = String(result?.calculated_amount ?? '0.00');
    const decimalAmount = new Decimal(rawAmount).toDecimalPlaces(2, Decimal.ROUND_HALF_EVEN);
    return {
      calculatedAmount: decimalAmount.toFixed(2),
      count: parseInt(String(result?.count ?? '0'), 10),
    };
  }
}
