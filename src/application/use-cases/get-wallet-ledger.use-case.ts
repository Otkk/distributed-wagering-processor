import { LedgerPaginationResult, WalletLedgerRepository, WalletRepository } from '../../domain/ports/repositories.port';
import { WalletNotFoundError } from './process-wager-transaction.use-case';

export interface GetWalletLedgerInput {
  walletId: string;
  limit?: number;
  cursor?: string;
}

export class GetWalletLedgerUseCase {
  constructor(
    private readonly walletRepo: WalletRepository,
    private readonly ledgerRepo: WalletLedgerRepository,
  ) {}

  public async execute(input: GetWalletLedgerInput): Promise<LedgerPaginationResult> {
    const wallet = await this.walletRepo.findById(input.walletId);
    if (!wallet) {
      throw new WalletNotFoundError(input.walletId);
    }

    const limit = Math.min(Math.max(input.limit || 50, 1), 100);
    return this.ledgerRepo.findByWalletId(input.walletId, {
      limit,
      cursor: input.cursor,
    });
  }
}
