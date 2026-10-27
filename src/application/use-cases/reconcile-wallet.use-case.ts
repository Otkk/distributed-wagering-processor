import { Money } from '../../domain/models/money';
import {
  WalletLedgerRepository,
  WalletRepository,
} from '../../domain/ports/repositories.port';
import { ReconcileWalletOutput } from '../dto/wallet.dto';
import { WalletNotFoundError } from './process-wager-transaction.use-case';

export class ReconcileWalletUseCase {
  constructor(
    private readonly walletRepo: WalletRepository,
    private readonly ledgerRepo: WalletLedgerRepository,
  ) {}

  public async execute(walletId: string): Promise<ReconcileWalletOutput> {
    const wallet = await this.walletRepo.findById(walletId);
    if (!wallet) {
      throw new WalletNotFoundError(walletId);
    }

    const { calculatedAmount, count } = await this.ledgerRepo.calculateBalance(walletId);

    const calculatedMoney = Money.from({
      amount: calculatedAmount,
      currency: wallet.currency,
    });

    const differenceMoney = wallet.balance.subtract(calculatedMoney);
    const isConsistent = differenceMoney.isZero();

    return {
      walletId,
      storedBalance: wallet.balance.toJSON(),
      calculatedBalance: calculatedMoney.toJSON(),
      difference: differenceMoney.toJSON(),
      consistent: isConsistent,
      checkedEntries: count,
    };
  }
}
