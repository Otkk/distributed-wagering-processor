import { Money, MoneyProps } from '../../domain/models/money';
import {
  WalletLedgerRepository,
  WalletRepository,
} from '../../domain/ports/repositories.port';
import { ReconcileWalletOutput } from '../dto/wallet.dto';
import { WalletNotFoundError } from './process-wager-transaction.use-case';

export interface ReconciliationObserverPort {
  onDivergence(details: {
    walletId: string;
    storedBalance: MoneyProps;
    calculatedBalance: MoneyProps;
    difference: MoneyProps;
    checkedEntries: number;
  }): void;
}

export class ReconcileWalletUseCase {
  constructor(
    private readonly walletRepo: WalletRepository,
    private readonly ledgerRepo: WalletLedgerRepository,
    private readonly observer?: ReconciliationObserverPort,
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

    const output: ReconcileWalletOutput = {
      walletId,
      storedBalance: wallet.balance.toJSON(),
      calculatedBalance: calculatedMoney.toJSON(),
      difference: differenceMoney.toJSON(),
      consistent: isConsistent,
      checkedEntries: count,
    };

    if (!isConsistent) {
      this.observer?.onDivergence({
        walletId: output.walletId,
        storedBalance: output.storedBalance,
        calculatedBalance: output.calculatedBalance,
        difference: output.difference,
        checkedEntries: output.checkedEntries,
      });
    }

    return output;
  }
}
