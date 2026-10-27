import { InvalidLedgerEntryError } from '../errors/domain.error';
import { LedgerDirection } from './ledger-direction';
import { Money } from './money';

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt?: Date;
}

export interface LedgerEntryState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {}

  public static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    if (!props.id || !props.walletId || !props.transactionId) {
      throw new InvalidLedgerEntryError('id, walletId and transactionId are required');
    }

    props.balanceBefore.assertSameCurrency(props.money);
    props.balanceBefore.assertSameCurrency(props.balanceAfter);

    const now = props.createdAt || new Date();
    const entry = new WalletLedgerEntry(
      props.id,
      props.walletId,
      props.transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      now,
    );

    if (!entry.isBalanced()) {
      throw new InvalidLedgerEntryError(
        `Unbalanced ledger entry for wallet ${props.walletId}: before=${props.balanceBefore.amount}, ` +
          `after=${props.balanceAfter.amount}, amount=${props.money.amount}, direction=${props.direction}`,
      );
    }

    return entry;
  }

  public static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  public isBalanced(): boolean {
    if (this.direction === LedgerDirection.Debit) {
      return this.balanceBefore.subtract(this.money).equals(this.balanceAfter);
    }
    if (this.direction === LedgerDirection.Credit) {
      return this.balanceBefore.add(this.money).equals(this.balanceAfter);
    }
    return false;
  }
}
