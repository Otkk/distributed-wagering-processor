import { CurrencyMismatchError, InsufficientBalanceError } from '../errors/domain.error';
import { Money } from './money';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  createdAt?: Date;
}

export interface BalanceChangeResult {
  previousBalance: Money;
  newBalance: Money;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  public static open(props: {
    id: string;
    playerId: string;
    initialBalance: Money;
    createdAt?: Date;
  }): Wallet {
    if (!props.id || !props.playerId || !props.initialBalance) {
      throw new Error('Wallet id, playerId and initialBalance are required');
    }

    const now = props.createdAt || new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1, // version starts at 1
      now,
      now,
    );
  }

  public static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  public get balance(): Money {
    return this._balance;
  }

  public get version(): number {
    return this._version;
  }

  public get updatedAt(): Date {
    return this._updatedAt;
  }

  public debit(money: Money, at?: Date): BalanceChangeResult {
    this.assertSameCurrency(money);

    if (this._balance.isLessThan(money)) {
      throw new InsufficientBalanceError(this.id, this._balance.amount, money.amount);
    }

    const previousBalance = this._balance;
    const newBalance = this._balance.subtract(money);

    this._balance = newBalance;
    this._version += 1;
    this._updatedAt = at || new Date();

    return { previousBalance, newBalance };
  }

  public credit(money: Money, at?: Date): BalanceChangeResult {
    this.assertSameCurrency(money);

    const previousBalance = this._balance;
    const newBalance = this._balance.add(money);

    this._balance = newBalance;
    this._version += 1;
    this._updatedAt = at || new Date();

    return { previousBalance, newBalance };
  }

  private assertSameCurrency(money: Money): void {
    if (this.currency !== money.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
