import { MoneyProps } from '../../domain/models/money';

export interface CreateWalletInput {
  playerId: string;
  initialBalance: MoneyProps;
}

export interface WalletOutput {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

export interface ReconcileWalletOutput {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
}
