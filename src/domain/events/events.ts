import { FailureCode } from '../errors/domain.error';
import { LedgerDirection } from '../models/ledger-direction';
import { MoneyProps } from '../models/money';
import { WagerTransactionKind } from '../models/wager-transaction';
import { IntegrationEvent, IntegrationEventProps } from './integration-event';

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  public readonly eventType = 'WalletBalanceChanged';
  public readonly version = 1;

  constructor(props: IntegrationEventProps<WalletBalanceChangedData>) {
    super(props);
  }
}

export interface WagerTransactionProcessedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceTransactionId?: string;
  processedAt: string;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  public readonly eventType = 'WagerTransactionProcessed';
  public readonly version = 1;

  constructor(props: IntegrationEventProps<WagerTransactionProcessedData>) {
    super(props);
  }
}

export interface WagerTransactionRejectedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  failureCode: FailureCode;
  rejectedAt: string;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  public readonly eventType = 'WagerTransactionRejected';
  public readonly version = 1;

  constructor(props: IntegrationEventProps<WagerTransactionRejectedData>) {
    super(props);
  }
}

export interface WagerTransactionPendingReferenceData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId: string;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  public readonly eventType = 'WagerTransactionPendingReference';
  public readonly version = 1;

  constructor(props: IntegrationEventProps<WagerTransactionPendingReferenceData>) {
    super(props);
  }
}
