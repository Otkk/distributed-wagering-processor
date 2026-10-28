import { FailureCode } from '../../domain/errors/domain.error';
import { MoneyProps } from '../../domain/models/money';
import {
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/models/wager-transaction';

export interface ProcessWagerInput {
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  // Metadata para mensageria assíncrona
  messageId?: string;
  consumerName?: string;
  inboxContext?: {
    consumerName: string;
    messageId: string;
  };
}

export interface ProcessWagerOutput {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  idempotentReplay: boolean;
  failureCode?: FailureCode;
}
