import { InboxMessage } from '../models/inbox-message';
import { OutboxMessage } from '../models/outbox-message';
import { WagerTransaction } from '../models/wager-transaction';
import { Wallet } from '../models/wallet';
import { WalletLedgerEntry } from '../models/wallet-ledger-entry';

export interface WalletRepository {
  findById(id: string, lock?: 'pessimistic_write'): Promise<Wallet | null>;
  findByPlayerIdAndCurrency(playerId: string, currency: string): Promise<Wallet | null>;
  save(wallet: Wallet): Promise<void>;
}

export interface WagerTransactionRepository {
  findById(id: string): Promise<WagerTransaction | null>;
  findByIdempotencyKey(key: string): Promise<WagerTransaction | null>;
  findByProviderAndExternalId(providerId: string, externalId: string): Promise<WagerTransaction | null>;
  findPendingReferences(limit: number): Promise<WagerTransaction[]>;
  findProcessedReversal?(referenceTransactionId: string, kind: string): Promise<WagerTransaction | null>;
  save(transaction: WagerTransaction): Promise<void>;
}

export interface LedgerPaginationResult {
  entries: WalletLedgerEntry[];
  nextCursor?: string;
}

export interface WalletLedgerRepository {
  save(entry: WalletLedgerEntry): Promise<void>;
  findByWalletId(
    walletId: string,
    options?: { limit: number; cursor?: string },
  ): Promise<LedgerPaginationResult>;
  calculateBalance(walletId: string): Promise<{ calculatedAmount: string; count: number }>;
}

export interface InboxRepository {
  find(consumerName: string, messageId: string): Promise<InboxMessage | null>;
  save(inbox: InboxMessage): Promise<void>;
}

export interface OutboxRepository {
  save(outbox: OutboxMessage): Promise<void>;
  findPendingToDispatch(limit: number, now: Date): Promise<OutboxMessage[]>;
}

export interface UnitOfWork {
  transactional<T>(fn: () => Promise<T>): Promise<T>;
}
