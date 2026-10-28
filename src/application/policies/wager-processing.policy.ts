import { v4 as uuidv4 } from 'uuid';
import { FailureCode } from '../../domain/errors/domain.error';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../../domain/events/events';
import { computePayloadHash } from '../../domain/models/canonical-hash';
import { Money } from '../../domain/models/money';
import { OutboxMessage } from '../../domain/models/outbox-message';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/models/wager-transaction';
import { WalletLedgerEntry } from '../../domain/models/wallet-ledger-entry';
import { ProcessWagerInput } from '../dto/wager.dto';

export function computeWagerPayloadHash(input: ProcessWagerInput, money: Money): string {
  return computePayloadHash({
    providerId: input.providerId,
    externalTransactionId: input.externalTransactionId,
    playerId: input.playerId,
    walletId: input.walletId,
    roundId: input.roundId,
    gameId: input.gameId,
    kind: input.kind,
    money: {
      amount: money.amount,
      currency: money.currency,
    },
    referenceExternalTransactionId: input.referenceExternalTransactionId || undefined,
  });
}

export function validateReferenceConstraints(
  input: ProcessWagerInput,
  money: Money,
  referenceTx: WagerTransaction,
): FailureCode | null {
  if (referenceTx.status !== WagerTransactionStatus.Processed) {
    return FailureCode.INVALID_REFERENCE_TYPE;
  }

  if (
    referenceTx.playerId !== input.playerId ||
    referenceTx.walletId !== input.walletId ||
    referenceTx.roundId !== input.roundId ||
    referenceTx.money.currency !== money.currency
  ) {
    return FailureCode.REFERENCE_ROUND_MISMATCH;
  }

  const isInvalidRefundRef =
    input.kind === WagerTransactionKind.Refund &&
    referenceTx.kind !== WagerTransactionKind.Bet;
  const isInvalidRollbackRef =
    input.kind === WagerTransactionKind.Rollback &&
    referenceTx.kind !== WagerTransactionKind.Bet &&
    referenceTx.kind !== WagerTransactionKind.Win &&
    referenceTx.kind !== WagerTransactionKind.Refund;

  if (isInvalidRefundRef || isInvalidRollbackRef) {
    return FailureCode.INVALID_REFERENCE_TYPE;
  }

  if (!referenceTx.money.equals(money)) {
    return FailureCode.REFERENCE_AMOUNT_MISMATCH;
  }

  return null;
}

export function buildPendingReferenceOutboxMessage(
  tx: WagerTransaction,
  referenceExternalTransactionId: string,
  correlationId: string,
  now: Date,
): OutboxMessage {
  return OutboxMessage.enqueue(
    new WagerTransactionPendingReference({
      eventId: uuidv4(),
      aggregateId: tx.id,
      correlationId,
      occurredAt: now,
      data: {
        transactionId: tx.id,
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        walletId: tx.walletId,
        playerId: tx.playerId,
        roundId: tx.roundId,
        gameId: tx.gameId,
        kind: tx.kind,
        money: tx.money.toJSON(),
        referenceExternalTransactionId,
      },
    }),
  );
}

export function buildSuccessOutboxMessages(
  tx: WagerTransaction,
  wallet: { id: string; version: number; balance: Money },
  ledgerEntry: WalletLedgerEntry,
  correlationId: string,
  now: Date,
): [OutboxMessage, OutboxMessage] {
  const balanceChangedEvent = new WalletBalanceChanged({
    eventId: uuidv4(),
    aggregateId: wallet.id,
    correlationId,
    occurredAt: now,
    data: {
      walletId: wallet.id,
      transactionId: tx.id,
      direction: ledgerEntry.direction,
      money: ledgerEntry.money.toJSON(),
      balanceBefore: ledgerEntry.balanceBefore.toJSON(),
      balanceAfter: ledgerEntry.balanceAfter.toJSON(),
      walletVersion: wallet.version,
    },
  });

  const processedEvent = new WagerTransactionProcessed({
    eventId: uuidv4(),
    aggregateId: tx.id,
    correlationId,
    occurredAt: now,
    data: {
      transactionId: tx.id,
      providerId: tx.providerId,
      externalTransactionId: tx.externalTransactionId,
      walletId: tx.walletId,
      playerId: tx.playerId,
      roundId: tx.roundId,
      gameId: tx.gameId,
      kind: tx.kind,
      money: tx.money.toJSON(),
      referenceTransactionId: tx.referenceTransactionId,
      processedAt: now.toISOString(),
    },
  });

  return [OutboxMessage.enqueue(balanceChangedEvent), OutboxMessage.enqueue(processedEvent)];
}

export function buildLossProcessedOutboxMessage(
  tx: WagerTransaction,
  correlationId: string,
  now: Date,
): OutboxMessage {
  return OutboxMessage.enqueue(
    new WagerTransactionProcessed({
      eventId: uuidv4(),
      aggregateId: tx.id,
      correlationId,
      occurredAt: now,
      data: {
        transactionId: tx.id,
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        walletId: tx.walletId,
        playerId: tx.playerId,
        roundId: tx.roundId,
        gameId: tx.gameId,
        kind: tx.kind,
        money: tx.money.toJSON(),
        referenceTransactionId: undefined,
        processedAt: now.toISOString(),
      },
    }),
  );
}

export function buildRejectionOutboxMessage(
  tx: WagerTransaction,
  correlationId: string,
  now: Date,
): OutboxMessage {
  return OutboxMessage.enqueue(
    new WagerTransactionRejected({
      eventId: uuidv4(),
      aggregateId: tx.id,
      correlationId,
      occurredAt: now,
      data: {
        transactionId: tx.id,
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        walletId: tx.walletId,
        playerId: tx.playerId,
        roundId: tx.roundId,
        gameId: tx.gameId,
        kind: tx.kind,
        money: tx.money.toJSON(),
        failureCode: tx.failureCode || FailureCode.INSUFFICIENT_FUNDS,
        rejectedAt: now.toISOString(),
      },
    }),
  );
}
