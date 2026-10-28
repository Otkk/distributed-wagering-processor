import { v4 as uuidv4 } from 'uuid';
import {
  CurrencyMismatchError,
  DomainError,
  FailureCode,
  IdempotencyConflictError,
} from '../../domain/errors/domain.error';
import { InboxMessage } from '../../domain/models/inbox-message';
import { LedgerDirection } from '../../domain/models/ledger-direction';
import { Money } from '../../domain/models/money';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/models/wager-transaction';
import { Wallet } from '../../domain/models/wallet';
import { WalletLedgerEntry } from '../../domain/models/wallet-ledger-entry';
import {
  InboxRepository,
  OutboxRepository,
  UnitOfWork,
  WagerTransactionRepository,
  WalletLedgerRepository,
  WalletRepository,
} from '../../domain/ports/repositories.port';
import { ProcessWagerInput, ProcessWagerOutput } from '../dto/wager.dto';
import {
  buildLossProcessedOutboxMessage,
  buildPendingReferenceOutboxMessage,
  buildRejectionOutboxMessage,
  buildSuccessOutboxMessages,
  computeWagerPayloadHash,
  validateReferenceConstraints,
} from '../policies/wager-processing.policy';

export class WalletNotFoundError extends DomainError {
  constructor(walletId: string) {
    super(`Wallet with id ${walletId} not found`);
  }
}

export class ProcessWagerTransactionUseCase {
  constructor(
    private readonly walletRepo: WalletRepository,
    private readonly txRepo: WagerTransactionRepository,
    private readonly ledgerRepo: WalletLedgerRepository,
    private readonly outboxRepo: OutboxRepository,
    private readonly inboxRepo: InboxRepository,
    private readonly uow: UnitOfWork,
  ) {}

  public async execute(input: ProcessWagerInput): Promise<ProcessWagerOutput> {
    if (input.kind === WagerTransactionKind.Opening) {
      throw new DomainError('OPENING transactions are internal and cannot be submitted externally');
    }

    const money = Money.from(input.money);
    const payloadHash = computeWagerPayloadHash(input, money);

    return this.uow.transactional(async () => {
      const msgId = input.messageId ?? input.inboxContext?.messageId;
      const consName = input.consumerName ?? input.inboxContext?.consumerName;

      // 1. Concorrência: Pessimistic Lock na Wallet (Unidade de concorrência é a walletId)
      const wallet = await this.walletRepo.findById(input.walletId, 'pessimistic_write');
      if (!wallet) {
        throw new WalletNotFoundError(input.walletId);
      }
      if (wallet.playerId !== input.playerId) {
        throw new DomainError(`Player ${input.playerId} does not own wallet ${input.walletId}`);
      }
      if (wallet.currency !== money.currency) {
        throw new CurrencyMismatchError(wallet.currency, money.currency);
      }

      // 2. Deduplicação de fila (Persistent Inbox) sob o lock da carteira
      if (msgId && consName) {
        const existingInbox = await this.inboxRepo.find(consName, msgId);
        if (existingInbox) {
          if (!existingInbox.matchesPayload(payloadHash)) {
            throw new IdempotencyConflictError(`${consName}:${msgId}`);
          }
          if (existingInbox.isProcessed()) {
            const txForInbox = await this.txRepo.findByIdempotencyKey(input.idempotencyKey);
            return {
              transactionId: txForInbox ? txForInbox.id : input.externalTransactionId,
              status: txForInbox ? txForInbox.status : WagerTransactionStatus.Processed,
              balance: wallet.balance.toJSON(),
              idempotentReplay: true,
              failureCode: txForInbox?.failureCode,
            };
          }
        }
      }

      // 3. Verificação de Idempotência sob o Lock da Carteira
      const existingTx = await this.txRepo.findByIdempotencyKey(input.idempotencyKey);
      if (existingTx) {
        if (!existingTx.matchesPayload(payloadHash)) {
          throw new IdempotencyConflictError(input.idempotencyKey);
        }
        if (existingTx.isTerminal()) {
          await this.recordInboxIfPresent(input, payloadHash, new Date());
          return {
            transactionId: existingTx.id,
            status: existingTx.status,
            balance: wallet.balance.toJSON(),
            idempotentReplay: true,
            failureCode: existingTx.failureCode,
          };
        }
      } else {
        const existingByExternal = await this.txRepo.findByProviderAndExternalId(
          input.providerId,
          input.externalTransactionId,
        );
        if (existingByExternal) {
          throw new IdempotencyConflictError(
            `${input.providerId}:${input.externalTransactionId}`,
          );
        }
      }

      const now = new Date();
      const correlationId = input.idempotencyKey;
      const isResumingPending =
        existingTx !== null && existingTx.status === WagerTransactionStatus.PendingReference;

      const tx =
        existingTx ??
        WagerTransaction.create({
          id: uuidv4(),
          providerId: input.providerId,
          externalTransactionId: input.externalTransactionId,
          idempotencyKey: input.idempotencyKey,
          payloadHash,
          walletId: input.walletId,
          playerId: input.playerId,
          roundId: input.roundId,
          gameId: input.gameId,
          kind: input.kind,
          money,
          referenceExternalTransactionId: input.referenceExternalTransactionId,
          createdAt: now,
        });

      // 4. Resolução de Referências (REFUND e ROLLBACK)
      let referenceTx: WagerTransaction | null = null;
      if (tx.requiresReference()) {
        referenceTx = await this.txRepo.findByProviderAndExternalId(
          input.providerId,
          input.referenceExternalTransactionId!,
        );

        if (
          !referenceTx ||
          referenceTx.status === WagerTransactionStatus.Pending ||
          referenceTx.status === WagerTransactionStatus.PendingReference
        ) {
          if (isResumingPending) {
            await this.recordInboxIfPresent(input, payloadHash, now);
            return {
              transactionId: tx.id,
              status: WagerTransactionStatus.PendingReference,
              balance: wallet.balance.toJSON(),
              idempotentReplay: true,
            };
          }

          tx.markPendingReference();
          await this.txRepo.save(tx);
          await this.outboxRepo.save(
            buildPendingReferenceOutboxMessage(
              tx,
              input.referenceExternalTransactionId!,
              correlationId,
              now,
            ),
          );
          await this.recordInboxIfPresent(input, payloadHash, now);

          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.PendingReference,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
          };
        }

        const refFailure = validateReferenceConstraints(input, money, referenceTx);
        if (refFailure) {
          return this.rejectAndPersist(tx, wallet, refFailure, input, payloadHash, correlationId, now);
        }

        if (this.txRepo.findProcessedReversal) {
          const existingReversal = await this.txRepo.findProcessedReversal(
            referenceTx.id,
            input.kind,
          );
          if (existingReversal && existingReversal.id !== tx.id) {
            return this.rejectAndPersist(
              tx,
              wallet,
              FailureCode.REFERENCE_ALREADY_REVERSED,
              input,
              payloadHash,
              correlationId,
              now,
            );
          }
        }
      }

      // 5. Aplicação da Movimentação Financeira
      if (input.kind === WagerTransactionKind.Bet) {
        if (wallet.balance.isLessThan(money)) {
          return this.rejectAndPersist(
            tx,
            wallet,
            FailureCode.INSUFFICIENT_FUNDS,
            input,
            payloadHash,
            correlationId,
            now,
          );
        }
        await this.applyLedgerMovement(
          tx,
          wallet,
          LedgerDirection.Debit,
          money,
          undefined,
          correlationId,
          now,
        );
      } else if (input.kind === WagerTransactionKind.Win || input.kind === WagerTransactionKind.Refund) {
        await this.applyLedgerMovement(
          tx,
          wallet,
          LedgerDirection.Credit,
          money,
          referenceTx?.id,
          correlationId,
          now,
        );
      } else if (input.kind === WagerTransactionKind.Rollback) {
        const direction = tx.ledgerDirectionFor(referenceTx!);
        if (direction === LedgerDirection.Debit && wallet.balance.isLessThan(money)) {
          return this.rejectAndPersist(
            tx,
            wallet,
            FailureCode.NEGATIVE_BALANCE_PREVENTED,
            input,
            payloadHash,
            correlationId,
            now,
          );
        }
        await this.applyLedgerMovement(
          tx,
          wallet,
          direction,
          money,
          referenceTx!.id,
          correlationId,
          now,
        );
      } else if (input.kind === WagerTransactionKind.Loss) {
        tx.markProcessed(undefined, now);
        await this.txRepo.save(tx);
        await this.outboxRepo.save(buildLossProcessedOutboxMessage(tx, correlationId, now));
      }

      await this.recordInboxIfPresent(input, payloadHash, now);

      return {
        transactionId: tx.id,
        status: tx.status,
        balance: wallet.balance.toJSON(),
        idempotentReplay: false,
      };
    });
  }

  private async applyLedgerMovement(
    tx: WagerTransaction,
    wallet: Wallet,
    direction: LedgerDirection,
    money: Money,
    referenceTxId: string | undefined,
    correlationId: string,
    now: Date,
  ): Promise<void> {
    const { previousBalance, newBalance } =
      direction === LedgerDirection.Debit
        ? wallet.debit(money, now)
        : wallet.credit(money, now);

    await this.walletRepo.save(wallet);

    // Persiste a transação ANTES do ledger para respeitar a FK wallet_ledger_entries(transaction_id)
    tx.markProcessed(referenceTxId, now);
    await this.txRepo.save(tx);

    const ledgerEntry = WalletLedgerEntry.create({
      id: uuidv4(),
      walletId: wallet.id,
      transactionId: tx.id,
      direction,
      money,
      balanceBefore: previousBalance,
      balanceAfter: newBalance,
      createdAt: now,
    });
    await this.ledgerRepo.save(ledgerEntry);

    const [balanceMsg, processedMsg] = buildSuccessOutboxMessages(
      tx,
      wallet,
      ledgerEntry,
      correlationId,
      now,
    );
    await this.outboxRepo.save(balanceMsg);
    await this.outboxRepo.save(processedMsg);
  }

  private async rejectAndPersist(
    tx: WagerTransaction,
    wallet: Wallet,
    failureCode: FailureCode,
    input: ProcessWagerInput,
    payloadHash: string,
    correlationId: string,
    now: Date,
  ): Promise<ProcessWagerOutput> {
    tx.reject(failureCode);
    await this.txRepo.save(tx);
    await this.outboxRepo.save(buildRejectionOutboxMessage(tx, correlationId, now));
    await this.recordInboxIfPresent(input, payloadHash, now);
    return {
      transactionId: tx.id,
      status: WagerTransactionStatus.Rejected,
      balance: wallet.balance.toJSON(),
      idempotentReplay: false,
      failureCode,
    };
  }

  private async recordInboxIfPresent(
    input: ProcessWagerInput,
    payloadHash: string,
    now: Date,
  ): Promise<void> {
    const msgId = input.messageId ?? input.inboxContext?.messageId;
    const consName = input.consumerName ?? input.inboxContext?.consumerName;
    if (msgId && consName) {
      const inbox = InboxMessage.receive({
        messageId: msgId,
        consumerName: consName,
        payloadHash,
        receivedAt: now,
      });
      inbox.markProcessed(now);
      await this.inboxRepo.save(inbox);
    }
  }
}
