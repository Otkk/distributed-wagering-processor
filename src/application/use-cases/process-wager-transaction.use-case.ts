import { v4 as uuidv4 } from 'uuid';
import {
  CurrencyMismatchError,
  DomainError,
  FailureCode,
  IdempotencyConflictError,
} from '../../domain/errors/domain.error';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../../domain/events/events';
import { computePayloadHash } from '../../domain/models/canonical-hash';
import { InboxMessage } from '../../domain/models/inbox-message';
import { LedgerDirection } from '../../domain/models/ledger-direction';
import { Money } from '../../domain/models/money';
import { OutboxMessage } from '../../domain/models/outbox-message';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/models/wager-transaction';
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

    // 1. Hash canônico determinístico
    const canonicalPayload = {
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
    };
    const payloadHash = computePayloadHash(canonicalPayload);

    return this.uow.transactional(async () => {
      const msgId = input.messageId ?? input.inboxContext?.messageId;
      const consName = input.consumerName ?? input.inboxContext?.consumerName;

      // 2. Concorrência: Pessimistic Lock na Wallet (Unidade de concorrência é a walletId)
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

      // 3. Deduplicação de fila (Persistent Inbox) sob o lock da carteira
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

      // 4. Verificação de Idempotência sob o Lock da Carteira
      const existingTx = await this.txRepo.findByIdempotencyKey(input.idempotencyKey);
      if (existingTx) {
        if (!existingTx.matchesPayload(payloadHash)) {
          // Mesma chave com payload divergente = Conflito
          throw new IdempotencyConflictError(input.idempotencyKey);
        }
        if (existingTx.isTerminal()) {
          // Replay idêntico de transação terminal
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
        // Verifica unicidade de (providerId, externalTransactionId) caso enviem outra Idempotency-Key
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

      // Cria a entidade da transação em estado PENDING (ou reutiliza a existente em PENDING_REFERENCE)
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

      // 5. Resolução de Referências (REFUND e ROLLBACK)
      let referenceTx: WagerTransaction | null = null;
      if (tx.requiresReference()) {
        referenceTx = await this.txRepo.findByProviderAndExternalId(
          input.providerId,
          input.referenceExternalTransactionId!,
        );

        // 5.1 Se a referência não foi encontrada (ou ainda não está finalizada): armazena como PENDING_REFERENCE
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

          const pendingEvent = new WagerTransactionPendingReference({
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
              referenceExternalTransactionId: input.referenceExternalTransactionId!,
            },
          });
          await this.outboxRepo.save(OutboxMessage.enqueue(pendingEvent));
          await this.recordInboxIfPresent(input, payloadHash, now);

          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.PendingReference,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
          };
        }

        // 5.2 A referência precisa estar PROCESSED para ser revertida
        if (referenceTx.status !== WagerTransactionStatus.Processed) {
          tx.reject(FailureCode.INVALID_REFERENCE_TYPE);
          await this.txRepo.save(tx);
          await this.saveRejectionOutbox(tx, correlationId, now);
          await this.recordInboxIfPresent(input, payloadHash, now);
          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.Rejected,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
            failureCode: FailureCode.INVALID_REFERENCE_TYPE,
          };
        }

        // 5.3 Validações de titularidade, carteira, rodada e moeda da referência
        if (
          referenceTx.playerId !== input.playerId ||
          referenceTx.walletId !== input.walletId ||
          referenceTx.roundId !== input.roundId ||
          referenceTx.money.currency !== money.currency
        ) {
          tx.reject(FailureCode.REFERENCE_ROUND_MISMATCH);
          await this.txRepo.save(tx);
          await this.saveRejectionOutbox(tx, correlationId, now);
          await this.recordInboxIfPresent(input, payloadHash, now);
          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.Rejected,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
            failureCode: FailureCode.REFERENCE_ROUND_MISMATCH,
          };
        }

        // 5.4 Validação de tipo: REFUND só referencia BET; ROLLBACK referencia BET, WIN ou REFUND
        const isInvalidRefundRef =
          input.kind === WagerTransactionKind.Refund &&
          referenceTx.kind !== WagerTransactionKind.Bet;
        const isInvalidRollbackRef =
          input.kind === WagerTransactionKind.Rollback &&
          referenceTx.kind !== WagerTransactionKind.Bet &&
          referenceTx.kind !== WagerTransactionKind.Win &&
          referenceTx.kind !== WagerTransactionKind.Refund;

        if (isInvalidRefundRef || isInvalidRollbackRef) {
          tx.reject(FailureCode.INVALID_REFERENCE_TYPE);
          await this.txRepo.save(tx);
          await this.saveRejectionOutbox(tx, correlationId, now);
          await this.recordInboxIfPresent(input, payloadHash, now);
          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.Rejected,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
            failureCode: FailureCode.INVALID_REFERENCE_TYPE,
          };
        }

        // 5.5 Validação de valor: montante deve ser igual ao da referência
        if (!referenceTx.money.equals(money)) {
          tx.reject(FailureCode.REFERENCE_AMOUNT_MISMATCH);
          await this.txRepo.save(tx);
          await this.saveRejectionOutbox(tx, correlationId, now);
          await this.recordInboxIfPresent(input, payloadHash, now);
          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.Rejected,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
            failureCode: FailureCode.REFERENCE_AMOUNT_MISMATCH,
          };
        }

        // 5.6 Validação de duplicidade de reversão: referência não pode ser revertida duas vezes pelo mesmo tipo
        if (this.txRepo.findProcessedReversal) {
          const existingReversal = await this.txRepo.findProcessedReversal(
            referenceTx.id,
            input.kind,
          );
          if (existingReversal && existingReversal.id !== tx.id) {
            tx.reject(FailureCode.REFERENCE_ALREADY_REVERSED);
            await this.txRepo.save(tx);
            await this.saveRejectionOutbox(tx, correlationId, now);
            await this.recordInboxIfPresent(input, payloadHash, now);
            return {
              transactionId: tx.id,
              status: WagerTransactionStatus.Rejected,
              balance: wallet.balance.toJSON(),
              idempotentReplay: false,
              failureCode: FailureCode.REFERENCE_ALREADY_REVERSED,
            };
          }
        }
      }

      // 6. Aplicação da Movimentação Financeira
      if (input.kind === WagerTransactionKind.Bet) {
        if (wallet.balance.isLessThan(money)) {
          tx.reject(FailureCode.INSUFFICIENT_FUNDS);
          await this.txRepo.save(tx);
          await this.saveRejectionOutbox(tx, correlationId, now);
          await this.recordInboxIfPresent(input, payloadHash, now);
          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.Rejected,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
            failureCode: FailureCode.INSUFFICIENT_FUNDS,
          };
        }

        const { previousBalance, newBalance } = wallet.debit(money, now);
        await this.walletRepo.save(wallet);

        const ledgerEntry = WalletLedgerEntry.create({
          id: uuidv4(),
          walletId: wallet.id,
          transactionId: tx.id,
          direction: LedgerDirection.Debit,
          money,
          balanceBefore: previousBalance,
          balanceAfter: newBalance,
          createdAt: now,
        });
        await this.ledgerRepo.save(ledgerEntry);

        tx.markProcessed(undefined, now);
        await this.txRepo.save(tx);

        await this.saveSuccessOutbox(tx, wallet, ledgerEntry, correlationId, now);
      } else if (input.kind === WagerTransactionKind.Win || input.kind === WagerTransactionKind.Refund) {
        const { previousBalance, newBalance } = wallet.credit(money, now);
        await this.walletRepo.save(wallet);

        const ledgerEntry = WalletLedgerEntry.create({
          id: uuidv4(),
          walletId: wallet.id,
          transactionId: tx.id,
          direction: LedgerDirection.Credit,
          money,
          balanceBefore: previousBalance,
          balanceAfter: newBalance,
          createdAt: now,
        });
        await this.ledgerRepo.save(ledgerEntry);

        tx.markProcessed(referenceTx?.id, now);
        await this.txRepo.save(tx);

        await this.saveSuccessOutbox(tx, wallet, ledgerEntry, correlationId, now);
      } else if (input.kind === WagerTransactionKind.Rollback) {
        const direction = tx.ledgerDirectionFor(referenceTx!);

        if (direction === LedgerDirection.Debit) {
          // Reversão exigindo débito (ex: revertendo WIN)
          if (wallet.balance.isLessThan(money)) {
            tx.reject(FailureCode.NEGATIVE_BALANCE_PREVENTED);
            await this.txRepo.save(tx);
            await this.saveRejectionOutbox(tx, correlationId, now);
            await this.recordInboxIfPresent(input, payloadHash, now);
            return {
              transactionId: tx.id,
              status: WagerTransactionStatus.Rejected,
              balance: wallet.balance.toJSON(),
              idempotentReplay: false,
              failureCode: FailureCode.NEGATIVE_BALANCE_PREVENTED,
            };
          }

          const { previousBalance, newBalance } = wallet.debit(money, now);
          await this.walletRepo.save(wallet);

          const ledgerEntry = WalletLedgerEntry.create({
            id: uuidv4(),
            walletId: wallet.id,
            transactionId: tx.id,
            direction: LedgerDirection.Debit,
            money,
            balanceBefore: previousBalance,
            balanceAfter: newBalance,
            createdAt: now,
          });
          await this.ledgerRepo.save(ledgerEntry);

          tx.markProcessed(referenceTx!.id, now);
          await this.txRepo.save(tx);

          await this.saveSuccessOutbox(tx, wallet, ledgerEntry, correlationId, now);
        } else {
          // Reversão exigindo crédito (ex: revertendo BET)
          const { previousBalance, newBalance } = wallet.credit(money, now);
          await this.walletRepo.save(wallet);

          const ledgerEntry = WalletLedgerEntry.create({
            id: uuidv4(),
            walletId: wallet.id,
            transactionId: tx.id,
            direction: LedgerDirection.Credit,
            money,
            balanceBefore: previousBalance,
            balanceAfter: newBalance,
            createdAt: now,
          });
          await this.ledgerRepo.save(ledgerEntry);

          tx.markProcessed(referenceTx!.id, now);
          await this.txRepo.save(tx);

          await this.saveSuccessOutbox(tx, wallet, ledgerEntry, correlationId, now);
        }
      } else if (input.kind === WagerTransactionKind.Loss) {
        // LOSS não move saldo nem cria ledger
        tx.markProcessed(undefined, now);
        await this.txRepo.save(tx);

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
            referenceTransactionId: undefined,
            processedAt: now.toISOString(),
          },
        });
        await this.outboxRepo.save(OutboxMessage.enqueue(processedEvent));
      }

      // 7. Registra Inbox se a mensagem veio de mensageria
      await this.recordInboxIfPresent(input, payloadHash, now);

      return {
        transactionId: tx.id,
        status: tx.status,
        balance: wallet.balance.toJSON(),
        idempotentReplay: false,
      };
    });
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

  private async saveSuccessOutbox(
    tx: WagerTransaction,
    wallet: { id: string; version: number; balance: Money },
    ledgerEntry: WalletLedgerEntry,
    correlationId: string,
    now: Date,
  ): Promise<void> {
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

    await this.outboxRepo.save(OutboxMessage.enqueue(balanceChangedEvent));
    await this.outboxRepo.save(OutboxMessage.enqueue(processedEvent));
  }

  private async saveRejectionOutbox(
    tx: WagerTransaction,
    correlationId: string,
    now: Date,
  ): Promise<void> {
    const rejectedEvent = new WagerTransactionRejected({
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
        failureCode: tx.failureCode!,
        rejectedAt: now.toISOString(),
      },
    });

    await this.outboxRepo.save(OutboxMessage.enqueue(rejectedEvent));
  }
}
