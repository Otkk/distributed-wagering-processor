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
      // 2. Deduplicação de fila (Persistent Inbox)
      if (input.messageId && input.consumerName) {
        const existingInbox = await this.inboxRepo.find(input.consumerName, input.messageId);
        if (existingInbox && existingInbox.isProcessed()) {
          const wallet = await this.walletRepo.findById(input.walletId);
          return {
            transactionId: input.externalTransactionId,
            status: WagerTransactionStatus.Processed,
            balance: wallet ? wallet.balance.toJSON() : money.toJSON(),
            idempotentReplay: true,
          };
        }
      }

      // 3. Verificação de Idempotência
      const existingTx = await this.txRepo.findByIdempotencyKey(input.idempotencyKey);
      if (existingTx) {
        if (existingTx.matchesPayload(payloadHash)) {
          // Replay idêntico
          const wallet = await this.walletRepo.findById(input.walletId);
          return {
            transactionId: existingTx.id,
            status: existingTx.status,
            balance: wallet ? wallet.balance.toJSON() : money.toJSON(),
            idempotentReplay: true,
            failureCode: existingTx.failureCode,
          };
        } else {
          // Mesma chave com payload divergente = Conflito
          throw new IdempotencyConflictError(input.idempotencyKey);
        }
      }

      // 4. Concorrência: Pessimistic Lock na Wallet
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

      const now = new Date();
      const txId = uuidv4();
      const correlationId = input.idempotencyKey;

      // Cria a entidade da transação em estado PENDING
      const tx = WagerTransaction.create({
        id: txId,
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

        // 5.1 Se a referência não foi encontrada: armazena como PENDING_REFERENCE
        if (!referenceTx) {
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

          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.PendingReference,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
          };
        }

        // 5.2 Validações da referência encontrada
        if (
          referenceTx.playerId !== input.playerId ||
          referenceTx.walletId !== input.walletId ||
          referenceTx.roundId !== input.roundId ||
          referenceTx.money.currency !== money.currency
        ) {
          tx.reject(FailureCode.REFERENCE_ROUND_MISMATCH);
          await this.txRepo.save(tx);
          await this.saveRejectionOutbox(tx, correlationId, now);
          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.Rejected,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
            failureCode: FailureCode.REFERENCE_ROUND_MISMATCH,
          };
        }

        // Validação de tipo: REFUND só referencia BET
        if (input.kind === WagerTransactionKind.Refund && referenceTx.kind !== WagerTransactionKind.Bet) {
          tx.reject(FailureCode.INVALID_REFERENCE_TYPE);
          await this.txRepo.save(tx);
          await this.saveRejectionOutbox(tx, correlationId, now);
          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.Rejected,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
            failureCode: FailureCode.INVALID_REFERENCE_TYPE,
          };
        }

        // Validação de valor: montante deve ser igual ao da referência
        if (!referenceTx.money.equals(money)) {
          tx.reject(FailureCode.REFERENCE_AMOUNT_MISMATCH);
          await this.txRepo.save(tx);
          await this.saveRejectionOutbox(tx, correlationId, now);
          return {
            transactionId: tx.id,
            status: WagerTransactionStatus.Rejected,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
            failureCode: FailureCode.REFERENCE_AMOUNT_MISMATCH,
          };
        }

        // Validação de duplicidade de reversão: já foi revertida?
        // (Será verificada por busca de transações existentes com referenceTransactionId igual)
      }

      // 6. Aplicação da Movimentação Financeira
      if (input.kind === WagerTransactionKind.Bet) {
        if (wallet.balance.isLessThan(money)) {
          tx.reject(FailureCode.INSUFFICIENT_FUNDS);
          await this.txRepo.save(tx);
          await this.saveRejectionOutbox(tx, correlationId, now);
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
      if (input.messageId && input.consumerName) {
        const inbox = InboxMessage.receive({
          messageId: input.messageId,
          consumerName: input.consumerName,
          payloadHash,
          receivedAt: now,
        });
        inbox.markProcessed(now);
        await this.inboxRepo.save(inbox);
      }

      return {
        transactionId: tx.id,
        status: tx.status,
        balance: wallet.balance.toJSON(),
        idempotentReplay: false,
      };
    });
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
