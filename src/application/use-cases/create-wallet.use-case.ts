import { v4 as uuidv4 } from 'uuid';
import { DomainError } from '../../domain/errors/domain.error';
import { WalletBalanceChanged } from '../../domain/events/events';
import { computePayloadHash } from '../../domain/models/canonical-hash';
import { LedgerDirection } from '../../domain/models/ledger-direction';
import { Money } from '../../domain/models/money';
import { OutboxMessage } from '../../domain/models/outbox-message';
import {
  WagerTransaction,
  WagerTransactionKind,
} from '../../domain/models/wager-transaction';
import { Wallet } from '../../domain/models/wallet';
import { WalletLedgerEntry } from '../../domain/models/wallet-ledger-entry';
import {
  OutboxRepository,
  UnitOfWork,
  WagerTransactionRepository,
  WalletLedgerRepository,
  WalletRepository,
} from '../../domain/ports/repositories.port';
import { CreateWalletInput, WalletOutput } from '../dto/wallet.dto';

export class WalletConflictError extends DomainError {
  constructor(playerId: string, currency: string) {
    super(`Wallet already exists for player ${playerId} and currency ${currency}`);
  }
}

export class CreateWalletUseCase {
  constructor(
    private readonly walletRepo: WalletRepository,
    private readonly txRepo: WagerTransactionRepository,
    private readonly ledgerRepo: WalletLedgerRepository,
    private readonly outboxRepo: OutboxRepository,
    private readonly uow: UnitOfWork,
  ) {}

  public async execute(input: CreateWalletInput): Promise<WalletOutput> {
    const initialMoney = Money.from(input.initialBalance);

    return this.uow.transactional(async () => {
      // 1. Verifica conflito de duplicidade (playerId + currency)
      const existing = await this.walletRepo.findByPlayerIdAndCurrency(
        input.playerId,
        initialMoney.currency,
      );
      if (existing) {
        throw new WalletConflictError(input.playerId, initialMoney.currency);
      }

      const walletId = uuidv4();
      const now = new Date();

      // 2. Cria a Wallet
      const wallet = Wallet.open({
        id: walletId,
        playerId: input.playerId,
        initialBalance: initialMoney,
        createdAt: now,
      });

      await this.walletRepo.save(wallet);

      // 3. Se saldo inicial > 0, cria transação interna OPENING e CREDIT no ledger
      if (initialMoney.isPositive()) {
        const txId = uuidv4();
        const openingPayload = {
          type: 'OPENING',
          walletId,
          playerId: input.playerId,
          amount: initialMoney.amount,
          currency: initialMoney.currency,
        };
        const payloadHash = computePayloadHash(openingPayload);

        const openingTx = WagerTransaction.create({
          id: txId,
          providerId: 'INTERNAL',
          externalTransactionId: `opening-${walletId}`,
          idempotencyKey: `internal:opening:${walletId}`,
          payloadHash,
          walletId,
          playerId: input.playerId,
          roundId: 'opening-round',
          gameId: 'system-wallet',
          kind: WagerTransactionKind.Opening,
          money: initialMoney,
          createdAt: now,
        });

        openingTx.markProcessed(undefined, now);
        await this.txRepo.save(openingTx);

        // Entrada correspondente no ledger
        const ledgerEntry = WalletLedgerEntry.create({
          id: uuidv4(),
          walletId,
          transactionId: txId,
          direction: LedgerDirection.Credit,
          money: initialMoney,
          balanceBefore: Money.zero(initialMoney.currency),
          balanceAfter: initialMoney,
          createdAt: now,
        });

        await this.ledgerRepo.save(ledgerEntry);

        // Evento de integração no Outbox
        const balanceEvent = new WalletBalanceChanged({
          eventId: uuidv4(),
          aggregateId: walletId,
          correlationId: `opening-${walletId}`,
          occurredAt: now,
          data: {
            walletId,
            transactionId: txId,
            direction: LedgerDirection.Credit,
            money: initialMoney.toJSON(),
            balanceBefore: Money.zero(initialMoney.currency).toJSON(),
            balanceAfter: initialMoney.toJSON(),
            walletVersion: wallet.version,
          },
        });

        await this.outboxRepo.save(OutboxMessage.enqueue(balanceEvent));
      }

      return {
        id: wallet.id,
        playerId: wallet.playerId,
        balance: wallet.balance.toJSON(),
        version: wallet.version,
      };
    });
  }
}
