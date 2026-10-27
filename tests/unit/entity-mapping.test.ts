import { describe, expect, it } from 'bun:test';
import { LedgerDirection } from '../../src/domain/models/ledger-direction';
import { Money } from '../../src/domain/models/money';
import {
  WagerTransaction,
  WagerTransactionKind,
} from '../../src/domain/models/wager-transaction';
import { Wallet } from '../../src/domain/models/wallet';
import { WalletLedgerEntry } from '../../src/domain/models/wallet-ledger-entry';
import { InboxMessage } from '../../src/domain/models/inbox-message';
import { OutboxMessage } from '../../src/domain/models/outbox-message';
import { WalletBalanceChanged } from '../../src/domain/events/events';
import { WalletEntity } from '../../src/infrastructure/database/entities/wallet.entity';
import { WagerTransactionEntity } from '../../src/infrastructure/database/entities/wager-transaction.entity';
import { WalletLedgerEntryEntity } from '../../src/infrastructure/database/entities/wallet-ledger-entry.entity';
import { InboxMessageEntity } from '../../src/infrastructure/database/entities/inbox-message.entity';
import { OutboxMessageEntity } from '../../src/infrastructure/database/entities/outbox-message.entity';

describe('MikroORM Entity Data Mappers (Domain <-> DB Entity Roundtrip)', () => {
  it('should map Wallet to WalletEntity and back to Wallet preserving invariants', () => {
    const originalWallet = Wallet.open({
      id: 'w-100',
      playerId: 'player-200',
      initialBalance: Money.from({ amount: '500.00', currency: 'BRL' }),
    });

    const entity = WalletEntity.fromDomain(originalWallet);
    expect(entity.id).toBe('w-100');
    expect(entity.balanceAmount).toBe('500.00');
    expect(entity.currency).toBe('BRL');

    const mappedBack = entity.toDomain();
    expect(mappedBack.id).toBe(originalWallet.id);
    expect(mappedBack.balance.amount).toBe('500.00');
    expect(mappedBack.version).toBe(1);
  });

  it('should map WagerTransaction to WagerTransactionEntity and back', () => {
    const tx = WagerTransaction.create({
      id: 'tx-100',
      providerId: 'prov-1',
      externalTransactionId: 'ext-1',
      idempotencyKey: 'prov-1:ext-1',
      payloadHash: 'hash-123',
      walletId: 'w-100',
      playerId: 'p-1',
      roundId: 'r-1',
      gameId: 'g-1',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '25.00', currency: 'BRL' }),
    });

    const entity = WagerTransactionEntity.fromDomain(tx);
    const mappedBack = entity.toDomain();

    expect(mappedBack.id).toBe(tx.id);
    expect(mappedBack.idempotencyKey).toBe(tx.idempotencyKey);
    expect(mappedBack.kind).toBe(WagerTransactionKind.Bet);
    expect(mappedBack.money.amount).toBe('25.00');
  });

  it('should map WalletLedgerEntry to WalletLedgerEntryEntity and back', () => {
    const entry = WalletLedgerEntry.create({
      id: 'ledger-1',
      walletId: 'w-100',
      transactionId: 'tx-100',
      direction: LedgerDirection.Debit,
      money: Money.from({ amount: '25.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '75.00', currency: 'BRL' }),
    });

    const entity = WalletLedgerEntryEntity.fromDomain(entry);
    const mappedBack = entity.toDomain();

    expect(mappedBack.isBalanced()).toBe(true);
    expect(mappedBack.balanceAfter.amount).toBe('75.00');
  });

  it('should map InboxMessage and OutboxMessage entities and back', () => {
    const inbox = InboxMessage.receive({
      messageId: 'msg-99',
      consumerName: 'wager-worker',
      payloadHash: 'hash-xyz',
    });

    const inboxEntity = InboxMessageEntity.fromDomain(inbox);
    const inboxBack = inboxEntity.toDomain();
    expect(inboxBack.messageId).toBe('msg-99');

    const event = new WalletBalanceChanged({
      eventId: 'evt-1',
      aggregateId: 'w-100',
      correlationId: 'corr-1',
      occurredAt: new Date(),
      data: {
        walletId: 'w-100',
        transactionId: 'tx-1',
        direction: LedgerDirection.Debit,
        money: { amount: '10.00', currency: 'BRL' },
        balanceBefore: { amount: '100.00', currency: 'BRL' },
        balanceAfter: { amount: '90.00', currency: 'BRL' },
        walletVersion: 2,
      },
    });

    const outbox = OutboxMessage.enqueue(event);
    const outboxEntity = OutboxMessageEntity.fromDomain(outbox);
    const outboxBack = outboxEntity.toDomain();

    expect(outboxBack.aggregateId).toBe('w-100');
    expect(outboxBack.eventType).toBe('WalletBalanceChanged');
  });
});
