import { describe, expect, it } from 'bun:test';
import { WalletBalanceChanged } from '../../src/domain/events/events';
import { LedgerDirection } from '../../src/domain/models/ledger-direction';
import { InboxMessage } from '../../src/domain/models/inbox-message';
import { OutboxMessage } from '../../src/domain/models/outbox-message';

describe('Messaging Domain Entities', () => {
  it('should track InboxMessage reception and processing state', () => {
    const inbox = InboxMessage.receive({
      messageId: 'msg-1',
      consumerName: 'wagering-consumer',
      payloadHash: 'hash-abc',
    });

    expect(inbox.isProcessed()).toBe(false);
    expect(inbox.messageId).toBe('msg-1');

    const now = new Date();
    inbox.markProcessed(now);

    expect(inbox.isProcessed()).toBe(true);
    expect(inbox.processedAt).toBe(now);
  });

  it('should create OutboxMessage from IntegrationEvent and handle retries with backoff', () => {
    const now = new Date();
    const event = new WalletBalanceChanged({
      eventId: 'evt-1',
      aggregateId: 'wallet-1',
      correlationId: 'corr-1',
      occurredAt: now,
      data: {
        walletId: 'wallet-1',
        transactionId: 'tx-1',
        direction: LedgerDirection.Debit,
        money: { amount: '25.00', currency: 'BRL' },
        balanceBefore: { amount: '100.00', currency: 'BRL' },
        balanceAfter: { amount: '75.00', currency: 'BRL' },
        walletVersion: 2,
      },
    });

    const outbox = OutboxMessage.enqueue(event);

    expect(outbox.aggregateId).toBe('wallet-1');
    expect(outbox.eventType).toBe('WalletBalanceChanged');
    expect(outbox.attempts).toBe(0);
    expect(outbox.isPending()).toBe(true);
    expect(outbox.isDue(now)).toBe(true);

    // Schedule retry
    outbox.scheduleRetry(now);
    expect(outbox.attempts).toBe(1);
    expect(outbox.nextAttemptAt).toBeDefined();
    expect(outbox.nextAttemptAt!.getTime()).toBeGreaterThan(now.getTime());

    // Mark published
    outbox.markPublished(new Date());
    expect(outbox.isPending()).toBe(false);
    expect(outbox.isDue(now)).toBe(false);
  });
});
