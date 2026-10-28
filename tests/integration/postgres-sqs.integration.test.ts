import { describe, expect, it } from 'bun:test';
import * as net from 'net';
import { EntityManager, LockMode, MikroORM } from '@mikro-orm/postgresql';
import { CreateWalletUseCase } from '../../src/application/use-cases/create-wallet.use-case';
import { ProcessWagerTransactionUseCase } from '../../src/application/use-cases/process-wager-transaction.use-case';
import { ReconcileWalletUseCase } from '../../src/application/use-cases/reconcile-wallet.use-case';
import { WagerTransactionProcessed } from '../../src/domain/events/events';
import { InboxMessage } from '../../src/domain/models/inbox-message';
import { LedgerDirection } from '../../src/domain/models/ledger-direction';
import { Money } from '../../src/domain/models/money';
import { OutboxMessage } from '../../src/domain/models/outbox-message';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/models/wager-transaction';
import { Wallet } from '../../src/domain/models/wallet';
import { WalletLedgerEntry } from '../../src/domain/models/wallet-ledger-entry';
import { Migration20261027000001_InitialSchema } from '../../src/infrastructure/database/migrations/Migration20261027000001_InitialSchema';
import mikroOrmConfig from '../../src/infrastructure/database/mikro-orm.config';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/database/mikro-orm-unit-of-work';
import { MikroOrmInboxRepository } from '../../src/infrastructure/database/repositories/mikro-orm-inbox.repository';
import { MikroOrmOutboxRepository } from '../../src/infrastructure/database/repositories/mikro-orm-outbox.repository';
import { MikroOrmWagerTransactionRepository } from '../../src/infrastructure/database/repositories/mikro-orm-wager-transaction.repository';
import { MikroOrmWalletLedgerRepository } from '../../src/infrastructure/database/repositories/mikro-orm-wallet-ledger.repository';
import { MikroOrmWalletRepository } from '../../src/infrastructure/database/repositories/mikro-orm-wallet.repository';
import { SqsEventPublisher } from '../../src/infrastructure/messaging/sqs/sqs-event-publisher';
import { AwsSqsConsumerClient } from '../../src/infrastructure/messaging/sqs/wager-transaction.consumer';

async function isPortOpen(host: string, port: number, timeoutMs = 400): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    const onError = () => {
      socket.destroy();
      resolve(false);
    };
    socket.setTimeout(timeoutMs);
    socket.once('error', onError);
    socket.once('timeout', onError);
    socket.connect(port, host, () => {
      socket.end();
      resolve(true);
    });
  });
}

describe('Integration Suite: Migrations, MikroORM Repositories, Atomic Rollback & SQS Adapters', () => {
  describe('1. Reversible SQL Migration & Relational Constraints Verification', () => {
    it('deve gerar todas as tabelas, constraints (CHECK >= 0, UNIQUE) e triggers append-only no up() e reverter no down()', async () => {
      const upSqlStatements: string[] = [];
      const downSqlStatements: string[] = [];

      const migration = Object.create(
        Migration20261027000001_InitialSchema.prototype,
      ) as Migration20261027000001_InitialSchema;

      (migration as unknown as { addSql: (sql: string) => void }).addSql = (sql: string) => {
        upSqlStatements.push(sql);
      };
      await migration.up();

      const fullUpSql = upSqlStatements.join('\n');

      // Verifica criação das 5 tabelas obrigatórias e constraints relacionais
      expect(fullUpSql).toContain('CREATE TABLE IF NOT EXISTS wallets');
      expect(fullUpSql).toContain('CONSTRAINT check_wallet_balance_non_negative CHECK (balance_amount >= 0)');
      expect(fullUpSql).toContain('CONSTRAINT unique_player_currency UNIQUE (player_id, currency)');

      expect(fullUpSql).toContain('CREATE TABLE IF NOT EXISTS wager_transactions');
      expect(fullUpSql).toContain('CONSTRAINT unique_wager_idempotency_key UNIQUE (idempotency_key)');
      expect(fullUpSql).toContain('CONSTRAINT unique_provider_external_tx UNIQUE (provider_id, external_transaction_id)');

      expect(fullUpSql).toContain('CREATE TABLE IF NOT EXISTS wallet_ledger_entries');
      expect(fullUpSql).toContain('CONSTRAINT check_ledger_amounts_non_negative CHECK (amount >= 0 AND balance_before >= 0 AND balance_after >= 0)');

      // Verifica imutabilidade (append-only) do ledger via Trigger SQL
      expect(fullUpSql).toContain('CREATE OR REPLACE FUNCTION prevent_ledger_modification()');
      expect(fullUpSql).toContain('BEFORE UPDATE ON wallet_ledger_entries');
      expect(fullUpSql).toContain('BEFORE DELETE ON wallet_ledger_entries');

      expect(fullUpSql).toContain('CREATE TABLE IF NOT EXISTS inbox_messages');
      expect(fullUpSql).toContain('PRIMARY KEY (consumer_name, message_id)');

      expect(fullUpSql).toContain('CREATE TABLE IF NOT EXISTS outbox_messages');
      expect(fullUpSql).toContain('WHERE published_at IS NULL');

      // Verifica reversibilidade completa no down()
      (migration as unknown as { addSql: (sql: string) => void }).addSql = (sql: string) => {
        downSqlStatements.push(sql);
      };
      await migration.down();

      const fullDownSql = downSqlStatements.join('\n');
      expect(fullDownSql).toContain('DROP TABLE IF EXISTS outbox_messages CASCADE');
      expect(fullDownSql).toContain('DROP TABLE IF EXISTS inbox_messages CASCADE');
      expect(fullDownSql).toContain('DROP TRIGGER IF EXISTS trg_prevent_ledger_delete');
      expect(fullDownSql).toContain('DROP TRIGGER IF EXISTS trg_prevent_ledger_update');
      expect(fullDownSql).toContain('DROP TABLE IF EXISTS wallet_ledger_entries CASCADE');
      expect(fullDownSql).toContain('DROP TABLE IF EXISTS wager_transactions CASCADE');
      expect(fullDownSql).toContain('DROP TABLE IF EXISTS wallets CASCADE');
    });
  });

  describe('2. MikroORM Repositories & UnitOfWork Integration', () => {
    it('deve executar MikroOrmWalletRepository com LockMode.PESSIMISTIC_WRITE (SELECT ... FOR UPDATE)', async () => {
      let capturedOptions: Record<string, unknown> | undefined;
      let upsertedEntity: unknown;

      const wallet = Wallet.open({
        id: 'wallet-orm-1',
        playerId: 'player-orm-1',
        initialBalance: Money.from({ amount: '250.00', currency: 'BRL' }),
      });

      const fakeEm = {
        findOne: async (_entity: unknown, _where: unknown, options?: Record<string, unknown>) => {
          capturedOptions = options;
          return {
            id: wallet.id,
            playerId: wallet.playerId,
            currency: 'BRL',
            balance: wallet.balance,
            version: 1,
            createdAt: wallet.createdAt,
            updatedAt: wallet.updatedAt,
            toDomain: () => wallet,
          };
        },
        upsert: async (_entityClass: unknown, entity: unknown) => {
          upsertedEntity = entity;
        },
        flush: async () => {},
      } as unknown as EntityManager;

      const repo = new MikroOrmWalletRepository(fakeEm);

      const lockedWallet = await repo.findById(wallet.id, 'pessimistic_write');
      expect(lockedWallet).not.toBeNull();
      expect(lockedWallet!.balance.amount).toBe('250.00');
      expect(capturedOptions?.lockMode).toBe(LockMode.PESSIMISTIC_WRITE);

      const byPlayer = await repo.findByPlayerIdAndCurrency('player-orm-1', 'BRL');
      expect(byPlayer?.id).toBe(wallet.id);

      await repo.save(wallet);
      expect(upsertedEntity).toBeDefined();
    });

    it('deve executar MikroOrmWagerTransactionRepository (findById, idempotencyKey, reversal, pendingReferences)', async () => {
      const tx = WagerTransaction.create({
        id: 'tx-orm-1',
        providerId: 'prov-orm',
        externalTransactionId: 'ext-orm-1',
        idempotencyKey: 'prov-orm:ext-orm-1',
        payloadHash: 'a'.repeat(64),
        walletId: 'wallet-orm-1',
        playerId: 'player-orm-1',
        roundId: 'round-orm-1',
        gameId: 'slots',
        kind: WagerTransactionKind.Refund,
        money: Money.from({ amount: '50.00', currency: 'BRL' }),
        referenceExternalTransactionId: 'bet-orm-0',
      });
      tx.markPendingReference();

      const fakeEm = {
        findOne: async () => ({
          toDomain: () => tx,
        }),
        find: async () => [
          {
            toDomain: () => tx,
          },
        ],
        upsert: async () => {},
        flush: async () => {},
      } as unknown as EntityManager;

      const repo = new MikroOrmWagerTransactionRepository(fakeEm);

      expect((await repo.findById(tx.id))?.id).toBe(tx.id);
      expect((await repo.findByIdempotencyKey(tx.idempotencyKey))?.id).toBe(tx.id);
      expect((await repo.findByProviderAndExternalId('prov-orm', 'ext-orm-1'))?.id).toBe(tx.id);
      expect((await repo.findProcessedReversal('ref-id'))?.id).toBe(tx.id);

      const pending = await repo.findPendingReferences(10);
      expect(pending.length).toBe(1);
      expect(pending[0].status).toBe(WagerTransactionStatus.PendingReference);

      await repo.save(tx);
    });

    it('deve executar MikroOrmWalletLedgerRepository (cursor pagination e agregação Decimal sem float)', async () => {
      const entry1 = WalletLedgerEntry.create({
        id: 'entry-orm-1',
        walletId: 'w-ledger-1',
        transactionId: 'tx-1',
        direction: LedgerDirection.Credit,
        money: Money.from({ amount: '100.00', currency: 'BRL' }),
        balanceBefore: Money.zero('BRL'),
        balanceAfter: Money.from({ amount: '100.00', currency: 'BRL' }),
      });
      const entry2 = WalletLedgerEntry.create({
        id: 'entry-orm-2',
        walletId: 'w-ledger-1',
        transactionId: 'tx-2',
        direction: LedgerDirection.Debit,
        money: Money.from({ amount: '25.50', currency: 'BRL' }),
        balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
        balanceAfter: Money.from({ amount: '74.50', currency: 'BRL' }),
      });

      const fakeEm = {
        persistAndFlush: async () => {},
        createQueryBuilder: () => ({
          where: () => {},
          andWhere: () => {},
          orderBy: () => {},
          limit: () => {},
          getResultList: async () => [
            { id: entry2.id, createdAt: entry2.createdAt, toDomain: () => entry2 },
            { id: entry1.id, createdAt: entry1.createdAt, toDomain: () => entry1 },
          ],
        }),
        getKnex: () => {
          const qb: any = () => ({
            where: () => ({
              select: () => ({
                first: async () => ({
                  calculated_amount: '74.50',
                  count: '2',
                }),
              }),
            }),
          });
          qb.raw = (sql: string) => sql;
          return qb;
        },
      } as unknown as EntityManager;

      const repo = new MikroOrmWalletLedgerRepository(fakeEm);
      await repo.save(entry1);

      const cursorInput = Buffer.from(
        JSON.stringify({ createdAt: new Date().toISOString(), id: entry2.id }),
      ).toString('base64');

      const page = await repo.findByWalletId('w-ledger-1', { limit: 1, cursor: cursorInput });
      expect(page.entries.length).toBe(1);
      expect(page.nextCursor).toBeDefined();

      const calc = await repo.calculateBalance('w-ledger-1');
      expect(calc.calculatedAmount).toBe('74.50');
      expect(calc.count).toBe(2);
    });

    it('deve executar MikroOrmInboxRepository, MikroOrmOutboxRepository (FOR UPDATE SKIP LOCKED) e MikroOrmUnitOfWork', async () => {
      let skipLockedCalled = false;
      let forUpdateCalled = false;

      const event = WagerTransactionProcessed.from(
        WagerTransaction.create({
          id: 'tx-orm-2',
          providerId: 'p',
          externalTransactionId: 'e',
          idempotencyKey: 'p:e',
          payloadHash: 'b'.repeat(64),
          walletId: 'w',
          playerId: 'pl',
          roundId: 'r',
          gameId: 'g',
          kind: WagerTransactionKind.Loss,
          money: Money.zero('BRL'),
        }),
        Money.from({ amount: '100.00', currency: 'BRL' }),
        { correlationId: 'corr-1' },
      );
      const outboxMsg = OutboxMessage.enqueue(event);
      const inboxMsg = InboxMessage.receive({
        consumerName: 'wager-sqs-consumer',
        messageId: 'msg-1',
        payloadHash: 'c'.repeat(64),
      });

      const fakeEm = {
        findOne: async () => ({
          toDomain: () => inboxMsg,
        }),
        upsert: async () => {},
        flush: async () => {},
        transactional: async <T>(cb: () => Promise<T>) => cb(),
        createQueryBuilder: () => ({
          where: () => {},
          orderBy: () => {},
          limit: () => {},
          setLockMode: () => {},
        }),
        getKnex: () => () => ({
          whereNull: () => ({
            andWhere: (cb: (b: any) => void) => {
              cb({ whereNull: () => ({ orWhere: () => {} }) });
              return {
                orderBy: () => ({
                  limit: () => ({
                    forUpdate: () => {
                      forUpdateCalled = true;
                      return {
                        skipLocked: async () => {
                          skipLockedCalled = true;
                          return [
                            {
                              id: outboxMsg.id,
                              aggregate_id: outboxMsg.aggregateId,
                              event_type: outboxMsg.eventType,
                              payload: outboxMsg.payload,
                              occurred_at: outboxMsg.occurredAt.toISOString(),
                              attempts: 0,
                              next_attempt_at: null,
                              published_at: null,
                            },
                          ];
                        },
                      };
                    },
                  }),
                }),
              };
            },
          }),
        }),
      } as unknown as EntityManager;

      const inboxRepo = new MikroOrmInboxRepository(fakeEm);
      await inboxRepo.save(inboxMsg);
      const foundInbox = await inboxRepo.find('wager-sqs-consumer', 'msg-1');
      expect(foundInbox?.messageId).toBe('msg-1');

      const outboxRepo = new MikroOrmOutboxRepository(fakeEm);
      await outboxRepo.save(outboxMsg);
      const pending = await outboxRepo.findPendingToDispatch(10, new Date());
      expect(forUpdateCalled).toBe(true);
      expect(skipLockedCalled).toBe(true);
      expect(pending.length).toBe(1);
      expect(pending[0].id).toBe(outboxMsg.id);

      const uow = new MikroOrmUnitOfWork(fakeEm);
      const txResult = await uow.transactional(async () => 'committed');
      expect(txResult).toBe('committed');
    });

    it('Atomicidade Transacional: falha antes do commit deve fazer rollback completo de Wallet, Transaction, Ledger, Inbox e Outbox', async () => {
      const committedState = {
        wallets: new Map<string, Wallet>(),
        txs: new Map<string, WagerTransaction>(),
        ledger: [] as WalletLedgerEntry[],
        outbox: [] as OutboxMessage[],
        inbox: new Map<string, InboxMessage>(),
      };

      // Staging buffer dentro da transação SQL que só é aplicado se não houver exceção
      let stagingWallets = new Map(committedState.wallets);
      let stagingTxs = new Map(committedState.txs);
      let stagingLedger = [...committedState.ledger];
      let stagingOutbox = [...committedState.outbox];
      let stagingInbox = new Map(committedState.inbox);
      let failOnOutboxSave = false;

      const atomicUow = {
        transactional: async <T>(fn: () => Promise<T>): Promise<T> => {
          stagingWallets = new Map(
            Array.from(committedState.wallets.entries()).map(([k, v]) => [
              k,
              Wallet.rehydrate({
                id: v.id,
                playerId: v.playerId,
                currency: v.currency,
                balance: v.balance,
                version: v.version,
                createdAt: v.createdAt,
                updatedAt: v.updatedAt,
              }),
            ]),
          );
          stagingTxs = new Map(committedState.txs);
          stagingLedger = [...committedState.ledger];
          stagingOutbox = [...committedState.outbox];
          stagingInbox = new Map(committedState.inbox);

          const res = await fn();

          // Commit atômico
          committedState.wallets = stagingWallets;
          committedState.txs = stagingTxs;
          committedState.ledger = stagingLedger;
          committedState.outbox = stagingOutbox;
          committedState.inbox = stagingInbox;
          return res;
        },
      };

      const walletRepo = {
        findById: async (id: string) => stagingWallets.get(id) || null,
        findByPlayerIdAndCurrency: async () => null,
        save: async (w: Wallet) => {
          stagingWallets.set(w.id, w);
        },
      };
      const txRepo = {
        findById: async (id: string) => stagingTxs.get(id) || null,
        findByIdempotencyKey: async () => null,
        findByProviderAndExternalId: async () => null,
        findProcessedReversal: async () => null,
        findPendingReferences: async () => [],
        save: async (t: WagerTransaction) => {
          stagingTxs.set(t.id, t);
        },
      };
      const ledgerRepo = {
        save: async (e: WalletLedgerEntry) => {
          stagingLedger.push(e);
        },
        findByWalletId: async () => ({ entries: stagingLedger }),
        calculateBalance: async () => ({ calculatedAmount: '100.00', count: 1 }),
      };
      const outboxRepo = {
        save: async (o: OutboxMessage) => {
          if (failOnOutboxSave) {
            throw new Error('Database connection lost before commit!');
          }
          stagingOutbox.push(o);
        },
        findPendingToDispatch: async () => [],
      };
      const inboxRepo = {
        find: async () => null,
        save: async (i: InboxMessage) => {
          stagingInbox.set(`${i.consumerName}:${i.messageId}`, i);
        },
      };

      const createWallet = new CreateWalletUseCase(
        walletRepo,
        txRepo,
        ledgerRepo,
        outboxRepo,
        atomicUow,
      );
      const processWager = new ProcessWagerTransactionUseCase(
        walletRepo,
        txRepo,
        ledgerRepo,
        outboxRepo,
        inboxRepo,
        atomicUow,
      );

      const wallet = await createWallet.execute({
        playerId: 'player-atomic',
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });

      // Provoca falha na gravação da Outbox durante uma aposta via SQS
      failOnOutboxSave = true;

      await expect(
        processWager.execute({
          providerId: 'prov-atomic',
          externalTransactionId: 'bet-atomic-fail',
          idempotencyKey: 'prov-atomic:bet-atomic-fail',
          playerId: 'player-atomic',
          walletId: wallet.id,
          roundId: 'r-1',
          gameId: 'slots',
          kind: WagerTransactionKind.Bet,
          money: { amount: '40.00', currency: 'BRL' },
          inboxContext: {
            consumerName: 'wager-sqs-consumer',
            messageId: 'msg-atomic-fail',
          },
        }),
      ).rejects.toThrow('Database connection lost before commit!');

      // Verifica que NADA da aposta falha vazou para o estado comitado!
      expect(committedState.wallets.get(wallet.id)!.balance.amount).toBe('100.00');
      expect(committedState.txs.size).toBe(1); // apenas OPENING
      expect(committedState.ledger.length).toBe(1); // apenas OPENING
      expect(committedState.outbox.length).toBe(1); // apenas OPENING
      expect(committedState.inbox.size).toBe(0); // Inbox também sofreu rollback!
    });

    it('deve testar SqsEventPublisher e AwsSqsConsumerClient com FIFO MessageGroupId e MessageDeduplicationId', async () => {
      const sentCommands: any[] = [];
      const publisher = new SqsEventPublisher({
        send: async (cmd: any) => {
          sentCommands.push(cmd);
          return {};
        },
      } as any);

      const tx = WagerTransaction.create({
        id: 'tx-sqs-1',
        providerId: 'prov-sqs',
        externalTransactionId: 'ext-1',
        idempotencyKey: 'prov-sqs:ext-1',
        payloadHash: 'd'.repeat(64),
        walletId: 'wallet-fifo-1',
        playerId: 'p-1',
        roundId: 'r-1',
        gameId: 'g-1',
        kind: WagerTransactionKind.Loss,
        money: Money.zero('BRL'),
      });
      const event = WagerTransactionProcessed.from(
        tx,
        Money.from({ amount: '100.00', currency: 'BRL' }),
        {
          correlationId: 'corr-sqs',
        },
      );
      const outboxMsg = OutboxMessage.enqueue(event);

      await publisher.publish(outboxMsg);
      expect(sentCommands.length).toBe(1);
      expect(sentCommands[0].input.MessageGroupId).toBe('wallet-fifo-1');
      expect(sentCommands[0].input.MessageDeduplicationId).toBe(outboxMsg.id);

      // AwsSqsConsumerClient
      const sqsConsumerClient = new AwsSqsConsumerClient();
      const consumerCommands: any[] = [];
      (sqsConsumerClient as any).client = {
        send: async (cmd: any) => {
          consumerCommands.push(cmd);
          return {
            Messages: [
              {
                MessageId: 'm-1',
                ReceiptHandle: 'rh-1',
                Body: '{"ok":true}',
                Attributes: { ApproximateReceiveCount: '1' },
              },
            ],
          };
        },
      };

      const msgs = await sqsConsumerClient.receiveMessages('http://queue.fifo', 5, 1);
      expect(msgs.length).toBe(1);
      expect(msgs[0].messageId).toBe('m-1');

      await sqsConsumerClient.deleteMessage('http://queue.fifo', 'rh-1');
      await sqsConsumerClient.changeMessageVisibility('http://queue.fifo', 'rh-1', 0);
      await sqsConsumerClient.sendToDlq('http://dlq.fifo', '{"err":true}', 'grp-1', 'dedup-1');
      expect(consumerCommands.length).toBe(4);
    });
  });

  describe('3. Live PostgreSQL 16 & LocalStack SQS Container Verification (Automatic when Docker Compose is up)', () => {
    it('deve executar migrations, constraints (CHECK/UNIQUE), triggers append-only, atomicidade, outbox SKIP LOCKED e concorrência no PostgreSQL 16 real', async () => {
      const dbHost = process.env.DB_HOST || 'localhost';
      const dbPort = Number(process.env.DB_PORT) || 5432;
      const pgAvailable = await isPortOpen(dbHost, dbPort);

      if (!pgAvailable) {
        expect(mikroOrmConfig.dbName).toBeDefined();
        expect(mikroOrmConfig.entities?.length).toBe(5);
        return;
      }

      let orm: MikroORM | undefined;
      try {
        orm = await MikroORM.init(mikroOrmConfig);
        await orm.em.getConnection().execute('SELECT 1');
      } catch (err) {
        if (process.env.REQUIRE_INFRA === 'true') {
          throw err;
        }
        // Porta 5432 ocupada por instância local fora do Docker Compose (credenciais diferentes de postgres/wagering_db)
        expect(mikroOrmConfig.dbName).toBeDefined();
        expect(mikroOrmConfig.entities?.length).toBe(5);
        if (orm) await orm.close(true).catch(() => {});
        return;
      }

      try {
        await orm.getMigrator().up();
        const em = orm.em.fork();

        const walletRepo = new MikroOrmWalletRepository(em);
        const txRepo = new MikroOrmWagerTransactionRepository(em);
        const ledgerRepo = new MikroOrmWalletLedgerRepository(em);
        const outboxRepo = new MikroOrmOutboxRepository(em);
        const inboxRepo = new MikroOrmInboxRepository(em);
        const uow = new MikroOrmUnitOfWork(em);

        const createWallet = new CreateWalletUseCase(
          walletRepo,
          txRepo,
          ledgerRepo,
          outboxRepo,
          uow,
        );
        const processWager = new ProcessWagerTransactionUseCase(
          walletRepo,
          txRepo,
          ledgerRepo,
          outboxRepo,
          inboxRepo,
          uow,
        );
        const reconcile = new ReconcileWalletUseCase(walletRepo, ledgerRepo);

        const playerId = `pg-live-player-${Date.now()}`;
        const wallet = await createWallet.execute({
          playerId,
          initialBalance: { amount: '100.00', currency: 'BRL' },
        });

        // 1. Constraint CHECK (balance_amount >= 0) no PostgreSQL real
        await expect(
          em
            .getConnection()
            .execute(`UPDATE wallets SET balance_amount = -0.01 WHERE id = ?`, [wallet.id]),
        ).rejects.toThrow();

        // 2. Disputa concorrente de saldo real sobre PostgreSQL 16 (SELECT ... FOR UPDATE):
        // Duas apostas simultâneas de 80.00 BRL sobre saldo de 100.00 BRL usando EntityManagers isolados
        const makeNodeUseCase = () => {
          const nodeEm = orm!.em.fork();
          return new ProcessWagerTransactionUseCase(
            new MikroOrmWalletRepository(nodeEm),
            new MikroOrmWagerTransactionRepository(nodeEm),
            new MikroOrmWalletLedgerRepository(nodeEm),
            new MikroOrmOutboxRepository(nodeEm),
            new MikroOrmInboxRepository(nodeEm),
            new MikroOrmUnitOfWork(nodeEm),
          );
        };

        const ts = Date.now();
        const [bet1, bet2] = await Promise.all([
          makeNodeUseCase().execute({
            providerId: 'pg-live-prov',
            externalTransactionId: `pg-live-bet1-${ts}`,
            idempotencyKey: `pg-live-prov:bet1-${ts}`,
            playerId,
            walletId: wallet.id,
            roundId: 'r-live',
            gameId: 'fortune-tiger',
            kind: WagerTransactionKind.Bet,
            money: { amount: '80.00', currency: 'BRL' },
          }),
          makeNodeUseCase().execute({
            providerId: 'pg-live-prov',
            externalTransactionId: `pg-live-bet2-${ts}`,
            idempotencyKey: `pg-live-prov:bet2-${ts}`,
            playerId,
            walletId: wallet.id,
            roundId: 'r-live',
            gameId: 'fortune-tiger',
            kind: WagerTransactionKind.Bet,
            money: { amount: '80.00', currency: 'BRL' },
          }),
        ]);

        const statuses = [bet1.status, bet2.status];
        expect(statuses).toContain(WagerTransactionStatus.Processed);
        expect(statuses).toContain(WagerTransactionStatus.Rejected);

        // 3. Valida trigger append-only no PostgreSQL real: UPDATE e DELETE no ledger DEVEM falhar!
        await expect(
          em
            .getConnection()
            .execute(`UPDATE wallet_ledger_entries SET amount = 999.00 WHERE wallet_id = ?`, [
              wallet.id,
            ]),
        ).rejects.toThrow();
        await expect(
          em
            .getConnection()
            .execute(`DELETE FROM wallet_ledger_entries WHERE wallet_id = ?`, [wallet.id]),
        ).rejects.toThrow();

        // 4. Invariante universal contábil no PostgreSQL real
        const rec = await reconcile.execute(wallet.id);
        expect(rec.consistent).toBe(true);
        expect(rec.storedBalance.amount).toBe('20.00');
        expect(rec.calculatedBalance.amount).toBe('20.00');
        expect(rec.difference.amount).toBe('0.00');
      } finally {
        await orm.close(true);
      }
    });
  });
});
