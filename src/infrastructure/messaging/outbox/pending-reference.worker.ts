import {
  Inject,
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
  Optional,
} from '@nestjs/common';
import { v4 as uuidv4 } from 'uuid';
import { ProcessWagerTransactionUseCase } from '../../../application/use-cases/process-wager-transaction.use-case';
import { FailureCode } from '../../../domain/errors/domain.error';
import { WagerTransactionRejected } from '../../../domain/events/events';
import { OutboxMessage } from '../../../domain/models/outbox-message';
import { WagerTransactionStatus } from '../../../domain/models/wager-transaction';
import type {
  OutboxRepository,
  UnitOfWork,
  WagerTransactionRepository,
} from '../../../domain/ports/repositories.port';
import { AppLoggerService } from '../../logger/app-logger.service';
import { MetricsService } from '../../metrics/metrics.service';

export interface PendingReferenceBatchResult {
  resolved: number;
  timedOut: number;
  stillPending: number;
}

interface RetryMeta {
  attempts: number;
  nextAttemptAt: number;
}

@Injectable()
export class PendingReferenceWorker implements OnModuleInit, OnApplicationShutdown {
  private timer?: ReturnType<typeof setInterval>;
  private isShuttingDown = false;
  private activePromise: Promise<PendingReferenceBatchResult> | null = null;

  private readonly pollIntervalMs: number;
  private readonly maxAttempts: number;
  private readonly ttlMs: number;
  private readonly baseBackoffMs: number;
  private readonly retryState = new Map<string, RetryMeta>();

  constructor(
    @Inject('WagerTransactionRepository')
    private readonly txRepo: WagerTransactionRepository,
    @Inject('OutboxRepository')
    private readonly outboxRepo: OutboxRepository,
    @Inject('UnitOfWork')
    private readonly uow: UnitOfWork,
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
    private readonly logger: AppLoggerService,
    private readonly metrics: MetricsService,
    @Optional()
    @Inject('PENDING_REF_OPTIONS')
    options?: {
      pollIntervalMs?: number;
      maxAttempts?: number;
      ttlMs?: number;
      baseBackoffMs?: number;
    },
  ) {
    this.pollIntervalMs =
      options?.pollIntervalMs ?? (Number(process.env.PENDING_REF_POLL_INTERVAL_MS) || 3000);
    this.maxAttempts =
      options?.maxAttempts ?? (Number(process.env.PENDING_REF_MAX_ATTEMPTS) || 5);
    this.ttlMs = options?.ttlMs ?? (Number(process.env.PENDING_REF_TTL_MS) || 60000);
    this.baseBackoffMs = options?.baseBackoffMs ?? 500;
  }

  public onModuleInit(): void {
    if (process.env.DISABLE_WORKERS === 'true' || process.env.NODE_ENV === 'test') {
      return;
    }

    this.timer = setInterval(() => {
      if (!this.isShuttingDown && !this.activePromise) {
        this.activePromise = this.processPendingBatch()
          .catch((err) => {
            this.logger.error('PendingReferenceWorker error', (err as Error).stack);
            return { resolved: 0, timedOut: 0, stillPending: 0 };
          })
          .finally(() => {
            this.activePromise = null;
          });
      }
    }, this.pollIntervalMs);
  }

  public async onApplicationShutdown(_signal?: string): Promise<void> {
    this.isShuttingDown = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    if (this.activePromise) {
      await this.activePromise;
    }
  }

  public async processPendingBatch(
    limit: number = 50,
    customNow?: Date,
  ): Promise<PendingReferenceBatchResult> {
    const now = customNow ?? new Date();
    const pendingList = await this.txRepo.findPendingReferences(limit);

    let resolved = 0;
    let timedOut = 0;
    let stillPending = 0;

    for (const tx of pendingList) {
      if (this.isShuttingDown) {
        break;
      }

      const meta = this.retryState.get(tx.id) ?? { attempts: 0, nextAttemptAt: 0 };

      // Verifica se já encontrou a transação de referência
      const refTx = tx.referenceExternalTransactionId
        ? await this.txRepo.findByProviderAndExternalId(
            tx.providerId,
            tx.referenceExternalTransactionId,
          )
        : null;

      if (refTx && refTx.isTerminal()) {
        // A referência chegou! Reprocessa através do caso de uso principal
        const result = await this.processWagerUseCase.execute({
          providerId: tx.providerId,
          externalTransactionId: tx.externalTransactionId,
          idempotencyKey: tx.idempotencyKey,
          playerId: tx.playerId,
          walletId: tx.walletId,
          roundId: tx.roundId,
          gameId: tx.gameId,
          kind: tx.kind,
          money: tx.money.toJSON(),
          referenceExternalTransactionId: tx.referenceExternalTransactionId,
        });

        this.metrics.incrementTransactionStatus(result.status);
        this.retryState.delete(tx.id);
        resolved += 1;
        continue;
      }

      // Se está em período de backoff exponencial e ainda não expirou o TTL, aguarda
      const ageMs = now.getTime() - tx.createdAt.getTime();
      const isTtlExpired = ageMs >= this.ttlMs;

      if (!isTtlExpired && meta.nextAttemptAt > now.getTime()) {
        stillPending += 1;
        continue;
      }

      const nextAttempts = meta.attempts + 1;
      const isAttemptsExhausted = nextAttempts >= this.maxAttempts;

      if (isTtlExpired || isAttemptsExhausted) {
        // Esgotou tentativas ou TTL: rejeita com REFERENCE_TIMEOUT e publica evento
        await this.uow.transactional(async () => {
          const currentTx = await this.txRepo.findById(tx.id);
          if (!currentTx || currentTx.isTerminal()) {
            return;
          }

          currentTx.reject(FailureCode.REFERENCE_TIMEOUT);
          await this.txRepo.save(currentTx);

          const rejectedEvent = new WagerTransactionRejected({
            eventId: uuidv4(),
            aggregateId: currentTx.id,
            correlationId: currentTx.idempotencyKey,
            occurredAt: now,
            data: {
              transactionId: currentTx.id,
              providerId: currentTx.providerId,
              externalTransactionId: currentTx.externalTransactionId,
              walletId: currentTx.walletId,
              playerId: currentTx.playerId,
              roundId: currentTx.roundId,
              gameId: currentTx.gameId,
              kind: currentTx.kind,
              money: currentTx.money.toJSON(),
              failureCode: FailureCode.REFERENCE_TIMEOUT,
              rejectedAt: now.toISOString(),
            },
          });

          await this.outboxRepo.save(OutboxMessage.enqueue(rejectedEvent));
          this.metrics.incrementTransactionStatus(WagerTransactionStatus.Rejected);
        });

        this.retryState.delete(tx.id);
        timedOut += 1;

        this.logger.warn(`Pending reference transaction ${tx.id} rejected due to timeout/attempts`, {
          transactionId: tx.id,
          providerId: tx.providerId,
          referenceExternalTransactionId: tx.referenceExternalTransactionId,
          attempts: nextAttempts,
          ageMs,
        });
      } else {
        // Agenda próxima tentativa com backoff exponencial
        const backoffMs = Math.min(Math.pow(2, nextAttempts) * this.baseBackoffMs, 30000);
        this.retryState.set(tx.id, {
          attempts: nextAttempts,
          nextAttemptAt: now.getTime() + backoffMs,
        });
        this.metrics.incrementRetry();
        stillPending += 1;
      }
    }

    return { resolved, timedOut, stillPending };
  }
}
