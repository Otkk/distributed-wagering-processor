import {
  Inject,
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import type {
  OutboxRepository,
  UnitOfWork,
} from '../../../domain/ports/repositories.port';
import { AppLoggerService } from '../../logger/app-logger.service';
import { MetricsService } from '../../metrics/metrics.service';
import type { EventPublisherPort } from '../sqs/sqs-event-publisher';

export interface OutboxBatchResult {
  published: number;
  failed: number;
}

@Injectable()
export class OutboxDispatcherWorker implements OnModuleInit, OnApplicationShutdown {
  private timer?: ReturnType<typeof setInterval>;
  private isShuttingDown = false;
  private activeBatchPromise: Promise<OutboxBatchResult> | null = null;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly leaseDurationMs: number;

  constructor(
    @Inject('OutboxRepository') private readonly outboxRepo: OutboxRepository,
    @Inject('UnitOfWork') private readonly uow: UnitOfWork,
    @Inject('EventPublisherPort') private readonly publisher: EventPublisherPort,
    private readonly logger: AppLoggerService,
    private readonly metrics: MetricsService,
  ) {
    this.pollIntervalMs = Number(process.env.OUTBOX_POLL_INTERVAL_MS) || 1000;
    this.batchSize = Number(process.env.OUTBOX_BATCH_SIZE) || 50;
    this.leaseDurationMs = Number(process.env.OUTBOX_LEASE_DURATION_MS) || 30000;
  }

  public onModuleInit(): void {
    if (process.env.DISABLE_WORKERS === 'true' || process.env.NODE_ENV === 'test') {
      return;
    }

    this.timer = setInterval(() => {
      if (!this.isShuttingDown && !this.activeBatchPromise) {
        this.activeBatchPromise = this.dispatchBatch()
          .catch((err) => {
            this.logger.error('OutboxDispatcherWorker poll error', (err as Error).stack);
            return { published: 0, failed: 0 };
          })
          .finally(() => {
            this.activeBatchPromise = null;
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
    if (this.activeBatchPromise) {
      await this.activeBatchPromise;
    }
  }

  /**
   * Padrão Claim-Publish-Confirm:
   * 1. Fase Claim (transação SQL curta): seleciona com FOR UPDATE SKIP LOCKED e aplica lease curto (nextAttemptAt = now + 30s).
   * 2. Fase Publish (fora de transação SQL): publica no AWS SQS sem segurar conexões ou locks no PostgreSQL.
   * 3. Fase Confirm (transação SQL curta): grava publishedAt (sucesso) ou scheduleRetry com backoff exponencial (erro).
   */
  public async dispatchBatch(
    customBatchSize?: number,
    customNow?: Date,
  ): Promise<OutboxBatchResult> {
    const limit = customBatchSize ?? this.batchSize;
    const now = customNow ?? new Date();
    let published = 0;
    let failed = 0;

    // Fase 1: Claim curto sob FOR UPDATE SKIP LOCKED
    const claimedMessages = await this.uow.transactional(async () => {
      const pendingMessages = await this.outboxRepo.findPendingToDispatch(limit, now);
      for (const message of pendingMessages) {
        message.claimLease(now, this.leaseDurationMs);
        await this.outboxRepo.save(message);
      }
      return pendingMessages;
    });

    // Fase 2 & 3: Publicação na rede (fora de transação) e confirmação individual
    for (const message of claimedMessages) {
      if (this.isShuttingDown) {
        break;
      }

      const lagMs = Math.max(0, now.getTime() - message.occurredAt.getTime());
      this.metrics.setOutboxLag(lagMs);

      try {
        await this.publisher.publish(message);
        message.markPublished(new Date());
        await this.uow.transactional(async () => {
          await this.outboxRepo.save(message);
        });
        published += 1;
      } catch (err) {
        message.scheduleRetry(now);
        await this.uow.transactional(async () => {
          await this.outboxRepo.save(message);
        });
        this.metrics.incrementRetry();
        failed += 1;

        this.logger.warn(`Failed to publish outbox event ${message.id}, scheduled retry`, {
          outboxId: message.id,
          aggregateId: message.aggregateId,
          eventType: message.eventType,
          attempts: message.attempts,
          nextAttemptAt: message.nextAttemptAt?.toISOString(),
          error: (err as Error).message,
        });
      }
    }

    return { published, failed };
  }
}
