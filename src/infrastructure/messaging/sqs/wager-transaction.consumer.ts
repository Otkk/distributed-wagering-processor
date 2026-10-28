import {
  Inject,
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import {
  ProcessWagerTransactionUseCase,
  WalletNotFoundError,
} from '../../../application/use-cases/process-wager-transaction.use-case';
import {
  CurrencyMismatchError,
  IdempotencyConflictError,
  InvalidMoneyError,
  InvalidOperationError,
  InvalidTransactionReferenceError,
  WalletOwnershipError,
} from '../../../domain/errors/domain.error';
import { MoneyProps } from '../../../domain/models/money';
import { WagerTransactionKind } from '../../../domain/models/wager-transaction';
import { AppLoggerService } from '../../logger/app-logger.service';
import { MetricsService } from '../../metrics/metrics.service';

export interface SqsRawMessage {
  messageId: string;
  receiptHandle: string;
  body: string;
  attributes?: Record<string, string>;
}

export interface WagerTransactionRequestedEnvelope {
  messageId: string;
  type: 'WagerTransactionRequested';
  occurredAt: string;
  data: {
    providerId: string;
    externalTransactionId: string;
    idempotencyKey?: string;
    playerId: string;
    walletId: string;
    roundId: string;
    gameId: string;
    kind: WagerTransactionKind;
    money: MoneyProps;
    referenceExternalTransactionId?: string;
  };
}

export interface SqsConsumerClientPort {
  receiveMessages(
    queueUrl: string,
    maxMessages: number,
    waitTimeSeconds: number,
  ): Promise<SqsRawMessage[]>;
  deleteMessage(queueUrl: string, receiptHandle: string): Promise<void>;
  changeMessageVisibility(
    queueUrl: string,
    receiptHandle: string,
    visibilityTimeout: number,
  ): Promise<void>;
  sendToDlq(
    dlqUrl: string,
    body: string,
    groupId: string,
    dedupId: string,
  ): Promise<void>;
}

@Injectable()
export class AwsSqsConsumerClient implements SqsConsumerClientPort {
  private readonly client: SQSClient;

  constructor(client?: SQSClient) {
    this.client =
      client ??
      new SQSClient({
        region: process.env.AWS_REGION || 'us-east-1',
        endpoint: process.env.AWS_ENDPOINT || 'http://localhost:4566',
        credentials: {
          accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'test',
          secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'test',
        },
      });
  }

  public async receiveMessages(
    queueUrl: string,
    maxMessages: number,
    waitTimeSeconds: number,
  ): Promise<SqsRawMessage[]> {
    const response = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: waitTimeSeconds,
        AttributeNames: ['All'],
      }),
    );

    if (!response.Messages) {
      return [];
    }

    return response.Messages.map((msg) => ({
      messageId: msg.MessageId || '',
      receiptHandle: msg.ReceiptHandle || '',
      body: msg.Body || '',
      attributes: msg.Attributes,
    }));
  }

  public async deleteMessage(queueUrl: string, receiptHandle: string): Promise<void> {
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
      }),
    );
  }

  public async changeMessageVisibility(
    queueUrl: string,
    receiptHandle: string,
    visibilityTimeout: number,
  ): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: visibilityTimeout,
      }),
    );
  }

  public async sendToDlq(
    dlqUrl: string,
    body: string,
    groupId: string,
    dedupId: string,
  ): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: dlqUrl,
        MessageBody: body,
        MessageGroupId: groupId || 'dlq-default-group',
        MessageDeduplicationId: dedupId,
      }),
    );
  }
}

export type MessageHandlingDecision = 'ACK' | 'RETRY' | 'DLQ';

@Injectable()
export class WagerTransactionConsumer implements OnModuleInit, OnApplicationShutdown {
  public static readonly CONSUMER_NAME = 'wager-sqs-consumer';

  private isShuttingDown = false;
  private pollTimer?: ReturnType<typeof setInterval>;
  private readonly inFlightReceipts = new Map<string, string>(); // messageId -> receiptHandle
  private readonly inFlightPromises = new Set<Promise<unknown>>();

  private readonly queueUrl: string;
  private readonly dlqUrl: string;
  private readonly maxReceiveCount: number;
  private readonly shutdownTimeoutMs: number;

  constructor(
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
    @Inject('SqsConsumerClientPort')
    private readonly sqsClient: SqsConsumerClientPort,
    private readonly logger: AppLoggerService,
    private readonly metrics: MetricsService,
    options?: {
      queueUrl?: string;
      dlqUrl?: string;
      maxReceiveCount?: number;
      shutdownTimeoutMs?: number;
    },
  ) {
    this.queueUrl =
      options?.queueUrl ??
      (process.env.SQS_INGESTION_QUEUE_URL ||
        'http://localhost:4566/000000000000/wager-transactions.fifo');
    this.dlqUrl =
      options?.dlqUrl ??
      (process.env.SQS_DLQ_QUEUE_URL ||
        'http://localhost:4566/000000000000/wager-transactions-dlq.fifo');
    this.maxReceiveCount = options?.maxReceiveCount ?? 5;
    this.shutdownTimeoutMs = options?.shutdownTimeoutMs ?? 5000;
  }

  public onModuleInit(): void {
    if (process.env.DISABLE_WORKERS === 'true' || process.env.NODE_ENV === 'test') {
      return;
    }

    this.pollTimer = setInterval(() => {
      if (!this.isShuttingDown && this.inFlightPromises.size === 0) {
        this.pollOnce().catch((err) => {
          this.logger.error('SQS poll error', (err as Error).stack);
        });
      }
    }, 1000);
  }

  public async onApplicationShutdown(_signal?: string): Promise<void> {
    this.isShuttingDown = true;
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = undefined;
    }

    if (this.inFlightPromises.size > 0) {
      let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
      const timeoutPromise = new Promise<'TIMEOUT'>((resolve) => {
        timeoutHandle = setTimeout(() => resolve('TIMEOUT'), this.shutdownTimeoutMs);
      });

      const raceResult = await Promise.race([
        Promise.allSettled(Array.from(this.inFlightPromises)),
        timeoutPromise,
      ]);

      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }

      // Se ainda restarem mensagens em andamento no SIGTERM, devolve a visibilidade imediatamente
      if (raceResult === 'TIMEOUT' || this.inFlightReceipts.size > 0) {
        await this.releaseInFlightVisibility();
      }
    }
  }

  public async releaseInFlightVisibility(): Promise<void> {
    for (const [, receiptHandle] of this.inFlightReceipts.entries()) {
      try {
        await this.sqsClient.changeMessageVisibility(this.queueUrl, receiptHandle, 0);
      } catch (err) {
        this.logger.warn('Failed to release visibility during shutdown', {
          error: (err as Error).message,
        });
      }
    }
    this.inFlightReceipts.clear();
  }

  public async pollOnce(maxMessages: number = 10): Promise<MessageHandlingDecision[]> {
    if (this.isShuttingDown) {
      return [];
    }

    const messages = await this.sqsClient.receiveMessages(this.queueUrl, maxMessages, 1);
    const decisions: MessageHandlingDecision[] = [];

    for (const msg of messages) {
      if (this.isShuttingDown) {
        await this.sqsClient.changeMessageVisibility(this.queueUrl, msg.receiptHandle, 0);
        continue;
      }

      const decision = await this.handleMessage(msg);
      decisions.push(decision);
    }

    return decisions;
  }

  public async handleMessage(rawMessage: SqsRawMessage): Promise<MessageHandlingDecision> {
    const startTime = Date.now();
    this.inFlightReceipts.set(rawMessage.messageId, rawMessage.receiptHandle);

    const taskPromise = (async (): Promise<MessageHandlingDecision> => {
      let envelope: WagerTransactionRequestedEnvelope;

      // 1. Parse e validação estrutural do envelope
      try {
        envelope = JSON.parse(rawMessage.body) as WagerTransactionRequestedEnvelope;
        if (
          !envelope ||
          envelope.type !== 'WagerTransactionRequested' ||
          !envelope.data ||
          !envelope.data.providerId ||
          !envelope.data.externalTransactionId ||
          !envelope.data.walletId ||
          !envelope.data.playerId ||
          !envelope.data.kind ||
          !envelope.data.money
        ) {
          throw new InvalidOperationError('Malformed WagerTransactionRequested message envelope');
        }
      } catch (parseErr) {
        await this.forwardToDlq(
          rawMessage,
          'malformed-group',
          (parseErr as Error).message,
        );
        return 'DLQ';
      }

      const { data } = envelope;
      const effectiveMessageId = envelope.messageId || rawMessage.messageId;
      const idempotencyKey =
        data.idempotencyKey || `${data.providerId}:${data.externalTransactionId}`;

      try {
        // 2. Executa o caso de uso unificado (grava Wallet, Tx, Ledger, Outbox e Inbox na mesma transação SQL)
        const result = await this.processWagerUseCase.execute({
          providerId: data.providerId,
          externalTransactionId: data.externalTransactionId,
          idempotencyKey,
          playerId: data.playerId,
          walletId: data.walletId,
          roundId: data.roundId,
          gameId: data.gameId,
          kind: data.kind,
          money: data.money,
          referenceExternalTransactionId: data.referenceExternalTransactionId,
          messageId: effectiveMessageId,
          consumerName: WagerTransactionConsumer.CONSUMER_NAME,
        });

        this.metrics.incrementTransactionStatus(result.status);
        if (result.idempotentReplay) {
          this.metrics.incrementDuplicateReplay();
        }
        this.metrics.recordLatency(Date.now() - startTime);

        // 3. ACK estritamente APÓS o commit da transação SQL!
        await this.sqsClient.deleteMessage(this.queueUrl, rawMessage.receiptHandle);
        return 'ACK';
      } catch (err) {
        // 4. Classificação de falhas: Permanente (DLQ) vs Transitória (Retry com Backoff)
        if (this.isPermanentError(err)) {
          await this.forwardToDlq(
            rawMessage,
            data.walletId || 'dlq-group',
            (err as Error).message,
          );
          return 'DLQ';
        }

        const receiveCount = Number(rawMessage.attributes?.ApproximateReceiveCount || '1');
        if (receiveCount >= this.maxReceiveCount) {
          await this.forwardToDlq(
            rawMessage,
            data.walletId || 'dlq-group',
            `Max receive count (${this.maxReceiveCount}) exhausted: ${(err as Error).message}`,
          );
          return 'DLQ';
        }

        // Erro transitório de infraestrutura -> Retry com backoff exponencial
        const backoffSeconds = Math.min(Math.pow(2, receiveCount), 60);
        await this.sqsClient.changeMessageVisibility(
          this.queueUrl,
          rawMessage.receiptHandle,
          backoffSeconds,
        );
        this.metrics.incrementRetry();

        this.logger.warn('Transient error processing SQS message, scheduled retry', {
          messageId: effectiveMessageId,
          walletId: data.walletId,
          providerId: data.providerId,
          receiveCount,
          backoffSeconds,
          error: (err as Error).message,
        });

        return 'RETRY';
      }
    })();

    this.inFlightPromises.add(taskPromise);

    try {
      return await taskPromise;
    } finally {
      this.inFlightPromises.delete(taskPromise);
      this.inFlightReceipts.delete(rawMessage.messageId);
    }
  }

  private isPermanentError(err: unknown): boolean {
    return (
      err instanceof InvalidMoneyError ||
      err instanceof InvalidOperationError ||
      err instanceof CurrencyMismatchError ||
      err instanceof WalletOwnershipError ||
      err instanceof IdempotencyConflictError ||
      err instanceof WalletNotFoundError ||
      err instanceof InvalidTransactionReferenceError
    );
  }

  private async forwardToDlq(
    rawMessage: SqsRawMessage,
    groupId: string,
    reason: string,
  ): Promise<void> {
    await this.sqsClient.sendToDlq(
      this.dlqUrl,
      rawMessage.body,
      groupId,
      rawMessage.messageId || `dlq-${Date.now()}`,
    );
    await this.sqsClient.deleteMessage(this.queueUrl, rawMessage.receiptHandle);
    this.metrics.incrementDlq();

    this.logger.error(`Message ${rawMessage.messageId} moved to DLQ: ${reason}`, undefined, {
      messageId: rawMessage.messageId,
      reason,
    });
  }
}
