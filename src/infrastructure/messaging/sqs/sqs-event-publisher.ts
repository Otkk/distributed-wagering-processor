import { Injectable } from '@nestjs/common';
import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { OutboxMessage } from '../../../domain/models/outbox-message';

export interface EventPublisherPort {
  publish(outboxMessage: OutboxMessage): Promise<void>;
}

@Injectable()
export class SqsEventPublisher implements EventPublisherPort {
  private readonly sqsClient: SQSClient;
  private readonly eventsQueueUrl: string;

  constructor(sqsClient?: SQSClient, eventsQueueUrl?: string) {
    this.sqsClient =
      sqsClient ??
      new SQSClient({
        region: process.env.AWS_REGION || 'us-east-1',
        endpoint: process.env.AWS_ENDPOINT || 'http://localhost:4566',
        credentials: {
          accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'test',
          secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'test',
        },
      });

    this.eventsQueueUrl =
      eventsQueueUrl ??
      (process.env.SQS_EVENTS_QUEUE_URL ||
        'http://localhost:4566/000000000000/wagering-events.fifo');
  }

  public async publish(outboxMessage: OutboxMessage): Promise<void> {
    const command = new SendMessageCommand({
      QueueUrl: this.eventsQueueUrl,
      MessageBody: JSON.stringify(outboxMessage.payload),
      MessageGroupId: outboxMessage.aggregateId,
      MessageDeduplicationId: outboxMessage.id,
      MessageAttributes: {
        eventType: {
          DataType: 'String',
          StringValue: outboxMessage.eventType,
        },
      },
    });

    await this.sqsClient.send(command);
  }
}
