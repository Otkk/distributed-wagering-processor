import { Controller, Get, HttpStatus, Res } from '@nestjs/common';
import type { Response } from 'express';
import { EntityManager } from '@mikro-orm/postgresql';
import { ListQueuesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { MetricsService } from '../../metrics/metrics.service';

@Controller('health')
export class HealthController {
  private readonly sqsClient: SQSClient;

  constructor(
    private readonly em: EntityManager,
    private readonly metrics: MetricsService,
  ) {
    this.sqsClient = new SQSClient({
      region: process.env.AWS_REGION || 'us-east-1',
      endpoint: process.env.AWS_ENDPOINT || 'http://localhost:4566',
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'test',
      },
    });
  }

  @Get('live')
  getLive() {
    return {
      status: 'ok',
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
    };
  }

  @Get('ready')
  async getReady(@Res() res: Response) {
    let dbStatus = 'ok';
    let sqsStatus = 'ok';
    let isHealthy = true;

    // 1. Verifica conexão com PostgreSQL
    try {
      await this.em.getConnection().execute('SELECT 1');
    } catch (err) {
      dbStatus = `error: ${(err as Error).message}`;
      isHealthy = false;
    }

    // 2. Verifica conexão com AWS SQS / LocalStack
    try {
      await this.sqsClient.send(new ListQueuesCommand({ MaxResults: 1 }));
    } catch (err) {
      sqsStatus = `error: ${(err as Error).message}`;
      isHealthy = false;
    }

    const payload = {
      status: isHealthy ? 'ok' : 'degraded',
      services: {
        database: dbStatus,
        sqs: sqsStatus,
      },
      metrics: this.metrics.getSnapshot(),
      timestamp: new Date().toISOString(),
    };

    if (isHealthy) {
      return res.status(HttpStatus.OK).json(payload);
    } else {
      return res.status(HttpStatus.SERVICE_UNAVAILABLE).json(payload);
    }
  }

  @Get('metrics')
  getMetrics() {
    return this.metrics.getSnapshot();
  }
}
