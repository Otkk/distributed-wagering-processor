import { execFileSync } from 'child_process';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as http from 'http';
import * as net from 'net';
import * as path from 'path';
import {
  CreateQueueCommand,
  ListQueuesCommand,
  PurgeQueueCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import mikroOrmConfig from '../../src/infrastructure/database/mikro-orm.config';

export interface StoredSqsMessage {
  messageId: string;
  receiptHandle: string;
  body: string;
  md5OfBody: string;
  messageGroupId?: string;
  messageDeduplicationId?: string;
  receiveCount: number;
  visibleAt: number;
}

/**
 * Servidor HTTP compatível com o protocolo AWS SQS (application/x-amz-json-1.0)
 * para execução de testes E2E e integração real via @aws-sdk/client-sqs quando
 * o container LocalStack não estiver ativo na porta 4566.
 */
export class LocalSqsHttpServer {
  private server?: http.Server;
  public port = 0;
  public endpoint = '';
  public readonly queues = new Map<string, StoredSqsMessage[]>();
  public readonly dedupCache = new Map<string, string>();
  public readonly maxReceiveCount: number;

  constructor(maxReceiveCount = 5) {
    this.maxReceiveCount = maxReceiveCount;
    this.queues.set('wager-transactions.fifo', []);
    this.queues.set('wager-transactions-dlq.fifo', []);
    this.queues.set('wagering-events.fifo', []);
  }

  public reset(): void {
    this.queues.set('wager-transactions.fifo', []);
    this.queues.set('wager-transactions-dlq.fifo', []);
    this.queues.set('wagering-events.fifo', []);
    this.dedupCache.clear();
  }

  public getQueueMessages(queueName: string): StoredSqsMessage[] {
    return this.queues.get(queueName) || [];
  }

  public async start(preferredPort = 0): Promise<string> {
    if (this.server) {
      return this.endpoint;
    }

    this.server = http.createServer((req, res) => {
      let rawBody = '';
      req.on('data', (chunk) => {
        rawBody += chunk;
      });
      req.on('end', () => {
        try {
          const target = String(req.headers['x-amz-target'] || '');
          const body = rawBody ? JSON.parse(rawBody) : {};
          const response = this.handleOperation(target, body);
          res.writeHead(200, { 'Content-Type': 'application/x-amz-json-1.0' });
          res.end(JSON.stringify(response));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/x-amz-json-1.0' });
          res.end(
            JSON.stringify({
              __type: 'com.amazonaws.sqs#QueueDoesNotExist',
              message: (err as Error).message,
            }),
          );
        }
      });
    });

    await new Promise<void>((resolve, reject) => {
      this.server!.once('error', reject);
      this.server!.listen(preferredPort, '127.0.0.1', () => {
        const addr = this.server!.address() as net.AddressInfo;
        this.port = addr.port;
        this.endpoint = `http://127.0.0.1:${this.port}`;
        resolve();
      });
    });

    return this.endpoint;
  }

  public async stop(): Promise<void> {
    if (!this.server) {
      return;
    }
    await new Promise<void>((resolve) => {
      this.server!.close(() => resolve());
    });
    this.server = undefined;
  }

  private resolveQueueName(queueUrl?: string): string {
    if (!queueUrl) {
      return 'wager-transactions.fifo';
    }
    const parts = queueUrl.split('/');
    const name = parts[parts.length - 1] || 'wager-transactions.fifo';
    if (!this.queues.has(name)) {
      this.queues.set(name, []);
    }
    return name;
  }

  private handleOperation(
    target: string,
    body: Record<string, unknown>,
  ): Record<string, unknown> {
    const op = target.replace('AmazonSQS.', '');
    const now = Date.now();

    switch (op) {
      case 'ListQueues': {
        const urls = Array.from(this.queues.keys()).map(
          (name) => `${this.endpoint}/000000000000/${name}`,
        );
        return { QueueUrls: urls };
      }

      case 'CreateQueue': {
        const qName = String(body.QueueName || 'wager-transactions.fifo');
        if (!this.queues.has(qName)) {
          this.queues.set(qName, []);
        }
        return { QueueUrl: `${this.endpoint}/000000000000/${qName}` };
      }

      case 'GetQueueUrl': {
        const qName = String(body.QueueName || 'wager-transactions.fifo');
        return { QueueUrl: `${this.endpoint}/000000000000/${qName}` };
      }

      case 'SendMessage': {
        const queueName = this.resolveQueueName(String(body.QueueUrl || ''));
        const msgBody = String(body.MessageBody || '');
        const md5 = crypto.createHash('md5').update(msgBody, 'utf8').digest('hex');
        const dedupId = body.MessageDeduplicationId
          ? String(body.MessageDeduplicationId)
          : undefined;
        const groupId = body.MessageGroupId ? String(body.MessageGroupId) : undefined;

        if (dedupId && this.dedupCache.has(`${queueName}:${dedupId}`)) {
          return {
            MessageId: this.dedupCache.get(`${queueName}:${dedupId}`),
            MD5OfMessageBody: md5,
            SequenceNumber: String(now),
          };
        }

        const messageId = crypto.randomUUID();
        if (dedupId) {
          this.dedupCache.set(`${queueName}:${dedupId}`, messageId);
        }

        const queue = this.queues.get(queueName)!;
        queue.push({
          messageId,
          receiptHandle: `rh-${messageId}-${now}`,
          body: msgBody,
          md5OfBody: md5,
          messageGroupId: groupId,
          messageDeduplicationId: dedupId,
          receiveCount: 0,
          visibleAt: now,
        });

        return {
          MessageId: messageId,
          MD5OfMessageBody: md5,
          SequenceNumber: String(now),
        };
      }

      case 'ReceiveMessage': {
        const queueName = this.resolveQueueName(String(body.QueueUrl || ''));
        const maxMessages = Number(body.MaxNumberOfMessages || 1);
        const visibilityTimeoutSec = Number(body.VisibilityTimeout ?? 30);
        const queue = this.queues.get(queueName)!;
        const dlq = this.queues.get('wager-transactions-dlq.fifo')!;

        const delivered: Record<string, unknown>[] = [];
        const remaining: StoredSqsMessage[] = [];

        for (const msg of queue) {
          if (delivered.length < maxMessages && msg.visibleAt <= now) {
            msg.receiveCount += 1;
            if (
              queueName !== 'wager-transactions-dlq.fifo' &&
              msg.receiveCount > this.maxReceiveCount
            ) {
              dlq.push({ ...msg, visibleAt: now });
              continue;
            }
            msg.receiptHandle = `rh-${msg.messageId}-${now}-${msg.receiveCount}`;
            msg.visibleAt = now + visibilityTimeoutSec * 1000;
            delivered.push({
              MessageId: msg.messageId,
              ReceiptHandle: msg.receiptHandle,
              Body: msg.body,
              MD5OfBody: msg.md5OfBody,
              Attributes: {
                ApproximateReceiveCount: String(msg.receiveCount),
                ...(msg.messageGroupId ? { MessageGroupId: msg.messageGroupId } : {}),
                ...(msg.messageDeduplicationId
                  ? { MessageDeduplicationId: msg.messageDeduplicationId }
                  : {}),
              },
            });
          }
          remaining.push(msg);
        }

        this.queues.set(queueName, remaining);
        return { Messages: delivered };
      }

      case 'DeleteMessage': {
        const queueName = this.resolveQueueName(String(body.QueueUrl || ''));
        const receiptHandle = String(body.ReceiptHandle || '');
        const queue = this.queues.get(queueName)!;
        this.queues.set(
          queueName,
          queue.filter((m) => m.receiptHandle !== receiptHandle),
        );
        return {};
      }

      case 'ChangeMessageVisibility': {
        const queueName = this.resolveQueueName(String(body.QueueUrl || ''));
        const receiptHandle = String(body.ReceiptHandle || '');
        const timeoutSec = Number(body.VisibilityTimeout ?? 0);
        const queue = this.queues.get(queueName)!;
        for (const msg of queue) {
          if (msg.receiptHandle === receiptHandle) {
            msg.visibleAt = now + timeoutSec * 1000;
          }
        }
        return {};
      }

      case 'PurgeQueue': {
        const queueName = this.resolveQueueName(String(body.QueueUrl || ''));
        this.queues.set(queueName, []);
        return {};
      }

      default:
        return {};
    }
  }
}

export interface SqsEndpointHandle {
  endpoint: string;
  isExternalContainer: boolean;
  localServer?: LocalSqsHttpServer;
  stop: () => Promise<void>;
}

async function isTcpPortOpen(host: string, port: number, timeoutMs = 800): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    let settled = false;
    const finish = (ok: boolean) => {
      if (!settled) {
        settled = true;
        socket.destroy();
        resolve(ok);
      }
    };
    socket.setTimeout(timeoutMs);
    socket.once('connect', () => finish(true));
    socket.once('timeout', () => finish(false));
    socket.once('error', () => finish(false));
    socket.connect(port, host);
  });
}

/**
 * Conecta ao container LocalStack real na porta 4566 se estiver rodando (garantindo as 3 filas FIFO),
 * ou sobe o LocalSqsHttpServer em porta efêmera caso o container não esteja ativo.
 * Se REQUIRE_INFRA=true ou REQUIRE_LOCALSTACK=true, exige obrigatoriamente o container LocalStack.
 */
export async function ensureSqsEndpoint(): Promise<SqsEndpointHandle> {
  const candidateEndpoint = process.env.AWS_ENDPOINT || 'http://localhost:4566';
  let parsedUrl: URL;
  try {
    parsedUrl = new URL(candidateEndpoint);
  } catch {
    parsedUrl = new URL('http://localhost:4566');
  }

  const host = parsedUrl.hostname || 'localhost';
  const port = Number(parsedUrl.port || 4566);

  if (await isTcpPortOpen(host, port)) {
    const client = new SQSClient({
      region: process.env.AWS_REGION || 'us-east-1',
      endpoint: candidateEndpoint,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'test',
      },
    });

    try {
      await client.send(new ListQueuesCommand({}));
      await Promise.all(
        [
          'wager-transactions-dlq.fifo',
          'wager-transactions.fifo',
          'wagering-events.fifo',
        ].map(async (queueName) => {
          await client.send(
            new CreateQueueCommand({
              QueueName: queueName,
              Attributes: {
                FifoQueue: 'true',
                ContentBasedDeduplication: 'true',
              },
            }),
          );
          try {
            await client.send(
              new PurgeQueueCommand({
                QueueUrl: `${candidateEndpoint}/000000000000/${queueName}`,
              }),
            );
          } catch {
            // PurgeQueue pode ter cooldown em alguns brokers; ignorar se não suportado
          }
        }),
      );

      return {
        endpoint: candidateEndpoint,
        isExternalContainer: true,
        stop: async () => {},
      };
    } catch {
      // Se falhar o handshake SQS, verifica se o container era obrigatório
    }
  }

  if (process.env.REQUIRE_INFRA === 'true' || process.env.REQUIRE_LOCALSTACK === 'true') {
    throw new Error(
      `LocalStack SQS container is required (REQUIRE_INFRA=true) but not reachable at ${candidateEndpoint}. Run 'docker compose up -d'.`,
    );
  }

  const localServer = new LocalSqsHttpServer(5);
  const endpoint = await localServer.start(0);
  return {
    endpoint,
    isExternalContainer: false,
    localServer,
    stop: () => localServer.stop(),
  };
}

async function canConnectToPostgres(port: number): Promise<boolean> {
  let orm: MikroORM | null = null;
  try {
    orm = await MikroORM.init({
      ...mikroOrmConfig,
      port,
      connect: true,
    });
    await orm.em.getConnection().execute('SELECT 1');
    await orm.close(true);
    return true;
  } catch {
    if (orm) {
      try {
        await orm.close(true);
      } catch {
        // ignore
      }
    }
    return false;
  }
}

function findLocalPostgresBinDir(): string | null {
  const baseDir = 'C:\\Program Files\\PostgreSQL';
  if (fs.existsSync(baseDir)) {
    const versions = fs
      .readdirSync(baseDir)
      .sort((a, b) => Number(b) - Number(a));
    for (const v of versions) {
      const binPath = path.join(baseDir, v, 'bin');
      if (fs.existsSync(path.join(binPath, 'pg_ctl.exe'))) {
        return binPath;
      }
    }
  }
  return null;
}

/**
 * Garante que uma instância real do PostgreSQL está acessível e com as migrations aplicadas.
 * Tenta a porta configurada (5432), depois 5433, e caso necessário inicializa um cluster
 * local temporário na porta 5433 usando os binários nativos do PostgreSQL.
 */
export async function ensureRealPostgres(): Promise<{ port: number; orm: MikroORM }> {
  const configuredPort = Number(process.env.DB_PORT);
  const candidatePorts =
    process.env.REQUIRE_INFRA === 'true'
      ? Array.from(new Set([configuredPort || 5432, 5432, 5433]))
      : Array.from(new Set([5433, configuredPort || 5432, 5432]));

  for (const port of candidatePorts) {
    if (await canConnectToPostgres(port)) {
      process.env.DB_PORT = String(port);
      const orm = await MikroORM.init({
        ...mikroOrmConfig,
        port,
      });
      await orm.getMigrator().up();
      return { port, orm };
    }
  }

  if (process.env.REQUIRE_INFRA === 'true') {
    throw new Error(
      `PostgreSQL container is required (REQUIRE_INFRA=true) but not reachable on ports ${candidatePorts.join(', ')}. Run 'docker compose up -d'.`,
    );
  }

  // Fallback: se os binários locais do PostgreSQL existirem, sobe na porta 5433
  const pgBin = findLocalPostgresBinDir();
  if (pgBin) {
    const pgData = path.join(process.cwd(), '.pgdata-local-test');
    const pgCtl = path.join(pgBin, 'pg_ctl.exe');
    const initDb = path.join(pgBin, 'initdb.exe');
    const psql = path.join(pgBin, 'psql.exe');

    if (!fs.existsSync(pgData)) {
      execFileSync(initDb, ['-D', pgData, '-U', 'postgres', '-A', 'trust', '--encoding=UTF8', '--locale=C'], {
        stdio: 'ignore',
      });
    }

    try {
      execFileSync(pgCtl, ['-D', pgData, '-l', path.join(pgData, 'pg.log'), '-o', '-p 5433', 'start'], {
        stdio: 'ignore',
      });
    } catch {
      // Pode já estar iniciando
    }

    try {
      execFileSync(
        psql,
        ['-h', 'localhost', '-p', '5433', '-U', 'postgres', '-d', 'postgres', '-c', 'CREATE DATABASE wagering_db;'],
        { stdio: 'ignore' },
      );
    } catch {
      // Banco já existe
    }

    if (await canConnectToPostgres(5433)) {
      process.env.DB_PORT = '5433';
      const orm = await MikroORM.init({
        ...mikroOrmConfig,
        port: 5433,
      });
      await orm.getMigrator().up();
      return { port: 5433, orm };
    }
  }

  throw new Error(
    'No accessible PostgreSQL instance found on ports 5432 or 5433. Run `docker compose up -d` or ensure PostgreSQL is running.',
  );
}
