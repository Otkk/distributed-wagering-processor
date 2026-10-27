import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpStatus,
  Inject,
  NotFoundException,
  Param,
  Post,
  Res,
} from '@nestjs/common';
import type { Response } from 'express';
import { ProcessWagerTransactionUseCase } from '../../../application/use-cases/process-wager-transaction.use-case';
import { WagerTransactionStatus } from '../../../domain/models/wager-transaction';
import type { WagerTransactionRepository } from '../../../domain/ports/repositories.port';
import { MetricsService } from '../../metrics/metrics.service';
import { ProcessWagerDto } from '../dto/process-wager.dto';

@Controller()
export class WageringController {
  constructor(
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
    @Inject('WagerTransactionRepository') private readonly txRepo: WagerTransactionRepository,
    private readonly metrics: MetricsService,
  ) {}

  @Post('wagering/transactions')
  async submitTransaction(
    @Headers('idempotency-key') idempotencyKeyHeader: string | undefined,
    @Body() body: ProcessWagerDto,
    @Res() res: Response,
  ) {
    const startTime = Date.now();

    if (!idempotencyKeyHeader || !idempotencyKeyHeader.trim()) {
      throw new BadRequestException("Missing required 'Idempotency-Key' header");
    }

    const result = await this.processWagerUseCase.execute({
      providerId: body.providerId,
      externalTransactionId: body.externalTransactionId,
      idempotencyKey: idempotencyKeyHeader.trim(),
      playerId: body.playerId,
      walletId: body.walletId,
      roundId: body.roundId,
      gameId: body.gameId,
      kind: body.kind,
      money: body.money,
      referenceExternalTransactionId: body.referenceExternalTransactionId,
    });

    this.metrics.incrementTransactionStatus(result.status);
    if (result.idempotentReplay) {
      this.metrics.incrementDuplicateReplay();
    }
    this.metrics.recordLatency(Date.now() - startTime);

    if (result.status === WagerTransactionStatus.Rejected) {
      return res.status(HttpStatus.UNPROCESSABLE_ENTITY).json(result);
    }

    if (result.status === WagerTransactionStatus.PendingReference) {
      return res.status(HttpStatus.ACCEPTED).json(result);
    }

    return res.status(HttpStatus.OK).json(result);
  }

  @Get('wagering/transactions/:transactionId')
  async getTransactionById(@Param('transactionId') transactionId: string) {
    const tx = await this.txRepo.findById(transactionId);
    if (!tx) {
      throw new NotFoundException(`Transaction with id ${transactionId} not found`);
    }
    return {
      id: tx.id,
      providerId: tx.providerId,
      externalTransactionId: tx.externalTransactionId,
      idempotencyKey: tx.idempotencyKey,
      walletId: tx.walletId,
      playerId: tx.playerId,
      roundId: tx.roundId,
      gameId: tx.gameId,
      kind: tx.kind,
      money: tx.money.toJSON(),
      status: tx.status,
      referenceExternalTransactionId: tx.referenceExternalTransactionId,
      referenceTransactionId: tx.referenceTransactionId,
      failureCode: tx.failureCode,
      processedAt: tx.processedAt?.toISOString(),
      createdAt: tx.createdAt.toISOString(),
    };
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async getTransactionByExternalId(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ) {
    const tx = await this.txRepo.findByProviderAndExternalId(providerId, externalTransactionId);
    if (!tx) {
      throw new NotFoundException(
        `Transaction with providerId ${providerId} and externalTransactionId ${externalTransactionId} not found`,
      );
    }
    return {
      id: tx.id,
      providerId: tx.providerId,
      externalTransactionId: tx.externalTransactionId,
      idempotencyKey: tx.idempotencyKey,
      walletId: tx.walletId,
      playerId: tx.playerId,
      roundId: tx.roundId,
      gameId: tx.gameId,
      kind: tx.kind,
      money: tx.money.toJSON(),
      status: tx.status,
      referenceExternalTransactionId: tx.referenceExternalTransactionId,
      referenceTransactionId: tx.referenceTransactionId,
      failureCode: tx.failureCode,
      processedAt: tx.processedAt?.toISOString(),
      createdAt: tx.createdAt.toISOString(),
    };
  }
}
