import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import type { Response } from 'express';
import {
  CurrencyMismatchError,
  IdempotencyConflictError,
  InsufficientBalanceError,
  InvalidLedgerEntryError,
  InvalidMoneyError,
  InvalidOperationError,
  InvalidTransactionReferenceError,
  InvalidTransactionStateError,
  WalletOwnershipError,
} from '../../../domain/errors/domain.error';
import { WalletConflictError } from '../../../application/use-cases/create-wallet.use-case';
import { WalletNotFoundError } from '../../../application/use-cases/process-wager-transaction.use-case';
import { AppLoggerService } from '../../logger/app-logger.service';
import { RequestWithCorrelation } from '../middlewares/correlation-id.middleware';

@Catch()
export class GlobalHttpExceptionFilter implements ExceptionFilter {
  constructor(private readonly logger: AppLoggerService) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<RequestWithCorrelation>();
    const correlationId = request.correlationId;

    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let message = 'Internal server error';
    let failureCode: string | undefined = undefined;
    let details: unknown = undefined;

    if (exception instanceof HttpException) {
      status = exception.getStatus();
      const res = exception.getResponse();
      if (typeof res === 'object' && res !== null) {
        message = (res as Record<string, unknown>).message as string || exception.message;
        details = (res as Record<string, unknown>).message;
      } else {
        message = exception.message;
      }
    } else if (exception instanceof WalletNotFoundError) {
      status = HttpStatus.NOT_FOUND;
      message = exception.message;
    } else if (
      exception instanceof IdempotencyConflictError ||
      exception instanceof WalletConflictError
    ) {
      status = HttpStatus.CONFLICT;
      message = exception.message;
    } else if (
      exception instanceof InvalidMoneyError ||
      exception instanceof CurrencyMismatchError ||
      exception instanceof WalletOwnershipError ||
      exception instanceof InvalidOperationError
    ) {
      status = HttpStatus.BAD_REQUEST;
      message = exception.message;
    } else if (exception instanceof InsufficientBalanceError) {
      status = HttpStatus.UNPROCESSABLE_ENTITY;
      message = exception.message;
      failureCode = 'INSUFFICIENT_FUNDS';
    } else if (exception instanceof InvalidTransactionReferenceError) {
      status = HttpStatus.UNPROCESSABLE_ENTITY;
      message = exception.message;
      failureCode = exception.code;
    } else if (
      exception instanceof InvalidTransactionStateError ||
      exception instanceof InvalidLedgerEntryError
    ) {
      status = HttpStatus.UNPROCESSABLE_ENTITY;
      message = exception.message;
    } else if (exception instanceof Error) {
      message = exception.message;
    }

    this.logger.error(
      `HTTP Error ${status}: ${message}`,
      exception instanceof Error ? exception.stack : undefined,
      {
        correlationId,
        url: request.url,
        method: request.method,
        status,
        failureCode,
      },
    );

    response.status(status).json({
      statusCode: status,
      message,
      failureCode,
      details,
      correlationId,
      timestamp: new Date().toISOString(),
    });
  }
}
