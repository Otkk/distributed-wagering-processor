import { Injectable, LoggerService } from '@nestjs/common';
import pino from 'pino';

@Injectable()
export class AppLoggerService implements LoggerService {
  private readonly logger: pino.Logger;

  constructor() {
    this.logger = pino({
      level: process.env.LOG_LEVEL || 'info',
      formatters: {
        level: (label) => ({ level: label }),
      },
      timestamp: pino.stdTimeFunctions.isoTime,
    });
  }

  log(message: string, context?: Record<string, unknown>): void {
    this.logger.info(context || {}, message);
  }

  error(message: string, trace?: string, context?: Record<string, unknown>): void {
    this.logger.error({ ...context, trace }, message);
  }

  warn(message: string, context?: Record<string, unknown>): void {
    this.logger.warn(context || {}, message);
  }

  debug?(message: string, context?: Record<string, unknown>): void {
    this.logger.debug(context || {}, message);
  }

  child(bindings: pino.Bindings): pino.Logger {
    return this.logger.child(bindings);
  }
}
