import 'reflect-metadata';
import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { GlobalHttpExceptionFilter } from './infrastructure/http/filters/http-exception.filter';
import { AppLoggerService } from './infrastructure/logger/app-logger.service';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, {
    bufferLogs: true,
  });

  const logger = app.get(AppLoggerService);
  app.useLogger(logger);

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
      forbidNonWhitelisted: true,
    }),
  );

  app.useGlobalFilters(new GlobalHttpExceptionFilter(logger));

  const port = process.env.PORT || 3000;
  await app.listen(port);

  logger.log(`Distributed Wagering Processor running on port ${port}`, {
    port,
    environment: process.env.NODE_ENV || 'development',
  });
}

bootstrap();
