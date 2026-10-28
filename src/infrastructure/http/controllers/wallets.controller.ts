import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Inject,
  Param,
  Post,
  Query,
} from '@nestjs/common';
import { CreateWalletUseCase } from '../../../application/use-cases/create-wallet.use-case';
import { GetWalletLedgerUseCase } from '../../../application/use-cases/get-wallet-ledger.use-case';
import {
  ProcessWagerTransactionUseCase,
  WalletNotFoundError,
} from '../../../application/use-cases/process-wager-transaction.use-case';
import { ReconcileWalletUseCase } from '../../../application/use-cases/reconcile-wallet.use-case';
import type { WalletRepository } from '../../../domain/ports/repositories.port';
import { CreateWalletDto } from '../dto/create-wallet.dto';

@Controller('wallets')
export class WalletsController {
  constructor(
    private readonly createWalletUseCase: CreateWalletUseCase,
    private readonly getWalletLedgerUseCase: GetWalletLedgerUseCase,
    private readonly reconcileWalletUseCase: ReconcileWalletUseCase,
    @Inject('WalletRepository') private readonly walletRepo: WalletRepository,
  ) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  async createWallet(@Body() body: CreateWalletDto) {
    const result = await this.createWalletUseCase.execute({
      playerId: body.playerId,
      initialBalance: body.initialBalance,
    });
    return result;
  }

  @Get(':walletId')
  async getWallet(@Param('walletId') walletId: string) {
    const wallet = await this.walletRepo.findById(walletId);
    if (!wallet) {
      throw new WalletNotFoundError(walletId);
    }
    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
    };
  }

  @Get(':walletId/ledger')
  async getWalletLedger(
    @Param('walletId') walletId: string,
    @Query('cursor') cursor?: string,
    @Query('limit') limit?: string,
  ) {
    const limitNum = limit ? parseInt(limit, 10) : 50;
    const result = await this.getWalletLedgerUseCase.execute({
      walletId,
      cursor,
      limit: limitNum,
    });
    return {
      entries: result.entries.map((e) => ({
        id: e.id,
        walletId: e.walletId,
        transactionId: e.transactionId,
        direction: e.direction,
        money: e.money.toJSON(),
        balanceBefore: e.balanceBefore.toJSON(),
        balanceAfter: e.balanceAfter.toJSON(),
        createdAt: e.createdAt.toISOString(),
      })),
      nextCursor: result.nextCursor,
    };
  }

  @Post(':walletId/reconciliation')
  @HttpCode(HttpStatus.OK)
  async reconcileWallet(@Param('walletId') walletId: string) {
    return this.reconcileWalletUseCase.execute(walletId);
  }

  @Get(':walletId/reconcile')
  async reconcileWalletGet(@Param('walletId') walletId: string) {
    return this.reconcileWalletUseCase.execute(walletId);
  }
}

