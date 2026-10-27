import { Type } from 'class-transformer';
import {
  IsEnum,
  IsNotEmpty,
  IsObject,
  IsOptional,
  IsString,
  ValidateNested,
} from 'class-validator';
import { WagerTransactionKind } from '../../../domain/models/wager-transaction';
import { MoneyDto } from './create-wallet.dto';

export class ProcessWagerDto {
  @IsNotEmpty({ message: 'providerId is required' })
  @IsString({ message: 'providerId must be a string' })
  providerId!: string;

  @IsNotEmpty({ message: 'externalTransactionId is required' })
  @IsString({ message: 'externalTransactionId must be a string' })
  externalTransactionId!: string;

  @IsNotEmpty({ message: 'playerId is required' })
  @IsString({ message: 'playerId must be a string' })
  playerId!: string;

  @IsNotEmpty({ message: 'walletId is required' })
  @IsString({ message: 'walletId must be a string' })
  walletId!: string;

  @IsNotEmpty({ message: 'roundId is required' })
  @IsString({ message: 'roundId must be a string' })
  roundId!: string;

  @IsNotEmpty({ message: 'gameId is required' })
  @IsString({ message: 'gameId must be a string' })
  gameId!: string;

  @IsNotEmpty({ message: 'kind is required' })
  @IsEnum(WagerTransactionKind, {
    message: `kind must be one of: ${Object.values(WagerTransactionKind).join(', ')}`,
  })
  kind!: WagerTransactionKind;

  @IsNotEmpty({ message: 'money is required' })
  @IsObject({ message: 'money must be an object' })
  @ValidateNested()
  @Type(() => MoneyDto)
  money!: MoneyDto;

  @IsOptional()
  @IsString({ message: 'referenceExternalTransactionId must be a string' })
  referenceExternalTransactionId?: string;
}
