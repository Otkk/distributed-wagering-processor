import { Type } from 'class-transformer';
import { IsNotEmpty, IsObject, IsString, Matches, ValidateNested } from 'class-validator';

export class MoneyDto {
  @IsNotEmpty({ message: 'Amount is required' })
  @IsString({ message: 'Amount must be a string' })
  @Matches(/^\d+(\.\d{1,2})?$/, {
    message: 'Amount must be a non-negative decimal string with at most 2 decimal places',
  })
  amount!: string;

  @IsNotEmpty({ message: 'Currency is required' })
  @IsString({ message: 'Currency must be a string' })
  @Matches(/^[A-Z]{3}$/, { message: 'Currency must be a 3-letter ISO code' })
  currency!: string;
}

export class CreateWalletDto {
  @IsNotEmpty({ message: 'playerId is required' })
  @IsString({ message: 'playerId must be a string' })
  playerId!: string;

  @IsNotEmpty({ message: 'initialBalance is required' })
  @IsObject({ message: 'initialBalance must be an object' })
  @ValidateNested()
  @Type(() => MoneyDto)
  initialBalance!: MoneyDto;
}
