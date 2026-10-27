export enum FailureCode {
  INSUFFICIENT_FUNDS = 'INSUFFICIENT_FUNDS',
  REFERENCE_NOT_FOUND = 'REFERENCE_NOT_FOUND',
  REFERENCE_ALREADY_REVERSED = 'REFERENCE_ALREADY_REVERSED',
  INVALID_REFERENCE_TYPE = 'INVALID_REFERENCE_TYPE',
  REFERENCE_AMOUNT_MISMATCH = 'REFERENCE_AMOUNT_MISMATCH',
  REFERENCE_ROUND_MISMATCH = 'REFERENCE_ROUND_MISMATCH',
  NEGATIVE_BALANCE_PREVENTED = 'NEGATIVE_BALANCE_PREVENTED',
  REFERENCE_TIMEOUT = 'REFERENCE_TIMEOUT',
  PAYLOAD_CONFLICT = 'PAYLOAD_CONFLICT',
  CURRENCY_MISMATCH = 'CURRENCY_MISMATCH',
  INTERNAL_ERROR = 'INTERNAL_ERROR',
}

export class DomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class InvalidOperationError extends DomainError {
  constructor(message: string) {
    super(message);
  }
}

export class WalletOwnershipError extends DomainError {
  constructor(playerId: string, walletId: string) {
    super(`Player ${playerId} does not own wallet ${walletId}`);
  }
}

export class InvalidMoneyError extends DomainError {
  constructor(message: string) {
    super(`Invalid money value: ${message}`);
  }
}

export class CurrencyMismatchError extends DomainError {
  constructor(expected: string, received: string) {
    super(`Currency mismatch: expected ${expected}, received ${received}`);
  }
}

export class InsufficientBalanceError extends DomainError {
  constructor(walletId: string, currentBalance: string, requestedAmount: string) {
    super(`Insufficient balance in wallet ${walletId}: current ${currentBalance}, requested ${requestedAmount}`);
  }
}

export class InvalidTransactionStateError extends DomainError {
  constructor(transactionId: string, currentStatus: string, attemptedAction: string) {
    super(`Cannot execute ${attemptedAction} on transaction ${transactionId} in terminal/invalid state ${currentStatus}`);
  }
}

export class InvalidTransactionReferenceError extends DomainError {
  constructor(readonly code: FailureCode, message: string) {
    super(message);
  }
}

export class IdempotencyConflictError extends DomainError {
  constructor(idempotencyKey: string) {
    super(`Idempotency key ${idempotencyKey} already used with divergent payload`);
  }
}

export class InvalidLedgerEntryError extends DomainError {
  constructor(message: string) {
    super(`Invalid ledger entry: ${message}`);
  }
}
