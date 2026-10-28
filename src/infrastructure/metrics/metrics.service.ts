import { Injectable } from '@nestjs/common';

@Injectable()
export class MetricsService {
  private transactionsByStatus: Record<string, number> = {
    PROCESSED: 0,
    REJECTED: 0,
    PENDING_REFERENCE: 0,
    FAILED: 0,
  };

  private duplicatesDetected: number = 0;
  private retriesCount: number = 0;
  private dlqMessagesCount: number = 0;
  private lockConflictsCount: number = 0;
  private reconciliationDivergencesCount: number = 0;
  private outboxLagMs: number = 0;
  private latencies: number[] = [];

  public incrementTransactionStatus(status: string): void {
    this.transactionsByStatus[status] = (this.transactionsByStatus[status] || 0) + 1;
  }

  public incrementDuplicateReplay(): void {
    this.duplicatesDetected += 1;
  }

  public incrementRetry(): void {
    this.retriesCount += 1;
  }

  public incrementDlq(): void {
    this.dlqMessagesCount += 1;
  }

  public incrementLockConflict(): void {
    this.lockConflictsCount += 1;
  }

  public incrementReconciliationDivergence(): void {
    this.reconciliationDivergencesCount += 1;
  }

  public setOutboxLag(lagMs: number): void {
    this.outboxLagMs = lagMs;
  }

  public recordLatency(ms: number): void {
    this.latencies.push(ms);
    if (this.latencies.length > 1000) {
      this.latencies.shift();
    }
  }

  public getSnapshot(): Record<string, unknown> {
    const sorted = [...this.latencies].sort((a, b) => a - b);
    const p50 = sorted.length ? sorted[Math.floor(sorted.length * 0.5)] : 0;
    const p95 = sorted.length ? sorted[Math.floor(sorted.length * 0.95)] : 0;
    const p99 = sorted.length ? sorted[Math.floor(sorted.length * 0.99)] : 0;

    return {
      transactionsByStatus: { ...this.transactionsByStatus },
      duplicatesDetected: this.duplicatesDetected,
      retriesCount: this.retriesCount,
      dlqMessagesCount: this.dlqMessagesCount,
      lockConflictsCount: this.lockConflictsCount,
      reconciliationDivergencesCount: this.reconciliationDivergencesCount,
      outboxLagMs: this.outboxLagMs,
      latency: {
        p50,
        p95,
        p99,
        sampleCount: this.latencies.length,
      },
    };
  }
}
