import { Migration } from '@mikro-orm/migrations';

export class Migration20261027000001_InitialSchema extends Migration {
  async up(): Promise<void> {
    // 1. Wallets Table
    this.addSql(`
      CREATE TABLE IF NOT EXISTS wallets (
        id UUID PRIMARY KEY,
        player_id VARCHAR(255) NOT NULL,
        currency VARCHAR(3) NOT NULL,
        balance_amount NUMERIC(18, 2) NOT NULL,
        version BIGINT NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT check_wallet_balance_non_negative CHECK (balance_amount >= 0),
        CONSTRAINT unique_player_currency UNIQUE (player_id, currency)
      );
    `);
    this.addSql(`CREATE INDEX IF NOT EXISTS idx_wallets_player ON wallets (player_id);`);

    // 2. Wager Transactions Table
    this.addSql(`
      CREATE TABLE IF NOT EXISTS wager_transactions (
        id UUID PRIMARY KEY,
        provider_id VARCHAR(100) NOT NULL,
        external_transaction_id VARCHAR(255) NOT NULL,
        idempotency_key VARCHAR(255) NOT NULL,
        payload_hash VARCHAR(64) NOT NULL,
        wallet_id UUID NOT NULL REFERENCES wallets(id),
        player_id VARCHAR(255) NOT NULL,
        round_id VARCHAR(255) NOT NULL,
        game_id VARCHAR(255) NOT NULL,
        kind VARCHAR(20) NOT NULL,
        amount NUMERIC(18, 2) NOT NULL,
        currency VARCHAR(3) NOT NULL,
        status VARCHAR(20) NOT NULL,
        reference_external_transaction_id VARCHAR(255) NULL,
        reference_transaction_id UUID NULL REFERENCES wager_transactions(id),
        failure_code VARCHAR(50) NULL,
        processed_at TIMESTAMPTZ NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT unique_wager_idempotency_key UNIQUE (idempotency_key),
        CONSTRAINT unique_provider_external_tx UNIQUE (provider_id, external_transaction_id)
      );
    `);
    this.addSql(`CREATE INDEX IF NOT EXISTS idx_wager_tx_provider_ref ON wager_transactions (provider_id, reference_external_transaction_id);`);
    this.addSql(`CREATE INDEX IF NOT EXISTS idx_wager_tx_wallet_created ON wager_transactions (wallet_id, created_at);`);
    this.addSql(`CREATE INDEX IF NOT EXISTS idx_wager_tx_status ON wager_transactions (status);`);

    // 3. Wallet Ledger Entries Table (Append-Only)
    this.addSql(`
      CREATE TABLE IF NOT EXISTS wallet_ledger_entries (
        id UUID PRIMARY KEY,
        wallet_id UUID NOT NULL REFERENCES wallets(id),
        transaction_id UUID NOT NULL REFERENCES wager_transactions(id),
        direction VARCHAR(10) NOT NULL,
        amount NUMERIC(18, 2) NOT NULL,
        currency VARCHAR(3) NOT NULL,
        balance_before NUMERIC(18, 2) NOT NULL,
        balance_after NUMERIC(18, 2) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT check_ledger_amounts_non_negative CHECK (amount >= 0 AND balance_before >= 0 AND balance_after >= 0)
      );
    `);
    this.addSql(`CREATE INDEX IF NOT EXISTS idx_ledger_entries_wallet_cursor ON wallet_ledger_entries (wallet_id, created_at DESC, id DESC);`);

    // Immutability trigger for Ledger
    this.addSql(`
      CREATE OR REPLACE FUNCTION prevent_ledger_modification()
      RETURNS TRIGGER AS $$
      BEGIN
        RAISE EXCEPTION 'Ledger entries are strictly immutable. UPDATE and DELETE operations are forbidden.';
      END;
      $$ LANGUAGE plpgsql;
    `);

    this.addSql(`
      DROP TRIGGER IF EXISTS trg_prevent_ledger_update ON wallet_ledger_entries;
      CREATE TRIGGER trg_prevent_ledger_update
      BEFORE UPDATE ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION prevent_ledger_modification();
    `);

    this.addSql(`
      DROP TRIGGER IF EXISTS trg_prevent_ledger_delete ON wallet_ledger_entries;
      CREATE TRIGGER trg_prevent_ledger_delete
      BEFORE DELETE ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION prevent_ledger_modification();
    `);

    // 4. Inbox Messages Table (Persistent Deduplication)
    this.addSql(`
      CREATE TABLE IF NOT EXISTS inbox_messages (
        consumer_name VARCHAR(100) NOT NULL,
        message_id VARCHAR(255) NOT NULL,
        payload_hash VARCHAR(64) NOT NULL,
        received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        processed_at TIMESTAMPTZ NULL,
        PRIMARY KEY (consumer_name, message_id)
      );
    `);

    // 5. Outbox Messages Table
    this.addSql(`
      CREATE TABLE IF NOT EXISTS outbox_messages (
        id UUID PRIMARY KEY,
        aggregate_id VARCHAR(255) NOT NULL,
        event_type VARCHAR(100) NOT NULL,
        payload JSONB NOT NULL,
        attempts INT NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMPTZ NULL,
        published_at TIMESTAMPTZ NULL,
        occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `);
    this.addSql(`
      CREATE INDEX IF NOT EXISTS idx_outbox_pending_due 
      ON outbox_messages (occurred_at ASC) 
      WHERE published_at IS NULL;
    `);
  }

  async down(): Promise<void> {
    this.addSql(`DROP TABLE IF EXISTS outbox_messages CASCADE;`);
    this.addSql(`DROP TABLE IF EXISTS inbox_messages CASCADE;`);
    this.addSql(`DROP TRIGGER IF EXISTS trg_prevent_ledger_delete ON wallet_ledger_entries;`);
    this.addSql(`DROP TRIGGER IF EXISTS trg_prevent_ledger_update ON wallet_ledger_entries;`);
    this.addSql(`DROP FUNCTION IF EXISTS prevent_ledger_modification;`);
    this.addSql(`DROP TABLE IF EXISTS wallet_ledger_entries CASCADE;`);
    this.addSql(`DROP TABLE IF EXISTS wager_transactions CASCADE;`);
    this.addSql(`DROP TABLE IF EXISTS wallets CASCADE;`);
  }
}
