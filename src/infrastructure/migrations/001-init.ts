import type { MigrationInterface, QueryRunner } from 'typeorm';

export class Init1793700000000 implements MigrationInterface {
  async up(q: QueryRunner): Promise<void> {
    await q.query(`
CREATE TABLE wallets (
  id uuid PRIMARY KEY,
  player_id uuid NOT NULL,
  currency varchar(3) NOT NULL CHECK(currency ~ '^[A-Z]{3}$'),
  balance numeric(18,2) NOT NULL DEFAULT 0 CHECK(balance >= 0),
  version integer NOT NULL DEFAULT 1 CHECK(version >= 1),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(player_id,currency),
  UNIQUE(id,currency)
);

CREATE TABLE wager_transactions (
  id uuid PRIMARY KEY,
  provider_id text NOT NULL,
  external_transaction_id text NOT NULL,
  idempotency_key text NOT NULL,
  payload_hash char(64) NOT NULL,
  wallet_id uuid NOT NULL REFERENCES wallets(id),
  player_id uuid NOT NULL,
  round_id text NOT NULL,
  game_id text NOT NULL,
  kind text NOT NULL CHECK(kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
  amount numeric(18,2) NOT NULL CHECK(amount >= 0),
  currency varchar(3) NOT NULL CHECK(currency ~ '^[A-Z]{3}$'),
  reference_external_transaction_id text,
  reference_transaction_id uuid REFERENCES wager_transactions(id),
  status text NOT NULL CHECK(status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
  failure_code text,
  response jsonb,
  attempts integer NOT NULL DEFAULT 0 CHECK(attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  processed_at timestamptz,
  UNIQUE(provider_id,idempotency_key),
  UNIQUE(provider_id,external_transaction_id),
  UNIQUE(id,wallet_id,currency),
  CHECK(kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
  CHECK(status NOT IN ('REJECTED','FAILED') OR failure_code IS NOT NULL),
  CHECK(status <> 'PROCESSED' OR processed_at IS NOT NULL)
);

-- A processed transaction can be reversed once, by a REFUND or a ROLLBACK, never by both.
CREATE UNIQUE INDEX one_reversal_per_reference ON wager_transactions(reference_transaction_id)
  WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK');

CREATE INDEX pending_reference_due ON wager_transactions(next_attempt_at)
  WHERE status='PENDING_REFERENCE';

CREATE TABLE wallet_ledger (
  sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id uuid NOT NULL UNIQUE,
  wallet_id uuid NOT NULL,
  transaction_id uuid NOT NULL,
  currency varchar(3) NOT NULL,
  direction text NOT NULL CHECK(direction IN ('DEBIT','CREDIT')),
  amount numeric(18,2) NOT NULL CHECK(amount > 0),
  balance_before numeric(18,2) NOT NULL CHECK(balance_before >= 0),
  balance_after numeric(18,2) NOT NULL CHECK(balance_after >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(wallet_id,transaction_id),
  FOREIGN KEY(wallet_id,currency) REFERENCES wallets(id,currency),
  FOREIGN KEY(transaction_id,wallet_id,currency) REFERENCES wager_transactions(id,wallet_id,currency),
  CHECK(balance_after = balance_before + CASE WHEN direction='CREDIT' THEN amount ELSE -amount END)
);

CREATE INDEX ledger_cursor ON wallet_ledger(wallet_id,sequence);

CREATE TABLE inbox (
  consumer_name text NOT NULL,
  message_id text NOT NULL,
  payload_hash char(64) NOT NULL,
  transaction_id uuid NOT NULL REFERENCES wager_transactions(id),
  received_at timestamptz NOT NULL DEFAULT now(),
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(consumer_name,message_id)
);

CREATE TABLE outbox (
  id uuid PRIMARY KEY,
  aggregate_id uuid NOT NULL,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  occurred_at timestamptz NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);

CREATE INDEX outbox_due ON outbox(next_attempt_at,occurred_at) WHERE published_at IS NULL;

CREATE FUNCTION immutable_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'immutable audit record' USING ERRCODE='23514';
END $$;

CREATE TRIGGER immutable_ledger BEFORE UPDATE OR DELETE ON wallet_ledger
  FOR EACH ROW EXECUTE FUNCTION immutable_row();

CREATE TRIGGER immutable_inbox BEFORE UPDATE OR DELETE ON inbox
  FOR EACH ROW EXECUTE FUNCTION immutable_row();

CREATE FUNCTION protect_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    RAISE EXCEPTION 'transaction deletion forbidden' USING ERRCODE='23514';
  END IF;
  IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
    RAISE EXCEPTION 'terminal transaction immutable' USING ERRCODE='23514';
  END IF;
  IF (to_jsonb(NEW) - ARRAY['status','reference_transaction_id','failure_code','response','attempts','next_attempt_at','processed_at'])
     IS DISTINCT FROM
     (to_jsonb(OLD) - ARRAY['status','reference_transaction_id','failure_code','response','attempts','next_attempt_at','processed_at'])
  THEN
    RAISE EXCEPTION 'transaction payload immutable' USING ERRCODE='23514';
  END IF;
  IF OLD.status='PENDING_REFERENCE' AND NEW.status='PENDING' THEN
    RAISE EXCEPTION 'invalid transition' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER protect_transaction BEFORE UPDATE OR DELETE ON wager_transactions
  FOR EACH ROW EXECUTE FUNCTION protect_transaction();

CREATE FUNCTION protect_wallet() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    RAISE EXCEPTION 'wallet deletion forbidden' USING ERRCODE='23514';
  END IF;
  IF (NEW.id,NEW.player_id,NEW.currency,NEW.created_at)
     IS DISTINCT FROM (OLD.id,OLD.player_id,OLD.currency,OLD.created_at)
  THEN
    RAISE EXCEPTION 'wallet identity immutable' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER protect_wallet BEFORE UPDATE OR DELETE ON wallets
  FOR EACH ROW EXECUTE FUNCTION protect_wallet();

-- The ledger is the only writer of the materialized balance, which also serializes direct SQL inserts.
CREATE FUNCTION apply_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  w wallets%ROWTYPE;
  t wager_transactions%ROWTYPE;
  r wager_transactions%ROWTYPE;
  expected_direction text;
BEGIN
  SELECT * INTO w FROM wallets WHERE id=NEW.wallet_id FOR UPDATE;
  SELECT * INTO t FROM wager_transactions WHERE id=NEW.transaction_id;

  IF t.kind='ROLLBACK' THEN
    SELECT * INTO r FROM wager_transactions WHERE id=t.reference_transaction_id;
    expected_direction=CASE WHEN r.kind='BET' THEN 'CREDIT' ELSE 'DEBIT' END;
  ELSE
    expected_direction=CASE WHEN t.kind='BET' THEN 'DEBIT' ELSE 'CREDIT' END;
  END IF;

  IF NEW.direction<>expected_direction THEN
    RAISE EXCEPTION 'ledger direction mismatch' USING ERRCODE='23514';
  END IF;
  IF t.status<>'PROCESSED' OR t.kind='LOSS' OR t.amount<>NEW.amount OR w.balance<>NEW.balance_before THEN
    RAISE EXCEPTION 'ledger transaction/balance mismatch' USING ERRCODE='23514';
  END IF;

  UPDATE wallets
     SET balance=NEW.balance_after,
         version=version+CASE WHEN t.kind='OPENING' THEN 0 ELSE 1 END,
         updated_at=now()
   WHERE id=w.id;
  RETURN NEW;
END $$;

CREATE TRIGGER apply_ledger BEFORE INSERT ON wallet_ledger
  FOR EACH ROW EXECUTE FUNCTION apply_ledger();

-- Deferred checks reject manual balance changes and financial transactions without matching ledger rows.
CREATE FUNCTION check_wallet_balance() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  wid uuid;
  actual numeric;
  expected numeric;
  actual_version integer;
  expected_version integer;
BEGIN
  IF TG_TABLE_NAME='wallets' THEN wid=NEW.id; ELSE wid=NEW.wallet_id; END IF;

  SELECT balance,version INTO actual,actual_version FROM wallets WHERE id=wid;
  SELECT COALESCE(sum(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END),0),
         1+count(*) FILTER (WHERE t.kind<>'OPENING')
    INTO expected,expected_version
    FROM wallet_ledger l
    JOIN wager_transactions t ON t.id=l.transaction_id
   WHERE l.wallet_id=wid;

  IF actual<>expected OR actual_version<>expected_version THEN
    RAISE EXCEPTION 'wallet/ledger invariant violated' USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER wallet_balance_consistent AFTER INSERT OR UPDATE ON wallets
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wallet_balance();

CREATE CONSTRAINT TRIGGER ledger_balance_consistent AFTER INSERT ON wallet_ledger
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wallet_balance();

CREATE FUNCTION check_transaction_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  t wager_transactions%ROWTYPE;
  entries integer;
BEGIN
  SELECT * INTO t FROM wager_transactions WHERE id=NEW.id;
  SELECT count(*) INTO entries FROM wallet_ledger WHERE transaction_id=t.id;

  IF entries <> (CASE WHEN t.status='PROCESSED' AND t.kind<>'LOSS' AND t.amount>0 THEN 1 ELSE 0 END) THEN
    RAISE EXCEPTION 'transaction/ledger invariant violated' USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER transaction_ledger_consistent AFTER INSERT OR UPDATE ON wager_transactions
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_transaction_ledger();
`);
  }

  async down(q: QueryRunner): Promise<void> {
    await q.query(`
DROP TABLE outbox,inbox,wallet_ledger,wager_transactions,wallets CASCADE;
DROP FUNCTION check_transaction_ledger(),check_wallet_balance(),apply_ledger(),
              protect_wallet(),protect_transaction(),immutable_row();
`);
  }
}
