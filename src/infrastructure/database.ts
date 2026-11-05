import 'reflect-metadata';
import { DataSource, EntitySchema } from 'typeorm';
import { Init1793700000000 } from './migrations/001-init';

const DEFAULT_URL = 'postgres://wager:wager@localhost:5432/wager';

export interface WalletRow {
  id: string;
  player_id: string;
  currency: string;
  balance: string;
  version: number;
  created_at: Date;
  updated_at: Date;
}

export const WalletSchema = new EntitySchema<WalletRow>({
  name: 'WalletRecord',
  tableName: 'wallets',
  columns: {
    id: { type: 'uuid', primary: true },
    player_id: { type: 'uuid' },
    currency: { type: 'varchar', length: 3 },
    balance: { type: 'numeric', precision: 18, scale: 2 },
    version: { type: 'integer' },
    created_at: { type: 'timestamptz' },
    updated_at: { type: 'timestamptz' },
  },
});

export function database(url = process.env.DATABASE_URL ?? DEFAULT_URL) {
  return new DataSource({
    type: 'postgres',
    url,
    entities: [WalletSchema],
    migrations: [Init1793700000000],
    synchronize: false,
    logging: false,
    extra: {
      max: 20,
      connectionTimeoutMillis: 3000,
      statement_timeout: 15000,
      idle_in_transaction_session_timeout: 20000,
    },
  });
}
