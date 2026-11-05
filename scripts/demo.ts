import { randomUUID } from 'node:crypto';

const base = process.env.API_URL ?? 'http://localhost:3000';
const PARALLEL_REQUESTS = 50;

async function request(path: string, body?: unknown, key?: string) {
  const response = await fetch(`${base}${path}`, {
    method: body ? 'POST' : 'GET',
    headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await response.json();
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(data)}`);
  return data as Record<string, any>;
}

const wallet = await request('/wallets', {
  playerId: randomUUID(),
  initialBalance: { amount: '100.00', currency: 'BRL' },
});

const externalTransactionId = randomUUID();
const input = {
  providerId: 'demo-provider',
  externalTransactionId,
  playerId: wallet.playerId,
  walletId: wallet.id,
  roundId: 'demo-round',
  gameId: 'demo-game',
  kind: 'BET',
  money: { amount: '80.00', currency: 'BRL' },
};

const results = await Promise.all(
  Array.from({ length: PARALLEL_REQUESTS }, () =>
    request('/wagering/transactions', input, `demo-provider:${externalTransactionId}`),
  ),
);

const ledger = await request(`/wallets/${wallet.id}/ledger`);
const reconciliation = await request(`/wallets/${wallet.id}/reconciliation`, {});

const uniqueTransactions = new Set(results.map(result => result.transactionId)).size;
const replays = results.filter(result => result.idempotentReplay).length;

if (
  uniqueTransactions !== 1 ||
  replays !== PARALLEL_REQUESTS - 1 ||
  reconciliation.storedBalance.amount !== '20.00' ||
  !reconciliation.consistent
) {
  throw new Error('Financial invariant failed');
}

console.log(
  JSON.stringify(
    {
      walletId: wallet.id,
      parallelRequests: results.length,
      uniqueTransactions,
      replays,
      ledger: ledger.entries,
      reconciliation,
    },
    null,
    2,
  ),
);
