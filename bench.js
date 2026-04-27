'use strict';

const path = require('path');
const grpc = require('./gen/node/node_modules/@grpc/grpc-js');
const protoLoader = require('./gen/node/node_modules/@grpc/proto-loader');

const PROTO_PATH = path.join(__dirname, 'proto/henchman/v1/cache.proto');
const ADDRESS = process.env.HENCH_ADDR || 'localhost:6474';
const N = 1_000;
const SEED_CONCURRENCY = 200;
const VALUE = Buffer.from('benchmark-value-payload-xxxxxxxxxxxxxxxxxxxxxxxxxxxx');
// Pool of independent client connections. A single grpc-js Client is one HTTP/2
// connection sharing one Node event loop; that caps throughput long before the
// server saturates. Spreading concurrent workers across a pool removes the
// client-side bottleneck so the numbers reflect actual server throughput.
const POOL_SIZE = Number(process.env.HENCH_POOL_SIZE || 8);

// ── gRPC setup ───────────────────────────────────────────────────────────────

const pkgDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [path.join(__dirname, 'proto')],
});
const { henchman: { v1: { Cache } } } = grpc.loadPackageDefinition(pkgDef);

function rpc(client, method, request, meta) {
  return new Promise((resolve, reject) => {
    client[method](request, meta, (err, res) => (err ? reject(err) : resolve(res)));
  });
}

// ── Stats ────────────────────────────────────────────────────────────────────

function percentile(sorted, p) {
  return sorted[Math.ceil(sorted.length * p / 100) - 1];
}

function stats(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  const mean = samples.reduce((s, v) => s + v, 0) / samples.length;
  return {
    mean,
    p50: percentile(sorted, 50),
    p90: percentile(sorted, 90),
    p99: percentile(sorted, 99),
  };
}

// ── Table printer ────────────────────────────────────────────────────────────

function printTable(rows) {
  const fmt = (n) => n.toFixed(3);
  const headers = ['Benchmark', 'n', 'TPS', 'Mean (ms)', 'P50 (ms)', 'P90 (ms)', 'P99 (ms)'];
  const data = [
    headers,
    ...rows.map(r => [r.name, String(r.n), `${Math.round(r.tps)} tx/s`, fmt(r.mean), fmt(r.p50), fmt(r.p90), fmt(r.p99)]),
  ];

  const widths = headers.map((_, i) => Math.max(...data.map(row => row[i].length)));
  const sep = '+' + widths.map(w => '-'.repeat(w + 2)).join('+') + '+';
  const line = (row) => '| ' + row.map((cell, i) => cell.padEnd(widths[i])).join(' | ') + ' |';

  console.log(sep);
  data.forEach((row, i) => {
    console.log(line(row));
    if (i === 0) console.log(sep);
  });
  console.log(sep);
}

// ── Seeding helper (concurrent batches) ──────────────────────────────────────

async function seed(client, meta, prefix, count) {
  process.stdout.write(`  Seeding ${count} keys [${prefix}*] ... `);
  for (let i = 0; i < count; i += SEED_CONCURRENCY) {
    const batch = [];
    for (let j = i; j < Math.min(i + SEED_CONCURRENCY, count); j++) {
      batch.push(rpc(client, 'set', { key: `${prefix}${j}`, value: VALUE, ttlMs: 0, invalidateAfterRead: false }, meta));
    }
    await Promise.all(batch);
  }
  console.log('done');
}

// ── Benchmark runners ────────────────────────────────────────────────────────

async function bench(name, n, fn) {
  process.stdout.write(`  Running: ${name} ... `);
  const samples = [];
  const wall0 = process.hrtime.bigint();
  for (let i = 0; i < n; i++) {
    const t0 = process.hrtime.bigint();
    await fn(i);
    samples.push(Number(process.hrtime.bigint() - t0) / 1e6);
  }
  const wallMs = Number(process.hrtime.bigint() - wall0) / 1e6;
  console.log('done');
  return { name, n, tps: n / (wallMs / 1000), ...stats(samples) };
}

async function benchConcurrent(name, n, concurrency, fn) {
  process.stdout.write(`  Running: ${name} [c=${concurrency}] ... `);
  const samples = new Array(n);
  let next = 0;

  // workerIdx is passed to fn so each worker can pick a distinct client from
  // the pool; this is what spreads load across connections.
  async function worker(workerIdx) {
    while (next < n) {
      const i = next++;
      const t0 = process.hrtime.bigint();
      await fn(i, workerIdx);
      samples[i] = Number(process.hrtime.bigint() - t0) / 1e6;
    }
  }

  const wall0 = process.hrtime.bigint();
  await Promise.all(Array.from({ length: concurrency }, (_, w) => worker(w)));
  const wallMs = Number(process.hrtime.bigint() - wall0) / 1e6;
  console.log('done');
  return { name: `${name} [c=${concurrency}]`, n, tps: n / (wallMs / 1000), ...stats(samples) };
}

// ── Main ─────────────────────────────────────────────────────────────────────

function waitReady(client, timeoutMs) {
  return new Promise((resolve, reject) => {
    client.waitForReady(Date.now() + timeoutMs, (err) => err ? reject(err) : resolve());
  });
}

// Every token we hand out gets tracked here so we can release the server-side
// allocation on any exit path — normal return, thrown error, or signal.
const liveTokens = new Set();

async function flushToken(client, token) {
  const meta = new grpc.Metadata();
  meta.set('x-hench-token', token);
  await rpc(client, 'flush', {}, meta).catch(() => {});
  liveTokens.delete(token);
}

async function runSuite(pool, policy) {
  // Register once via pool[0]; every client in the pool reuses the same token,
  // so all connections target the same cache instance.
  const setup = pool[0];
  const { token } = await rpc(setup, 'register', {
    instanceId: `bench-${policy}-${Date.now()}`,
    config: {
      maxBytes: 256 * 1024 * 1024,
      maxKeys: N * 4,
      defaultTtlMs: 300_000,
      evictionPolicy: policy,
      sweepIntervalMs: 60_000,
      shardCount: 32
    },
  }, new grpc.Metadata());
  liveTokens.add(token);

  const meta = new grpc.Metadata();
  meta.set('x-hench-token', token);

  try {
    process.stdout.write(`\nWarming up [${policy}] ... `);
    const warmupVal = Buffer.from('w');
    for (let i = 0; i < 50; i++) {
      await rpc(setup, 'set', { key: `__warmup__${i}`, value: warmupVal, ttlMs: 0, invalidateAfterRead: false }, meta);
      await rpc(setup, 'get', { key: `__warmup__${i}` }, meta);
    }
    console.log('done');

    console.log(`\nPreparing benchmarks [${policy}] ...`);

    await seed(setup, meta, 'single:', 1);
    const singleRead = await bench('Single read', 100, () =>
      rpc(setup, 'get', { key: 'single:0' }, meta)
    );

    await seed(setup, meta, 'hit:', N);
    const hitReads = await bench(`${N} reads (100% hit)`, N, (i) =>
      rpc(setup, 'get', { key: `hit:${i}` }, meta)
    );

    await seed(setup, meta, 'mix:', N / 2);
    const mixReads = await bench(`${N} reads (50% hit/miss)`, N, (i) =>
      rpc(setup, 'get', { key: `mix:${i}` }, meta)
    );

    const writes = await bench(`${N} writes`, N, (i) =>
      rpc(setup, 'set', { key: `write:${i}`, value: VALUE, ttlMs: 0, invalidateAfterRead: false }, meta)
    );

    const pick = (w) => pool[w % pool.length];

    const CONCURRENCIES = [10, 50, 100, 500, 1000];
    const concurrentResults = [];
    for (const c of CONCURRENCIES) {
      concurrentResults.push(await benchConcurrent(`${N} reads (100% hit)`, N, c, (i, w) =>
        rpc(pick(w), 'get', { key: `hit:${i % N}` }, meta)
      ));
      concurrentResults.push(await benchConcurrent(`${N} reads (50% hit/miss)`, N, c, (i, w) =>
        rpc(pick(w), 'get', { key: `mix:${i % N}` }, meta)
      ));
    }

    return [singleRead, hitReads, mixReads, writes, ...concurrentResults];
  } finally {
    await flushToken(setup, token);
  }
}

function makeClient(id) {
  // grpc-js coalesces channels by serialized args; varying `unique-id` and
  // forcing a local subchannel pool guarantees each Client gets its own
  // HTTP/2 connection.
  return new Cache(ADDRESS, grpc.credentials.createInsecure(), {
    'grpc.use_local_subchannel_pool': 1,
    'unique-id': id,
  });
}

async function cleanup(pool) {
  if (liveTokens.size > 0) {
    process.stdout.write(`\nCleaning up ${liveTokens.size} instance(s) ... `);
    await Promise.all(
      [...liveTokens].map((t) => flushToken(pool[0], t))
    );
    console.log('done');
  }
  for (const c of pool) c.close();
}

async function main() {
  const pool = Array.from({ length: POOL_SIZE }, (_, i) => makeClient(i));

  process.stdout.write(`\nConnecting ${POOL_SIZE} client(s) to ${ADDRESS} ... `);
  await Promise.all(pool.map((c) => waitReady(c, 5000)));
  console.log('ready');

  // Best-effort cleanup if the process is interrupted. SIGINT/SIGTERM bypasses
  // try/finally, so we flush whatever tokens are still tracked and exit.
  let cleaningUp = false;
  const onSignal = async (sig) => {
    if (cleaningUp) return;
    cleaningUp = true;
    console.log(`\nReceived ${sig}, cleaning up...`);
    await cleanup(pool).catch(() => {});
    process.exit(130);
  };
  process.on('SIGINT', () => onSignal('SIGINT'));
  process.on('SIGTERM', () => onSignal('SIGTERM'));

  try {
    const policies = ['lru', 'lfu'];
    const resultsByPolicy = {};
    for (const policy of policies) {
      resultsByPolicy[policy] = await runSuite(pool, policy);
    }

    for (const policy of policies) {
      console.log(`\n── Latency Results [${policy.toUpperCase()}] ─────────────────────────────────\n`);
      printTable(resultsByPolicy[policy]);
    }
    console.log();
  } finally {
    await cleanup(pool);
  }
}

main().catch((err) => {
  console.error('\nError:', err.message ?? err);
  process.exit(1);
});
