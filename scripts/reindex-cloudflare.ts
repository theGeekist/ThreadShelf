import '../src/env.js';
import { createHash, randomUUID } from 'node:crypto';
import { connect, type Connection, type Table } from '@lancedb/lancedb';
import { mkdir, readFile, rename, stat, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { dataPath } from '../src/paths.js';
import {
  CloudflareRateLimitError,
  embedWithCloudflare,
  getCloudflareEmbeddingProfile,
} from '../src/cloudflare-embedding.js';
import {
  INDEX_COLLECTION_ARCHIVE,
  INDEX_METHOD_ARCHIVE_COPY,
  INDEX_METHOD_SEMANTIC,
  createIndexLedgerCheckpoint,
  indexLedgerKey,
  type IndexLedgerCheckpoint,
  type IndexLedgerIdentity,
} from '../src/index-ledger.js';

interface ReindexLedger {
  readonly version: 2;
  readonly source: string;
  readonly sourceFingerprint: string;
  jobs: Record<string, IndexLedgerCheckpoint>;
  complete: boolean;
}

const sourceDefault = resolve(process.env.LANCEDB_PATH || dataPath('lancedb'));
const targetDefault = resolve(process.env.BGE_M3_LANCEDB_PATH || `${sourceDefault}-bge-m3`);
const defaultBatchSize = Math.max(
  1,
  Math.min(128, Number(process.env.CLOUDFLARE_EMBED_BATCH_SIZE) || 64),
);
const args = process.argv.slice(2);
const option = (name: string): string | undefined => {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
};
const source = resolve(option('--source') || sourceDefault);
const target = resolve(option('--target') || targetDefault);
const configuredBatchSize = Number(option('--batch-size'));
const batchLimit =
  Number.isFinite(configuredBatchSize) && configuredBatchSize > 0
    ? Math.min(128, Math.floor(configuredBatchSize))
    : defaultBatchSize;
const ledgerPath = `${target}.reindex.json`;

if (args.includes('--help')) {
  console.log(
    'Usage: npm run reindex:cloudflare -- [--source PATH] [--target PATH] [--batch-size N]\n' +
      'Streams current LanceDB chunk text through an OpenAI-compatible embeddings endpoint.\n' +
      'The destination and its small resumable ledger are retained if the process stops.',
  );
  process.exit(0);
}

if (source === target) throw new Error('Source and target database paths must differ');
const embeddingProfile = getCloudflareEmbeddingProfile();
const profileFingerprint = createHash('sha256')
  .update(JSON.stringify(embeddingProfile))
  .digest('hex');
const targetModelIdentity = JSON.stringify({
  provider: 'cloudflare-openai-compatible',
  profile: profileFingerprint,
});

const pause = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

const embedBatch = async (
  texts: readonly string[],
  onRateLimit: (retryAt: number, message: string) => Promise<void>,
): Promise<number[][]> => {
  for (;;) {
    try {
      return await embedWithCloudflare(texts);
    } catch (error) {
      if (!(error instanceof CloudflareRateLimitError)) throw error;
      const waitMs = Math.max(0, error.retryAt - Date.now());
      console.warn(
        `[reindex] Cloudflare rate limit ${error.code ?? ''}; retry scheduled for ${new Date(error.retryAt).toISOString()}`,
      );
      await onRateLimit(error.retryAt, error.message);
      await pause(waitMs);
      await onRateLimit(0, '');
    }
  }
};

const persistLedger = async (ledger: ReindexLedger): Promise<void> => {
  const temporaryPath = `${ledgerPath}.${randomUUID()}.tmp`;
  await writeFile(temporaryPath, `${JSON.stringify(ledger)}\n`, { mode: 0o600 });
  await rename(temporaryPath, ledgerPath);
};

const readLedger = async (): Promise<ReindexLedger | undefined> => {
  try {
    return JSON.parse(await readFile(ledgerPath, 'utf8')) as ReindexLedger;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
    throw error;
  }
};

const sourceVersionFingerprint = async (
  sourceDb: Connection,
  tableNames: readonly string[],
): Promise<string> => {
  const versions: [string, number][] = [];
  for (const name of tableNames)
    versions.push([name, await (await sourceDb.openTable(name)).version()]);
  return createHash('sha256').update(JSON.stringify(versions)).digest('hex');
};

const inspectTarget = async (
  targetDb: Connection,
  ledger: ReindexLedger | undefined,
): Promise<void> => {
  let targetExists = false;
  try {
    const info = await stat(target);
    targetExists = true;
    if (!info.isDirectory())
      throw new Error(`Target path exists and is not a directory: ${target}`);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  const names = targetExists ? await targetDb.tableNames() : [];
  if (names.length > 0 && !ledger) {
    throw new Error(
      `Target contains partial or unrelated tables but has no resumable ledger. Choose a new empty target: ${target}`,
    );
  }
  if (ledger) {
    const checkpointedTables = Object.values(ledger.jobs)
      .filter((progress) => progress.cursor > 0 || progress.complete)
      .map((progress) => progress.scope);
    const missing = checkpointedTables.filter((name) => !names.includes(name));
    if (missing.length) {
      throw new Error(
        `Target is missing tables recorded by the checkpoint (${missing.join(', ')}); choose a new target`,
      );
    }
    const ledgerScopes = new Set(Object.values(ledger.jobs).map((progress) => progress.scope));
    const untracked = names.filter((name) => !ledgerScopes.has(name));
    if (untracked.length) {
      throw new Error(
        `Target contains tables not recorded by the checkpoint (${untracked.join(', ')}); choose a new target`,
      );
    }
  }
};

const assertArchiveReady = async (sourceDb: Connection): Promise<void> => {
  const threads = await sourceDb.openTable('__threads');
  for await (const recordBatch of threads.query().select(['sourceFile', 'indexPending'])) {
    for (const row of recordBatch.toArray() as Record<string, unknown>[]) {
      if (String(row.indexPending || '')) {
        throw new Error(
          `Cannot reindex while thread ${String(row.sourceFile)} has a pending index job. Finish recovery first.`,
        );
      }
    }
  }
};

const upsertRows = async (
  targetDb: Connection,
  tableName: string,
  rows: Record<string, unknown>[],
  mergeKeys: string | string[],
): Promise<Table> => {
  let table: Table;
  const names = await targetDb.tableNames();
  if (!names.includes(tableName)) {
    table = await targetDb.createTable(tableName, rows);
  } else {
    table = await targetDb.openTable(tableName);
    await table
      .mergeInsert(mergeKeys)
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute(rows);
  }
  return table;
};

const streamTable = async (
  sourceDb: Connection,
  targetDb: Connection,
  tableName: string,
  ledger: ReindexLedger,
  mode: 'vectors' | 'threads',
): Promise<void> => {
  const sourceTable = await sourceDb.openTable(tableName);
  const identity: IndexLedgerIdentity =
    mode === 'vectors'
      ? { collection: tableName, model: targetModelIdentity, method: INDEX_METHOD_SEMANTIC }
      : {
          collection: INDEX_COLLECTION_ARCHIVE,
          model: targetModelIdentity,
          method: INDEX_METHOD_ARCHIVE_COPY,
        };
  const key = indexLedgerKey(identity, tableName);
  let progress = ledger.jobs[key] ?? createIndexLedgerCheckpoint(identity, tableName);
  ledger.jobs[key] = progress;
  await persistLedger(ledger);
  if (progress.complete) return;
  if (progress.retryAt > Date.now()) await pause(progress.retryAt - Date.now());
  if (progress.retryAt > 0) {
    progress = createIndexLedgerCheckpoint(identity, tableName, {
      cursor: progress.cursor,
      attempts: progress.attempts,
    });
    ledger.jobs[key] = progress;
    await persistLedger(ledger);
  }

  let sourceRowsSeen = 0;
  let batch: Record<string, unknown>[] = [];
  const limit = mode === 'vectors' ? batchLimit : 256;
  const flush = async (): Promise<void> => {
    if (batch.length === 0) return;
    let rows = batch;
    if (mode === 'vectors') {
      const vectors = await embedBatch(
        rows.map((row) => String(row.document)),
        async (retryAt, error) => {
          progress = createIndexLedgerCheckpoint(identity, tableName, {
            cursor: progress.cursor,
            attempts: 0,
            retryAt,
            error,
          });
          ledger.jobs[key] = progress;
          await persistLedger(ledger);
        },
      );
      rows = rows.map((row, index) => ({ ...row, vector: vectors[index]! }));
    }
    const keys = mode === 'vectors' ? 'id' : ['collection', 'sourceFile', 'conversationKey'];
    await upsertRows(targetDb, tableName, rows, keys);
    progress = createIndexLedgerCheckpoint(identity, tableName, {
      cursor: progress.cursor + batch.length,
      attempts: progress.attempts,
    });
    ledger.jobs[key] = progress;
    batch = [];
    await persistLedger(ledger);
    console.log(`[reindex] ${tableName}: ${progress.cursor} rows written`);
  };

  for await (const recordBatch of sourceTable.query()) {
    const rows = recordBatch.toArray() as Record<string, unknown>[];
    for (const row of rows) {
      sourceRowsSeen++;
      if (sourceRowsSeen <= progress.cursor) continue;
      if (mode === 'threads' && String(row.indexPending || '')) {
        throw new Error(
          `Thread ${String(row.sourceFile)} gained a pending index job during reindex`,
        );
      }
      if (mode === 'vectors' && (typeof row.document !== 'string' || row.document.length === 0)) {
        throw new Error(`Table ${tableName} has a chunk without document text; cannot re-embed it`);
      }
      batch.push(row);
      if (batch.length >= limit) await flush();
    }
  }
  await flush();
  if (sourceRowsSeen < progress.cursor) {
    throw new Error(
      `Source table ${tableName} has fewer rows than the saved checkpoint; source changed during reindex`,
    );
  }
  progress = createIndexLedgerCheckpoint(identity, tableName, {
    cursor: sourceRowsSeen,
    complete: true,
  });
  ledger.jobs[key] = progress;
  await persistLedger(ledger);
};

const sourceDb = await connect(source, { readConsistencyInterval: 0 });
const names = await sourceDb.tableNames();
if (!names.includes('__threads'))
  throw new Error(`Source database has no __threads archive table: ${source}`);
const vectorTables = names.filter((name) => !name.startsWith('__'));
if (vectorTables.length === 0) throw new Error('Source database has no collection vector tables');
await assertArchiveReady(sourceDb);
const sourceFingerprint = await sourceVersionFingerprint(sourceDb, names);

let ledger = await readLedger();
if (ledger) {
  if (
    ledger.version !== 2 ||
    ledger.source !== source ||
    ledger.sourceFingerprint !== sourceFingerprint ||
    !Object.values(ledger.jobs).every((job) => job.model === targetModelIdentity)
  ) {
    throw new Error(
      'Saved reindex ledger does not match this source/model; use a new empty target path',
    );
  }
} else {
  ledger = {
    version: 2,
    source,
    sourceFingerprint,
    jobs: {},
    complete: false,
  };
}

await mkdir(dirname(target), { recursive: true, mode: 0o700 });
const targetDb = await connect(target, { readConsistencyInterval: 0 });
await inspectTarget(targetDb, ledger);
if (ledger.complete) {
  console.log(`[reindex] already complete: ${target}`);
  process.exit(0);
}
await persistLedger(ledger);

console.log(`[reindex] source: ${source}`);
console.log(`[reindex] target: ${target}`);
console.log(`[reindex] model: ${embeddingProfile.model}, batch size: ${batchLimit}`);
for (const name of vectorTables) await streamTable(sourceDb, targetDb, name, ledger, 'vectors');
await streamTable(sourceDb, targetDb, '__threads', ledger, 'threads');
ledger.complete = true;
await persistLedger(ledger);
console.log(`[reindex] complete: ${target}`);
console.log('[reindex] Keep the resumable ledger until the target index is activated.');
