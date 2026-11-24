import '../src/env.js';
import { createHash, randomUUID } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { connect, type Connection, type Table } from '@lancedb/lancedb';
import { mkdir, readFile, readdir, rename, rm, stat, writeFile } from 'node:fs/promises';
import { once } from 'node:events';
import { dirname, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { finished } from 'node:stream/promises';
import { dataPath } from '../src/paths.js';
import {
  CloudflareRateLimitError,
  embedWithCloudflare,
  getCloudflareEmbeddingProfile,
} from '../src/cloudflare-embedding.js';
import { getEmbeddingModelIdentity } from '../src/embedding.js';
import { writeIndexProfile } from '../src/index-profile.js';
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
  readonly version: 3;
  readonly source: string;
  readonly sourceFingerprint: string;
  readonly profileFingerprint: string;
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
const targetModelIdentity = getEmbeddingModelIdentity();

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

const keyManifestPath = (tableName: string): string =>
  `${ledgerPath}.${createHash('sha256').update(tableName).digest('hex').slice(0, 16)}.keys`;

const encodedKey = (row: Record<string, unknown>, mode: 'vectors' | 'threads'): string =>
  Buffer.from(
    mode === 'vectors'
      ? String(row.id)
      : JSON.stringify([row.collection, row.sourceFile, row.conversationKey]),
  ).toString('base64url');

const readLines = async function* (path: string): AsyncGenerator<string> {
  const input = createReadStream(path, { encoding: 'utf8' });
  const lines = createInterface({ input, crlfDelay: Infinity });
  try {
    for await (const line of lines) yield line;
  } finally {
    lines.close();
    input.destroy();
  }
};

const writeStreamChunk = async (stream: ReturnType<typeof createWriteStream>, text: string) => {
  if (!stream.write(text)) await once(stream, 'drain');
};

const createKeyManifest = async (
  sourceTable: Table,
  tableName: string,
  mode: 'vectors' | 'threads',
  sourceFingerprint: string,
): Promise<{ path: string; count: number }> => {
  const path = keyManifestPath(tableName);
  const header = `#ThreadShelf-key-manifest-v1 ${sourceFingerprint} ${tableName}`;
  try {
    let first = true;
    let valid = true;
    let count = 0;
    for await (const line of readLines(path)) {
      if (first) {
        first = false;
        if (line !== header) valid = false;
        continue;
      }
      count++;
      if (!valid) break;
    }
    if (valid && !first) return { path, count };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }

  const runPrefix = `${path}.run-`;
  const directory = dirname(path);
  const stale = (await readdir(directory)).filter((name) =>
    name.startsWith(runPrefix.slice(directory.length + 1)),
  );
  await Promise.all(stale.map((name) => rm(resolve(directory, name), { force: true })));

  const runPaths: string[] = [];
  let keys: string[] = [];
  const columns = mode === 'vectors' ? ['id'] : ['collection', 'sourceFile', 'conversationKey'];
  const flushRun = async (): Promise<void> => {
    if (!keys.length) return;
    keys.sort();
    const runPath = `${path}.run-${randomUUID()}`;
    await writeFile(runPath, `${keys.join('\n')}\n`, { mode: 0o600 });
    runPaths.push(runPath);
    keys = [];
  };
  for await (const batch of sourceTable.query().select(columns)) {
    for (const row of batch.toArray() as Record<string, unknown>[]) {
      keys.push(encodedKey(row, mode));
      if (keys.length >= 20_000) await flushRun();
    }
  }
  await flushRun();

  const temporaryPath = `${path}.${randomUUID()}.tmp`;
  const output = createWriteStream(temporaryPath, { flags: 'wx', mode: 0o600 });
  let mergedCount = 0;
  let previous = '';
  let outputBuffer = `${header}\n`;
  try {
    await writeStreamChunk(output, outputBuffer);
    outputBuffer = '';
    const iterators = runPaths.map((runPath) => readLines(runPath)[Symbol.asyncIterator]());
    const heads = await Promise.all(iterators.map((iterator) => iterator.next()));
    for (;;) {
      let smallest = -1;
      for (let index = 0; index < heads.length; index++) {
        if (
          !heads[index]!.done &&
          (smallest < 0 || heads[index]!.value! < heads[smallest]!.value!)
        ) {
          smallest = index;
        }
      }
      if (smallest < 0) break;
      const key = heads[smallest]!.value!;
      if (key === previous)
        throw new Error(`Source table ${tableName} contains duplicate stable row keys`);
      previous = key;
      outputBuffer += `${key}\n`;
      mergedCount++;
      heads[smallest] = await iterators[smallest]!.next();
      if (outputBuffer.length >= 64 * 1024) {
        await writeStreamChunk(output, outputBuffer);
        outputBuffer = '';
      }
    }
    if (outputBuffer) await writeStreamChunk(output, outputBuffer);
    output.end();
    await finished(output);
    await rename(temporaryPath, path);
  } catch (error) {
    output.destroy();
    await rm(temporaryPath, { force: true }).catch(() => {});
    throw error;
  } finally {
    await Promise.all(runPaths.map((runPath) => rm(runPath, { force: true })));
  }
  return { path, count: mergedCount };
};

const fetchRowsForKeys = async (
  sourceTable: Table,
  keys: readonly string[],
  mode: 'vectors' | 'threads',
  tableName: string,
): Promise<Record<string, unknown>[]> => {
  const clauses = keys.map((key) => {
    const raw = Buffer.from(key, 'base64url').toString('utf8');
    if (mode === 'vectors') return `id = '${raw.replace(/'/g, "''")}'`;
    let decoded: unknown;
    try {
      decoded = JSON.parse(raw);
    } catch {
      throw new Error(`Invalid stable key in ${tableName} migration manifest`);
    }
    if (!Array.isArray(decoded) || decoded.length !== 3)
      throw new Error(`Invalid thread key in ${tableName} migration manifest`);
    const [collection, sourceFile, conversationKey] = decoded.map((value) =>
      String(value).replace(/'/g, "''"),
    );
    return `(collection = '${collection}' AND sourceFile = '${sourceFile}' AND conversationKey = '${conversationKey}')`;
  });
  const rows = (await sourceTable
    .query()
    .where(clauses.map((clause) => `(${clause})`).join(' OR '))
    .limit(Number.MAX_SAFE_INTEGER)
    .toArray()) as Record<string, unknown>[];
  const byKey = new Map(rows.map((row) => [encodedKey(row, mode), row]));
  if (byKey.size !== keys.length || keys.some((key) => !byKey.has(key))) {
    throw new Error(`Source table ${tableName} changed while reading its stable key manifest`);
  }
  return keys.map((key) => byKey.get(key)!);
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
  const names = targetExists
    ? (await targetDb.tableNames()).filter((name) => !name.startsWith('__'))
    : [];
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
  sourceFingerprint: string,
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

  const manifest = await createKeyManifest(sourceTable, tableName, mode, sourceFingerprint);
  if (progress.cursor > manifest.count) {
    throw new Error(`Source table ${tableName} has fewer stable keys than the saved checkpoint`);
  }
  let sourceRowsSeen = 0;
  let batchKeys: string[] = [];
  const limit = mode === 'vectors' ? batchLimit : 256;
  const flush = async (): Promise<void> => {
    if (batchKeys.length === 0) return;
    let rows = await fetchRowsForKeys(sourceTable, batchKeys, mode, tableName);
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
    const committedCount = batchKeys.length;
    progress = createIndexLedgerCheckpoint(identity, tableName, {
      cursor: progress.cursor + committedCount,
      attempts: progress.attempts,
    });
    ledger.jobs[key] = progress;
    batchKeys = [];
    await persistLedger(ledger);
    console.log(`[reindex] ${tableName}: ${progress.cursor} rows written`);
  };

  let firstLine = true;
  for await (const stableKey of readLines(manifest.path)) {
    if (firstLine) {
      firstLine = false;
      continue;
    }
    sourceRowsSeen++;
    if (sourceRowsSeen <= progress.cursor) continue;
    batchKeys.push(stableKey);
    if (batchKeys.length >= limit) await flush();
  }
  await flush();
  if (sourceRowsSeen !== manifest.count) {
    throw new Error(`Source table ${tableName} manifest row count changed during reindex`);
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
    ledger.version !== 3 ||
    ledger.source !== source ||
    ledger.sourceFingerprint !== sourceFingerprint ||
    ledger.profileFingerprint !== profileFingerprint ||
    !Object.values(ledger.jobs).every((job) => job.model === targetModelIdentity)
  ) {
    throw new Error(
      'Saved reindex ledger does not match this source/model; use a new empty target path',
    );
  }
} else {
  ledger = {
    version: 3,
    source,
    sourceFingerprint,
    profileFingerprint,
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
for (const name of vectorTables) {
  await writeIndexProfile(targetDb, {
    collection: name,
    model: targetModelIdentity,
    dimension: embeddingProfile.dimension,
    method: INDEX_METHOD_SEMANTIC,
    state: 'building',
  });
}

console.log(`[reindex] source: ${source}`);
console.log(`[reindex] target: ${target}`);
console.log(`[reindex] model: ${embeddingProfile.model}, batch size: ${batchLimit}`);
for (const name of vectorTables)
  await streamTable(sourceDb, targetDb, name, ledger, 'vectors', sourceFingerprint);
await streamTable(sourceDb, targetDb, '__threads', ledger, 'threads', sourceFingerprint);
if ((await sourceVersionFingerprint(sourceDb, names)) !== sourceFingerprint) {
  throw new Error('Source database changed during reindex; the target remains unavailable');
}
for (const name of vectorTables) {
  await writeIndexProfile(targetDb, {
    collection: name,
    model: targetModelIdentity,
    dimension: embeddingProfile.dimension,
    method: INDEX_METHOD_SEMANTIC,
    state: 'ready',
  });
}
ledger.complete = true;
await persistLedger(ledger);
console.log(`[reindex] complete: ${target}`);
console.log('[reindex] Keep the resumable ledger until the target index is activated.');
