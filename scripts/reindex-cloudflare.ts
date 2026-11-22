import '../src/env.js';
import { connect, type Connection, type Table } from '@lancedb/lancedb';
import { basename, resolve } from 'node:path';
import { mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { dataPath } from '../src/paths.js';
import {
  embedWithCloudflare,
  parseCloudflareVectors,
  CLOUDFLARE_EMBEDDING_DIMENSION,
} from '../src/cloudflare-embedding.js';

const execFileAsync = promisify(execFile);

const sourcePath = resolve(process.env.LANCEDB_PATH || dataPath('lancedb'));
const targetPath = resolve(process.env.BGE_M3_LANCEDB_PATH || `${sourcePath}-bge-m3`);
const batchSize = Math.max(1, Math.min(128, Number(process.env.CLOUDFLARE_EMBED_BATCH_SIZE) || 64));
const args = process.argv.slice(2);
const option = (name: string): string | undefined => {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
};
const source = resolve(option('--source') || sourcePath);
const target = resolve(option('--target') || targetPath);
const configuredBatchSize = Number(option('--batch-size'));
const batchLimit =
  Number.isFinite(configuredBatchSize) && configuredBatchSize > 0
    ? Math.min(128, Math.floor(configuredBatchSize))
    : batchSize;

if (args.includes('--help')) {
  console.log(
    'Usage: npm run reindex:cloudflare -- [--source PATH] [--target PATH] [--batch-size N]\n' +
      'Streams existing LanceDB chunk text through Cloudflare BGE-M3 into a new database.\n' +
      'Uses API credentials or authenticated cf CLI. The destination must be empty.',
  );
  process.exit(0);
}

if (source === target) throw new Error('Source and target database paths must differ');
const hasApiCredentials = Boolean(
  process.env.CLOUDFLARE_ACCOUNT_ID?.trim() && process.env.CLOUDFLARE_API_TOKEN?.trim(),
);

const embedBatch = async (texts: readonly string[]): Promise<number[][]> => {
  if (hasApiCredentials) return embedWithCloudflare(texts);
  const privateDir = await mkdtemp(resolve(tmpdir(), 'threadshelf-bge-m3-'));
  try {
    const requestPath = resolve(privateDir, 'request.json');
    await writeFile(requestPath, JSON.stringify({ text: texts, truncate_inputs: false }), {
      mode: 0o600,
    });
    const { stdout } = await execFileAsync(
      'cf',
      ['ai', 'run', '@cf/baai/bge-m3', '--body', `@${requestPath}`, '--quiet'],
      { maxBuffer: 32 * 1024 * 1024, windowsHide: true },
    );
    const start = stdout.indexOf('{');
    const end = stdout.lastIndexOf('}');
    if (start < 0 || end < start) throw new Error('cf ai run returned no JSON response');
    const vectors = parseCloudflareVectors(JSON.parse(stdout.slice(start, end + 1)));
    if (
      vectors.length !== texts.length ||
      vectors.some((vector) => vector.length !== CLOUDFLARE_EMBEDDING_DIMENSION)
    ) {
      throw new Error(
        `cf ai run returned an unexpected embedding count for ${texts.length} inputs`,
      );
    }
    return vectors;
  } finally {
    await rm(privateDir, { recursive: true, force: true });
  }
};

const copyTable = async (
  sourceDb: Connection,
  targetDb: Connection,
  name: string,
): Promise<number> => {
  const sourceTable: Table = await sourceDb.openTable(name);
  let targetTable: Table | undefined;
  let buffer: Record<string, unknown>[] = [];
  let copied = 0;
  const flush = async (): Promise<void> => {
    if (buffer.length === 0) return;
    if (targetTable) await targetTable.add(buffer);
    else targetTable = await targetDb.createTable(name, buffer);
    copied += buffer.length;
    buffer = [];
  };
  for await (const recordBatch of sourceTable.query()) {
    const rows = recordBatch.toArray() as Record<string, unknown>[];
    for (const row of rows) {
      if (name === '__threads' && String(row.indexPending || '')) {
        throw new Error(
          `Cannot reindex while thread ${String(row.sourceFile)} has a pending index job. Finish recovery first.`,
        );
      }
      buffer.push(row);
      if (buffer.length >= 512) await flush();
    }
  }
  await flush();
  console.log(`[reindex] copied ${basename(name)}: ${copied} rows`);
  return copied;
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

const reindexVectorTable = async (
  sourceDb: Connection,
  targetDb: Connection,
  name: string,
): Promise<number> => {
  const sourceTable = await sourceDb.openTable(name);
  let targetTable: Table | undefined;
  let batch: Record<string, unknown>[] = [];
  let complete = 0;
  const flush = async (): Promise<void> => {
    if (batch.length === 0) return;
    const texts = batch.map((row) => String(row.document || ''));
    const vectors = await embedBatch(texts);
    const output = batch.map((row, index) => ({ ...row, vector: vectors[index]! }));
    if (targetTable) await targetTable.add(output);
    else targetTable = await targetDb.createTable(name, output);
    complete += output.length;
    batch = [];
    console.log(`[reindex] ${name}: ${complete} chunks embedded and written`);
  };
  for await (const recordBatch of sourceTable.query()) {
    const rows = recordBatch.toArray() as Record<string, unknown>[];
    for (const row of rows) {
      if (typeof row.document !== 'string' || row.document.length === 0) {
        throw new Error(`Table ${name} has a chunk without document text; cannot re-embed it`);
      }
      batch.push(row);
      if (batch.length >= batchLimit) await flush();
    }
  }
  await flush();
  if (complete === 0) throw new Error(`Vector table ${name} was empty`);
  return complete;
};

const ensureTargetIsEmpty = async (): Promise<void> => {
  try {
    const info = await stat(target);
    if (!info.isDirectory())
      throw new Error(`Target path exists and is not a directory: ${target}`);
    const db = await connect(target);
    const names = await db.tableNames();
    if (names.length) throw new Error(`Target database already contains tables: ${target}`);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
};

await ensureTargetIsEmpty();
await mkdir(target, { recursive: true, mode: 0o700 });
const sourceDb = await connect(source, { readConsistencyInterval: 0 });
const targetDb = await connect(target, { readConsistencyInterval: 0 });
const names = await sourceDb.tableNames();
if (!names.includes('__threads')) {
  throw new Error(`Source database has no __threads archive table: ${source}`);
}
const vectorTables = names.filter((name) => !name.startsWith('__'));
if (vectorTables.length === 0) throw new Error('Source database has no collection vector tables');
await assertArchiveReady(sourceDb);

console.log(`[reindex] source: ${source}`);
console.log(`[reindex] target: ${target}`);
console.log(
  `[reindex] model: @cf/baai/bge-m3, batch size: ${batchLimit}, auth: ${hasApiCredentials ? 'Cloudflare API token' : 'authenticated cf CLI'}`,
);
for (const name of vectorTables) await reindexVectorTable(sourceDb, targetDb, name);
await copyTable(sourceDb, targetDb, '__threads');
console.log(`[reindex] complete: ${target}`);
console.log(
  '[reindex] Activate with LANCEDB_PATH set to this path and THREADSHELF_EMBEDDING_PROVIDER=cloudflare.',
);
