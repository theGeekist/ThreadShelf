import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { totalmem, tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';

interface BenchChunk {
  readonly id: string;
  readonly text: string;
}

interface BenchRow extends Record<string, unknown> {
  readonly id: string;
  readonly vector: number[];
  readonly document: string;
  readonly sourceFile: string;
  readonly provider: string;
  readonly conversationKey: string;
  readonly title: string;
  readonly role: string;
  readonly turnIndex: string;
  readonly model: string;
  readonly createdAt: string;
  readonly createdInThreadShelf: boolean;
  readonly generationProvider: string;
}

interface BenchResult {
  readonly mode: 'whole-array' | 'stream';
  readonly conversations: number;
  readonly chunks: number;
  readonly textCharsPerChunk: number;
  readonly maxBatchSize: number;
  readonly paddedTokenBudget: number;
  readonly maxInputTokens: number;
  readonly totalMs: number;
  readonly embeddingMs: number;
  readonly storageMs: number;
  readonly chunksPerSecond: number;
  readonly rssBeforeMiB: number;
  readonly peakRssMiB: number;
  readonly peakRssDeltaMiB: number;
  readonly heapBeforeMiB: number;
  readonly peakHeapMiB: number;
}

const scriptPath = fileURLToPath(import.meta.url);
const conversations = positiveInt(process.env.BENCH_CONVERSATIONS, 5500);
const chunksPerConversation = positiveInt(process.env.BENCH_CHUNKS_PER_CONVERSATION, 4);
const textCharsPerChunk = positiveInt(process.env.BENCH_TEXT_CHARS, 240);
const maxBatchSize = positiveInt(process.env.EMBED_BATCH_SIZE, 25);
const paddedTokenBudget = positiveInt(process.env.EMBED_BATCH_TOKEN_BUDGET, 2048);

function positiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function* makeChunks(): Generator<BenchChunk> {
  const seed =
    'A conversation discusses project planning, technical research, writing, and local data. ';
  for (let conversation = 0; conversation < conversations; conversation++) {
    for (let turn = 0; turn < chunksPerConversation; turn++) {
      const prefix = `Conversation ${conversation}, turn ${turn}: `;
      const repeated = seed.repeat(Math.ceil(textCharsPerChunk / seed.length));
      yield {
        id: `synthetic.json|conversation-${conversation}|${turn}|0`,
        text: `${prefix}${repeated}`.slice(0, textCharsPerChunk),
      };
    }
  }
}

function rowFrom(chunk: BenchChunk, vector: number[]): BenchRow {
  const [, conversationKey = '', turnIndex = '0'] = chunk.id.split('|');
  return {
    id: chunk.id,
    vector,
    document: chunk.text,
    sourceFile: 'synthetic.json',
    provider: 'openai',
    conversationKey,
    title: `Synthetic conversation ${conversationKey}`,
    role: Number(turnIndex) % 2 === 0 ? 'user' : 'ai',
    turnIndex,
    model: '',
    createdAt: '',
    createdInThreadShelf: false,
    generationProvider: '',
  };
}

async function measure(mode: 'whole-array' | 'stream'): Promise<BenchResult> {
  const root = await mkdtemp(join(tmpdir(), 'threadshelf-stream-bench-'));
  process.env.LANCEDB_PATH = join(root, 'db');
  const [{ connect }, { embed, getEmbeddingLimits }, { streamEmbeddingsToStore }] =
    await Promise.all([
      import('@lancedb/lancedb'),
      import('../src/embedding.js'),
      import('../src/embedding-stream.js'),
    ]);
  const limits = await getEmbeddingLimits();
  await embed(['Warm up the local embedding model before measurement.']);
  const db = await connect(process.env.LANCEDB_PATH);
  const chunks = conversations * chunksPerConversation;
  const memoryBefore = process.memoryUsage();
  let peakRss = memoryBefore.rss;
  let peakHeap = memoryBefore.heapUsed;
  const sampleMemory = () => {
    const memory = process.memoryUsage();
    peakRss = Math.max(peakRss, memory.rss);
    peakHeap = Math.max(peakHeap, memory.heapUsed);
  };
  const sampler = setInterval(sampleMemory, 10);
  sampler.unref();
  let embeddingMs = 0;
  let storageMs = 0;
  const startedAt = performance.now();

  try {
    if (mode === 'whole-array') {
      const source = [...makeChunks()];
      const rows: BenchRow[] = [];
      for (let index = 0; index < source.length; index += maxBatchSize) {
        const batch = source.slice(index, index + maxBatchSize);
        const embeddingStarted = performance.now();
        const vectors = await embed(batch.map((chunk) => chunk.text));
        embeddingMs += performance.now() - embeddingStarted;
        rows.push(...batch.map((chunk, vectorIndex) => rowFrom(chunk, vectors[vectorIndex]!)));
      }
      const storageStarted = performance.now();
      await db.createTable('benchmark', rows);
      storageMs = performance.now() - storageStarted;
    } else {
      let table: Awaited<ReturnType<typeof db.openTable>> | undefined;
      await streamEmbeddingsToStore({
        source: makeChunks(),
        maxBatchSize,
        paddedTokenBudget,
        maxInputTokens: limits.maxInputTokens,
        countTokens: limits.countTokens,
        embedBatch: async (batch) => {
          const embeddingStarted = performance.now();
          const vectors = await embed(batch.map((chunk) => chunk.text));
          embeddingMs += performance.now() - embeddingStarted;
          return batch.map((chunk, vectorIndex) => rowFrom(chunk, vectors[vectorIndex]!));
        },
        writeBatch: async (rows) => {
          const storageStarted = performance.now();
          if (!table) table = await db.createTable('benchmark', [...rows]);
          else {
            await table.checkoutLatest();
            await table
              .mergeInsert('id')
              .whenMatchedUpdateAll()
              .whenNotMatchedInsertAll()
              .execute([...rows]);
          }
          storageMs += performance.now() - storageStarted;
        },
      });
    }
  } finally {
    sampleMemory();
    clearInterval(sampler);
  }

  const totalMs = performance.now() - startedAt;
  const peakRssMiB = peakRss / 1024 / 1024;
  const result: BenchResult = {
    mode,
    conversations,
    chunks,
    textCharsPerChunk,
    maxBatchSize,
    paddedTokenBudget,
    maxInputTokens: limits.maxInputTokens,
    totalMs: Math.round(totalMs),
    embeddingMs: Math.round(embeddingMs),
    storageMs: Math.round(storageMs),
    chunksPerSecond: Math.round((chunks / totalMs) * 1000),
    rssBeforeMiB: Math.round((memoryBefore.rss / 1024 / 1024) * 10) / 10,
    peakRssMiB: Math.round(peakRssMiB * 10) / 10,
    peakRssDeltaMiB: Math.round(((peakRss - memoryBefore.rss) / 1024 / 1024) * 10) / 10,
    heapBeforeMiB: Math.round((memoryBefore.heapUsed / 1024 / 1024) * 10) / 10,
    peakHeapMiB: Math.round((peakHeap / 1024 / 1024) * 10) / 10,
  };
  await rm(root, { recursive: true, force: true });
  return result;
}

async function runWorker(mode: 'whole-array' | 'stream'): Promise<void> {
  const result = await measure(mode);
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

function runProcess(mode: 'whole-array' | 'stream'): Promise<BenchResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--import', 'tsx', scriptPath, `--worker=${mode}`], {
      env: process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderrTail = '';
    child.stdout.setEncoding('utf8').on('data', (part: string) => (stdout += part));
    child.stderr.setEncoding('utf8').on('data', (part: string) => {
      stderrTail = `${stderrTail}${part}`.slice(-3000);
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`${mode} benchmark exited ${code}:\n${stderrTail}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout.trim()) as BenchResult);
      } catch (error) {
        reject(new Error(`Could not parse ${mode} benchmark output: ${String(error)}\n${stdout}`));
      }
    });
  });
}

const worker = process.argv.find((argument) => argument.startsWith('--worker='))?.slice(9);
if (worker === 'whole-array' || worker === 'stream') {
  await runWorker(worker);
} else {
  const hostMemoryMiB = Math.round((totalmem() / 1024 / 1024) * 10) / 10;
  const cpu = process.env.PROCESSOR_IDENTIFIER || process.arch;
  console.log(
    `Synthetic ingest benchmark: ${conversations.toLocaleString()} conversations × ` +
      `${chunksPerConversation} chunks, ${textCharsPerChunk} chars/chunk; ` +
      `Node ${process.version}, ${cpu}, host RAM ${hostMemoryMiB} MiB.`,
  );
  const results: BenchResult[] = [];
  for (const mode of ['whole-array', 'stream'] as const) {
    console.log(`Running ${mode}…`);
    results.push(await runProcess(mode));
    console.log(JSON.stringify(results.at(-1), null, 2));
  }
  const [before, after] = results;
  console.log(
    JSON.stringify(
      {
        comparison: {
          peakRssReductionMiB: Math.round((before!.peakRssMiB - after!.peakRssMiB) * 10) / 10,
          peakRssReductionPercent: Math.round(
            (100 * (before!.peakRssMiB - after!.peakRssMiB)) / before!.peakRssMiB,
          ),
          totalTimeChangePercent: Math.round(
            (100 * (after!.totalMs - before!.totalMs)) / before!.totalMs,
          ),
        },
      },
      null,
      2,
    ),
  );
}
