import { Readable, Transform, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

interface EmbeddedBatch<Chunk, Row> {
  readonly rows: readonly Row[];
  readonly chunks: readonly Chunk[];
  readonly chunkCount: number;
}

const STREAM_BATCH_HIGH_WATER_MARK = 2;

export interface EmbeddingStreamOptions<Chunk extends { readonly text: string }, Row> {
  readonly source: Iterable<Chunk> | AsyncIterable<Chunk>;
  readonly signal?: AbortSignal;
  readonly maxBatchSize: number;
  readonly paddedTokenBudget: number;
  readonly maxInputTokens: number;
  readonly countTokens: (text: string) => number;
  readonly embedBatch: (chunks: readonly Chunk[]) => Promise<readonly Row[]>;
  readonly writeBatch: (rows: readonly Row[]) => Promise<void>;
  readonly onBatchCommitted?: (
    chunksWritten: number,
    batch: readonly Chunk[],
  ) => void | Promise<void>;
}

/**
 * Stream chunks through bounded model batches. The writable queue applies
 * ordinary stream backpressure only when LanceDB cannot keep up.
 */
export const streamEmbeddingsToStore = async <Chunk extends { readonly text: string }, Row>(
  options: EmbeddingStreamOptions<Chunk, Row>,
): Promise<number> => {
  const maxBatchSize = Math.floor(options.maxBatchSize);
  const maxInputTokens = Math.floor(options.maxInputTokens);
  const paddedTokenBudget = Math.max(maxInputTokens, Math.floor(options.paddedTokenBudget));
  if (maxBatchSize < 1) throw new RangeError('maxBatchSize must be positive');
  if (maxInputTokens < 1) throw new RangeError('maxInputTokens must be positive');

  let chunksWritten = 0;
  const source = Readable.from(options.source, { objectMode: true, highWaterMark: 1 });
  let batch: Chunk[] = [];
  let maxBatchChunkTokens = 0;
  const model = new Transform({
    objectMode: true,
    readableHighWaterMark: STREAM_BATCH_HIGH_WATER_MARK,
    writableHighWaterMark: 1,
    transform(chunk: Chunk, _encoding, callback) {
      const processChunk = async () => {
        options.signal?.throwIfAborted();
        const tokenCount = options.countTokens(chunk.text);
        if (tokenCount > maxInputTokens) {
          throw new RangeError(
            `A chunk has ${tokenCount} tokens; the active embedding model accepts ${maxInputTokens}`,
          );
        }
        const nextMaxTokens = Math.max(maxBatchChunkTokens, tokenCount);
        const exceedsLimits =
          batch.length >= maxBatchSize || (batch.length + 1) * nextMaxTokens > paddedTokenBudget;
        if (batch.length > 0 && exceedsLimits) await commitBatch(this);
        batch.push(chunk);
        maxBatchChunkTokens = Math.max(maxBatchChunkTokens, tokenCount);
      };
      void processChunk().then(() => callback(), callback);
    },
    flush(callback) {
      void commitBatch(this).then(() => callback(), callback);
    },
  });

  const commitBatch = async (stream: Transform): Promise<void> => {
    if (batch.length === 0) return;
    const current = batch;
    batch = [];
    maxBatchChunkTokens = 0;
    const rows = await options.embedBatch(current);
    if (rows.length !== current.length) {
      throw new Error(`Embedding returned ${rows.length} vectors for ${current.length} chunks`);
    }
    stream.push({
      rows,
      chunks: current,
      chunkCount: current.length,
    });
  };

  const sink = new Writable({
    objectMode: true,
    highWaterMark: STREAM_BATCH_HIGH_WATER_MARK,
    write(batch: EmbeddedBatch<Chunk, Row>, _encoding, callback) {
      void options
        .writeBatch(batch.rows)
        .then(async () => {
          chunksWritten += batch.chunkCount;
          await options.onBatchCommitted?.(chunksWritten, batch.chunks);
          callback();
        })
        .catch((error: Error) => {
          callback(error);
        });
    },
  });

  await pipeline(source, model, sink, { signal: options.signal });
  return chunksWritten;
};
