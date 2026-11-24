import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { streamEmbeddingsToStore } from '../src/embedding-stream.js';

const deferred = () => {
  let resolve;
  const promise = new Promise((done) => (resolve = done));
  return { promise, resolve };
};

describe('streamEmbeddingsToStore', () => {
  it('lets inference continue while a LanceDB write is pending, within stream buffers', async () => {
    const chunks = ['aaaa', 'bbbb', 'cc', 'dddd', 'ee'].map((text) => ({ text }));
    const committed = [];
    const embedded = [];
    const firstWriteStarted = deferred();
    const releaseFirstWrite = deferred();

    const count = await streamEmbeddingsToStore({
      source: chunks,
      maxBatchSize: 10,
      paddedTokenBudget: 8,
      maxInputTokens: 8,
      countTokens: (text) => text.length,
      embedBatch: async (batch) => {
        embedded.push(batch.map(({ text }) => text));
        if (embedded.length === 2) releaseFirstWrite.resolve();
        return batch.map(({ text }) => text.toUpperCase());
      },
      writeBatch: async (rows) => {
        if (committed.length === 0) {
          firstWriteStarted.resolve();
          await releaseFirstWrite.promise;
        }
        await new Promise((resolve) => setImmediate(resolve));
        committed.push(rows);
      },
    });

    await firstWriteStarted.promise;
    assert.equal(count, chunks.length);
    assert.deepEqual(embedded, [['aaaa', 'bbbb'], ['cc', 'dddd'], ['ee']]);
    assert.deepEqual(committed, [['AAAA', 'BBBB'], ['CC', 'DDDD'], ['EE']]);
  });

  it('rejects chunks that exceed the active model context limit', async () => {
    await assert.rejects(
      streamEmbeddingsToStore({
        source: [{ text: 'too long' }],
        maxBatchSize: 2,
        paddedTokenBudget: 8,
        maxInputTokens: 4,
        countTokens: (text) => text.length,
        embedBatch: async () => [],
        writeBatch: async () => {},
      }),
      /active embedding model accepts 4/u,
    );
  });
});
