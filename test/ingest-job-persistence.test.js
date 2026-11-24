import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import process from 'node:process';

test('failed job persistence does not publish an in-memory running job', async () => {
  const root = await mkdtemp(join(tmpdir(), 'threadshelf-job-persistence-'));
  const previousPath = process.env.INGEST_JOBS_PATH;
  try {
    const exports = join(root, 'exports');
    const healthyPath = join(root, 'jobs.json');
    const blockedParent = join(root, 'not-a-directory');
    await mkdir(exports);
    await writeFile(join(exports, 'conversation.json'), '{}');
    await writeFile(blockedParent, 'file prevents creating a jobs directory');
    process.env.INGEST_JOBS_PATH = healthyPath;
    const { createIngestJob, getIngestJob } = await import('../src/services/ingest-jobs.ts');
    assert.equal(await getIngestJob('collection'), null);

    process.env.INGEST_JOBS_PATH = join(blockedParent, 'jobs.json');
    await assert.rejects(
      createIngestJob(exports, 'collection', [join(exports, 'conversation.json')]),
      /persist/i,
    );
    assert.equal(await getIngestJob('collection'), null);
  } finally {
    if (previousPath === undefined) delete process.env.INGEST_JOBS_PATH;
    else process.env.INGEST_JOBS_PATH = previousPath;
    await rm(root, { recursive: true, force: true });
  }
});
