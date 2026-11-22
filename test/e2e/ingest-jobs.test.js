import { test } from 'node:test';
import assert from 'node:assert/strict';
import { copyFile, mkdir, readFile, stat, writeFile } from 'node:fs/promises';
import { join, basename } from 'node:path';
import { startApiServer, repoRoot } from './helpers.js';

const waitForJob = async (baseUrl, collection, predicate) => {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const response = await fetch(
      `${baseUrl}/api/ingest-jobs?collection=${encodeURIComponent(collection)}`,
    );
    assert.equal(response.status, 200);
    const { job } = await response.json();
    if (predicate(job)) return job;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out waiting for ingest job in ${collection}`);
};

test('saved ingest job validates selections, persists progress, and retries failed files', async () => {
  const ctx = await startApiServer({ prefix: 'threadshelf-ingest-jobs-' });
  try {
    const folderPath = join(ctx.tempRoot, 'job-input');
    await mkdir(folderPath);
    const validPath = join(folderPath, 'valid.json');
    const invalidPath = join(folderPath, 'invalid.json');
    await copyFile(join(repoRoot, 'test', 'fixtures', 'gemini-polish.json'), validPath);
    await writeFile(invalidPath, '{invalid json', 'utf8');

    const collection = 'saved_jobs_test';
    const empty = await fetch(
      `${ctx.baseUrl}/api/ingest-jobs?collection=${encodeURIComponent(collection)}`,
    );
    assert.deepEqual(await empty.json(), { job: null });

    const rejected = await fetch(`${ctx.baseUrl}/api/ingest-jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        folderPath,
        collection,
        selectedFiles: [join(ctx.tempRoot, 'outside.json')],
      }),
    });
    assert.equal(rejected.status, 400);

    const accepted = await fetch(`${ctx.baseUrl}/api/ingest-jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ folderPath, collection, selectedFiles: [validPath, invalidPath] }),
    });
    assert.equal(accepted.status, 202);
    const { job: started } = await accepted.json();
    assert.equal(started.state, 'running');

    const completed = await waitForJob(
      ctx.baseUrl,
      collection,
      (job) => job?.state === 'completed_with_errors',
    );
    const files = Object.fromEntries(completed.files.map((file) => [basename(file.path), file]));
    assert.equal(files['valid.json'].state, 'completed');
    assert.equal(files['invalid.json'].state, 'failed');

    const jobPath = join(ctx.tempRoot, 'ingest-jobs.json');
    const persisted = JSON.parse(await readFile(jobPath, 'utf8'));
    assert.equal(persisted[collection].id, started.id);
    if (process.platform !== 'win32') assert.equal((await stat(jobPath)).mode & 0o777, 0o600);

    const continued = await fetch(`${ctx.baseUrl}/api/ingest-jobs/${started.id}/continue`, {
      method: 'POST',
    });
    assert.equal(continued.status, 200);
    assert.equal((await continued.json()).job.state, 'running');

    const retried = await waitForJob(
      ctx.baseUrl,
      collection,
      (job) => job?.state === 'completed_with_errors',
    );
    const retriedFiles = Object.fromEntries(
      retried.files.map((file) => [basename(file.path), file]),
    );
    assert.equal(retriedFiles['valid.json'].state, 'completed');
    assert.equal(retriedFiles['invalid.json'].state, 'failed');
  } finally {
    await ctx.stop();
  }
});
