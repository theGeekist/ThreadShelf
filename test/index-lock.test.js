import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { test } from 'node:test';
import process from 'node:process';

test('persistent index lock serializes writers across host processes', async () => {
  const root = await mkdtemp(join(tmpdir(), 'threadshelf-index-lock-'));
  const lock = join(root, 'collection.lock');
  const events = join(root, 'events.log');
  const moduleUrl = pathToFileURL(resolve('src/index-lock.ts')).href;
  const script = `
    import { appendFile } from 'node:fs/promises';
    import { withPersistentFileLock } from ${JSON.stringify(moduleUrl)};
    await withPersistentFileLock(process.env.THREADSHELF_TEST_LOCK, async () => {
      await appendFile(process.env.THREADSHELF_TEST_EVENTS, 'start\\n');
      await new Promise((resolve) => setTimeout(resolve, 120));
      await appendFile(process.env.THREADSHELF_TEST_EVENTS, 'end\\n');
    });
  `;
  const run = () =>
    new Promise((resolvePromise, reject) => {
      const child = spawn(
        process.execPath,
        ['--import', 'tsx', '--input-type=module', '-e', script],
        {
          env: {
            ...process.env,
            THREADSHELF_TEST_LOCK: lock,
            THREADSHELF_TEST_EVENTS: events,
          },
          stdio: 'inherit',
        },
      );
      child.once('error', reject);
      child.once('exit', (code) =>
        code === 0 ? resolvePromise() : reject(new Error(`Lock subprocess exited ${code}`)),
      );
    });

  try {
    await Promise.all([run(), run()]);
    assert.deepEqual((await readFile(events, 'utf8')).trim().split('\n'), [
      'start',
      'end',
      'start',
      'end',
    ]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
