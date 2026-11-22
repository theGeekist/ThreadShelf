import { test, expect } from './fixtures.js';
import { mkdtemp, copyFile, mkdir, rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const fixturesDir = join(__dirname, '..', 'fixtures');

test.describe('Upload + ingest UI flow', () => {
  test('selects files and pauses and continues a saved path indexing job', async ({ appPage }) => {
    const files = ['/tmp/fixture/first.json', '/tmp/fixture/second.json'];
    let job = null;
    let acceptedFiles = [];
    await appPage.route('**/api/ingest-preview**', (route) =>
      route.fulfill({
        contentType: 'application/json',
        json: { collection: 'pw_fixture', files, duplicates: [] },
      }),
    );
    await appPage.route('**/api/ingest-jobs**', async (route) => {
      const request = route.request();
      const pathname = new URL(request.url()).pathname;
      if (request.method() === 'GET') {
        return route.fulfill({ contentType: 'application/json', json: { job } });
      }
      if (pathname.endsWith('/stop')) {
        job = {
          ...job,
          state: 'stopped',
          files: job.files.map((file) =>
            file.state === 'running' ? { path: file.path, state: 'pending' } : file,
          ),
        };
        return route.fulfill({ contentType: 'application/json', json: { job } });
      }
      if (pathname.endsWith('/continue')) {
        job = { ...job, state: 'running' };
        return route.fulfill({ contentType: 'application/json', json: { job } });
      }
      const payload = request.postDataJSON();
      acceptedFiles = payload.selectedFiles;
      job = {
        id: 'saved-job-test',
        collection: payload.collection,
        folderPath: '/tmp/fixture',
        state: 'running',
        files: acceptedFiles.map((path) => ({ path, state: 'running' })),
        progress: { status: 'progress', phase: 'reading', progressPercent: 10 },
      };
      return route.fulfill({ status: 202, contentType: 'application/json', json: { job } });
    });

    await appPage.locator('#indexingNavBtn').click();
    await appPage.getByPlaceholder('/absolute/path/to/exports').fill('/tmp/fixture');
    await appPage.getByRole('button', { name: 'Use path' }).click();
    await expect(appPage.getByLabel('Select first.json')).toBeChecked();
    await appPage.getByLabel('Select first.json').uncheck();
    await appPage.locator('#ingestBtn').click();
    await expect(appPage.locator('.saved-job-panel')).toContainText('running');
    await expect(appPage.getByRole('button', { name: 'Pause indexing' })).toBeVisible();
    expect(acceptedFiles).toEqual(['/tmp/fixture/second.json']);

    await appPage.getByRole('button', { name: 'Pause indexing' }).click();
    await expect(appPage.locator('.saved-job-panel')).toContainText('stopped');
    await appPage.getByRole('button', { name: 'Continue' }).click();
    await expect(appPage.getByRole('button', { name: 'Pause indexing' })).toBeVisible();
    await appPage.getByRole('button', { name: 'Pause indexing' }).click();
    await expect(appPage.locator('.saved-job-panel')).toContainText('stopped');
    await appPage.screenshot({
      path: join(tmpdir(), 'threadshelf-indexing-screen.png'),
      fullPage: true,
    });
  });

  test('uploading a folder of exports adds a new collection', async ({ appPage }) => {
    // Disable the duplicate-confirm prompt (we don't expect any).
    await appPage.evaluate(() => {
      window.confirm = () => true;
    });

    // webkitdirectory inputs require a real directory path in Playwright.
    const root = await mkdtemp(join(tmpdir(), 'pw-upload-'));
    const subdir = join(root, 'upload_alpha');
    await mkdir(subdir, { recursive: true });
    await copyFile(join(fixturesDir, 'gemini-polish.json'), join(subdir, 'conversation.json'));

    try {
      await appPage.locator('#indexingNavBtn').click();
      await appPage.locator('#folderFileInput').setInputFiles(subdir);
      await expect(appPage.locator('#folderChosenLabel')).not.toBeEmpty({ timeout: 5000 });
      await appPage.locator('#ingestBtn').click();
      await expect(appPage.locator('#ingestStatus')).toBeVisible({ timeout: 90_000 });
      await expect(appPage.locator('#ingestStatus')).toContainText(/Indexed|done|complet/i, {
        timeout: 90_000,
      });

      await appPage.waitForFunction(
        async () => {
          const res = await fetch('/api/collections');
          const data = await res.json();
          return (data.collections || []).some((name) => name === 'upload_alpha');
        },
        undefined,
        { timeout: 30_000 },
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
