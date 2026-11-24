import { it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import AdmZip from 'adm-zip';
import { readChatGptImage } from '../src/chatgpt-image.js';

it('reads one verified image and rejects missing, ambiguous, oversized and non-image entries', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'threadshelf-image-'));
  try {
    const archive = new AdmZip();
    const png = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10, 1, 2, 3]);
    archive.addFile('file-good.dat', png);
    archive.addFile('file-a-b.dat', png);
    archive.addFile('file-dupe-one.dat', png);
    archive.addFile('file-dupe-two.dat', png);
    archive.addFile('file-text.dat', Buffer.from('not an image'));
    archive.addFile('file-big.dat', Buffer.alloc(8 * 1024 * 1024 + 1));
    const path = join(dir, 'synthetic.zip');
    await writeFile(path, archive.toBuffer());
    const image = await readChatGptImage(path, 'file-good');
    assert.equal(image.mimeType, 'image/png');
    assert.deepEqual(image.data, png);
    await assert.rejects(readChatGptImage(path, 'file-dupe'), /ambiguous/);
    await assert.rejects(readChatGptImage(path, 'file-missing'), /not in the export/);
    await assert.rejects(readChatGptImage(path, 'file-a'), /not in the export/);
    await assert.rejects(readChatGptImage(path, '../escape'), /Invalid attachment ID/);
    await assert.rejects(readChatGptImage(path, 'file-text'), /not a supported image/);
    await assert.rejects(readChatGptImage(path, 'file-big'), /8 MiB/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
