import { it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

it('exports an archived conversation as MCP text without reading the source file', async () => {
  const temp = await mkdtemp(join(tmpdir(), 'threadshelf-md-'));
  process.env.LANCEDB_PATH = join(temp, 'lancedb');
  process.env.COLLECTIONS_PATH = join(temp, 'collections.json');
  process.env.OBSIDIAN_CONFIG_PATH = join(temp, 'obsidian.json');
  try {
    const { replaceThreadsForFile } = await import('../src/store.js');
    const { __testing__ } = await import('../mcp/server.js');
    const sourceFile = join(temp, 'deleted-source.json');
    const conversationKey = 'openai:12345678-1234-1234-1234-123456789012';
    await replaceThreadsForFile('fixture', sourceFile, 'openai', [
      {
        key: conversationKey,
        title: 'Synthetic',
        turns: [{ user: 'Hello **world**' }, { ai: '```\ncode\n```' }],
      },
    ]);
    const result = await __testing__.HANDLERS['tools/call']({
      name: 'export_thread_markdown',
      arguments: { collection: 'fixture', sourceFile, conversationKey },
    });
    assert.equal(result.isError, false);
    assert.match(result.content[0].text, /^# Synthetic\n/);
    assert.match(result.content[0].text, /### 🧑 User/);
    assert.match(result.content[0].text, /Hello \*\*world\*\*/);
    assert.ok(result.content[0].text.includes('```\ncode\n```'));
    assert.throws(() => JSON.parse(result.content[0].text));
  } finally {
    await rm(temp, { recursive: true, force: true });
  }
});
