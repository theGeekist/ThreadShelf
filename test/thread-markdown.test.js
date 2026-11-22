import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildThreadMarkdown, fmtDate, renderThreadMarkdown } from '../src/thread-markdown.js';
import { __testing__ } from '../mcp/server.js';

describe('MCP Markdown export', () => {
  it('preserves Markdown and code content with conversation metadata', () => {
    const thread = {
      sourceFile: '/synthetic.json',
      conversationKey: 'openai:12345678-1234-1234-1234-123456789012',
      title: 'Synthetic title',
      createdInThreadShelf: false,
      turns: [
        { user: 'A list:\n\n- one\n- two', createdAt: '2026-01-01T00:00:00Z' },
        { ai: '```js\nconst x = 1;\n\n\nconsole.log(x);\n```' },
      ],
    };
    const md = renderThreadMarkdown(thread, 'chunks');
    assert.match(md, /^# Synthetic title\n/);
    assert.ok(md.includes('### 🧑 User'));
    assert.ok(md.includes('### 🤖 Response'));
    assert.ok(md.includes('**Collection:** Chunks'));
    assert.ok(md.includes('**Source:** `/synthetic.json`'));
    assert.ok(md.includes('A list:\n\n- one\n- two'));
    assert.ok(md.includes('```js\nconst x = 1;\n\nconsole.log(x);\n```'));
    assert.ok(md.includes(`*${fmtDate('2026-01-01T00:00:00Z')}*`));
    assert.equal(
      md,
      buildThreadMarkdown('Synthetic title', thread.turns, {
        collection: 'Chunks',
        sourceFile: '/synthetic.json',
      }),
    );
  });
  it('exposes the tool and validates the same source requirement as read_thread', async () => {
    const tool = __testing__.TOOL_DEFINITIONS.find(
      (item) => item.name === 'export_thread_markdown',
    );
    assert.deepEqual(tool.inputSchema.required, ['sourceFile']);
    const result = await __testing__.HANDLERS['tools/call']({
      name: 'export_thread_markdown',
      arguments: {},
    });
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /Missing sourceFile/);
  });
});
