import { it } from 'node:test';
import assert from 'node:assert/strict';
import { chatGptAttachmentReferences } from '../src/chatgpt-attachments.js';

it('finds only current-branch image and attachment references for the selected conversation', () => {
  const data = [
    {
      id: 'abc',
      current_node: 'b',
      mapping: {
        a: {
          id: 'a',
          parent: null,
          message: {
            id: 'm1',
            content: {
              parts: [
                { asset_pointer: 'file-service://file-safe123' },
                { asset_pointer: 'file-service://../../bad' },
              ],
            },
            metadata: {
              attachments: [
                { id: 'file-document1', name: 'notes.pdf', mime_type: 'application/pdf' },
              ],
            },
          },
        },
        b: { id: 'b', parent: 'a', message: { id: 'm2', content: { parts: ['hi'] } } },
        c: {
          id: 'c',
          parent: 'a',
          message: {
            id: 'm3',
            content: { parts: [{ asset_pointer: 'file-service://file-other' }] },
          },
        },
      },
    },
  ];
  assert.deepEqual(chatGptAttachmentReferences(data, 'openai:abc'), [
    { id: 'file-safe123', messageId: 'm1', source: 'image' },
    {
      id: 'file-document1',
      messageId: 'm1',
      source: 'attachment',
      name: 'notes.pdf',
      mimeType: 'application/pdf',
    },
  ]);
  assert.throws(() => chatGptAttachmentReferences(data, 'openai:missing'), /not found uniquely/);
  assert.throws(() => chatGptAttachmentReferences(data, 'other:abc'), /ChatGPT conversation key/);
});
