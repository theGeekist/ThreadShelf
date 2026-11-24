import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { splitTextByTokenBudget } from '../src/chunking.js';

const wordCount = (text) => text.trim().split(/\s+/u).filter(Boolean).length;

describe('splitTextByTokenBudget', () => {
  it('keeps a complete chunk when it is within the tokenizer limit', () => {
    const text = 'A short complete message.';
    assert.deepEqual(splitTextByTokenBudget(text, 8, wordCount), [text]);
  });

  it('splits oversized messages at sentence boundaries and respects the token limit', () => {
    const text = 'One two three. Four five six. Seven eight nine. Ten eleven twelve.';
    const chunks = splitTextByTokenBudget(text, 4, wordCount, 0);

    assert.ok(chunks.length > 1);
    assert.ok(chunks.every((chunk) => wordCount(chunk) <= 4));
    assert.ok(chunks[0].endsWith('.'));
    assert.ok(chunks.join(' ').includes('Ten eleven twelve.'));
  });

  it('does not split a surrogate pair when finding a token boundary', () => {
    const text = 'a 😀 b 😀 c';
    const chunks = splitTextByTokenBudget(text, 2, wordCount, 0);

    assert.ok(chunks.every((chunk) => wordCount(chunk) <= 2));
    assert.ok(chunks.join(' ').includes('😀'));
    assert.ok(chunks.every((chunk) => !/[\uD800-\uDBFF]$/u.test(chunk)));
  });

  it('rejects a token budget smaller than one code point', () => {
    assert.throws(() => splitTextByTokenBudget('x', 1, () => 2), /code point exceeds/u);
  });
});
