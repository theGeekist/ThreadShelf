import type { Turn } from './validation.js';
import type { Provider } from './parser.js';

const MAX_CHUNK_CHARS = Math.max(200, Number(process.env.CHUNK_MAX_CHARS) || 2000);
const OVERLAP_CHARS = Math.max(0, Number(process.env.CHUNK_OVERLAP_CHARS) || 100);

export interface Chunk {
  readonly text: string;
  readonly role: string;
  readonly turnIndex: number;
  readonly sourceFile: string;
  readonly provider: Provider | 'threadshelf';
  readonly conversationKey?: string;
  readonly title?: string;
  readonly model?: string;
  readonly createdAt?: string;
  readonly createdInThreadShelf?: boolean;
  readonly generationProvider?: string;
}

export interface ChunkMeta {
  readonly sourceFile: string;
  readonly provider: Provider | 'threadshelf';
  readonly conversationKey?: string;
  readonly title?: string;
}

export function* iterateTurnChunks(turns: readonly Turn[], meta: ChunkMeta): Generator<Chunk> {
  let turnIndex = 0;

  for (const turn of turns) {
    const role = turn.user !== undefined ? 'user' : turn.thinking !== undefined ? 'thinking' : 'ai';
    const text = turn.user ?? turn.thinking ?? turn.ai ?? '';

    if (!isIndexableText(text)) {
      turnIndex++;
      continue;
    }

    const parts = splitText(text, MAX_CHUNK_CHARS, OVERLAP_CHARS);
    for (const part of parts) {
      yield {
        text: part,
        role,
        turnIndex,
        sourceFile: meta.sourceFile,
        provider: meta.provider,
        conversationKey: meta.conversationKey,
        title: meta.title,
        model: turn.model,
        createdAt: turn.createdAt,
        createdInThreadShelf: turn.createdInThreadShelf,
        generationProvider: turn.generationProvider,
      };
    }
    turnIndex++;
  }
}

export const chunkTurns = (turns: readonly Turn[], meta: ChunkMeta): Chunk[] =>
  Array.from(iterateTurnChunks(turns, meta));

/**
 * Split only chunks that exceed the active embedding tokenizer's input limit.
 * Prefer paragraph, sentence and word boundaries, while retaining the existing
 * overlap so the split pieces keep enough local context for search.
 */
export const splitTextByTokenBudget = (
  text: string,
  maxTokens: number,
  countTokens: (value: string) => number,
  overlap = OVERLAP_CHARS,
): string[] => {
  if (maxTokens < 1) throw new RangeError('maxTokens must be positive');
  if (countTokens(text) <= maxTokens) return [text];

  const parts: string[] = [];
  let start = 0;
  while (start < text.length) {
    let low = start + 1;
    let high = text.length;
    let best = start;
    while (low <= high) {
      const middle = Math.floor((low + high) / 2);
      const end = safeCodePointEnd(text, middle);
      if (end <= start) {
        low = middle + 1;
        continue;
      }
      if (countTokens(text.slice(start, end).trim()) <= maxTokens) {
        best = end;
        low = middle + 1;
      } else {
        high = middle - 1;
      }
    }

    if (best <= start) {
      const next = safeCodePointEnd(text, start + 1);
      if (countTokens(text.slice(start, next).trim()) > maxTokens) {
        throw new RangeError('One Unicode code point exceeds the embedding token limit');
      }
      best = next;
    }

    const end = preferredBoundary(text, start, best);
    const part = text.slice(start, end).trim();
    if (part) parts.push(part);
    if (end >= text.length) break;

    const nextStart = Math.max(start + 1, end - Math.max(0, overlap));
    start = safeCodePointEnd(text, nextStart);
  }

  return parts;
};

export const isIndexableText = (text: string | undefined | null): boolean => {
  if (!text || text === '[image]') return false;
  const trimmed = text.trim();
  if (!trimmed) return false;
  if (trimmed === '{}' || trimmed === '[]' || trimmed === '<') return false;
  if (/^search\(/i.test(trimmed)) return false;
  if (trimmed.length < 2) return false;
  return true;
};

const splitText = (text: string, maxChars: number, overlap: number): string[] => {
  if (text.length <= maxChars) return [text];
  const parts: string[] = [];
  let start = 0;

  while (start < text.length) {
    let end = Math.min(start + maxChars, text.length);
    if (end < text.length) {
      const breakAt = text.lastIndexOf('\n\n', end);
      if (breakAt > start) {
        end = breakAt + 2;
      } else {
        const lineBreak = text.lastIndexOf('\n', end);
        if (lineBreak > start) end = lineBreak + 1;
      }
    }
    const chunk = text.slice(start, end).trim();
    if (chunk.length > 0) parts.push(chunk);
    const nextStart = end - (end < text.length ? overlap : 0);
    start = nextStart > start ? nextStart : end;
  }

  return parts;
};

const safeCodePointEnd = (text: string, end: number): number => {
  if (end > 0 && end < text.length) {
    const code = text.charCodeAt(end - 1);
    if (code >= 0xd800 && code <= 0xdbff) return end + 1;
  }
  return Math.min(end, text.length);
};

const preferredBoundary = (text: string, start: number, limit: number): number => {
  const range = text.slice(start, limit);
  const threshold = Math.floor(range.length * 0.6);
  const paragraph = range.lastIndexOf('\n\n');
  if (paragraph >= threshold) return start + paragraph + 2;

  let sentenceEnd = -1;
  for (const match of range.matchAll(/[.!?。！？;；](?=\s|$)/gu)) {
    sentenceEnd = match.index! + match[0].length;
  }
  if (sentenceEnd >= threshold) return start + sentenceEnd;

  const line = range.lastIndexOf('\n');
  if (line >= threshold) return start + line + 1;
  const space = Math.max(range.lastIndexOf(' '), range.lastIndexOf('\t'));
  if (space >= threshold) return start + space + 1;
  return limit;
};
