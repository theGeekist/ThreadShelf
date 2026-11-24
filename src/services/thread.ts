import { open, readFile } from 'node:fs/promises';
import { parseExport, getConversationFromExport } from '../parser.js';
import { getStoredThreads, listSourceFilesInCollection, type StoredThreadRow } from '../store.js';
import { validateTurns, type Turn } from '../validation.js';
import { getAllCollections } from './collections.js';
import { portableModelLabel } from '../model-label.js';

export interface ThreadResult {
  readonly sourceFile: string;
  readonly conversationKey: string;
  readonly title?: string;
  readonly createdInThreadShelf: boolean;
  readonly threadCreatedAt?: string;
  readonly turns: Turn[];
}

const sanitizeTurns = (turns: Turn[]): Turn[] => {
  const portable = turns.map((turn) =>
    turn.model ? { ...turn, model: portableModelLabel(turn.model) } : turn,
  );
  try {
    validateTurns(portable);
    return portable;
  } catch (e) {
    console.warn('[thread] dropped invalid turns:', (e as Error).message);
    return portable.filter(
      (turn: Turn) =>
        turn &&
        (typeof turn.user === 'string' ||
          typeof turn.thinking === 'string' ||
          typeof turn.ai === 'string'),
    );
  }
};

const parseStoredTurns = (turnsJson: string): Turn[] => {
  try {
    const parsed: unknown = JSON.parse(turnsJson);
    return Array.isArray(parsed) ? (parsed as Turn[]) : [];
  } catch {
    return [];
  }
};

// With collection "all" the same source file can be stored once per collection.
// Keep the most recently ingested copy so the thread matches the freshest index.
const pickNewestGroup = (rows: StoredThreadRow[]): StoredThreadRow[] => {
  const byCollection = new Map<string, StoredThreadRow[]>();
  for (const row of rows) {
    const group = byCollection.get(row.collection) ?? [];
    group.push(row);
    byCollection.set(row.collection, group);
  }
  let newest: StoredThreadRow[] = [];
  let newestAt = '';
  for (const group of byCollection.values()) {
    const at = group[0]?.ingestedAt ?? '';
    if (!newest.length || at > newestAt) {
      newest = group;
      newestAt = at;
    }
  }
  return newest;
};

const loadStoredThread = async (
  sourceFile: string,
  collection: string,
  conversationKey?: string,
): Promise<ThreadResult | null> => {
  const rows = await getStoredThreads(collection === 'all' ? null : collection, sourceFile);
  if (!rows.length) return null;

  const stored = pickNewestGroup(rows);

  if (conversationKey) {
    const row = stored.find((entry) => entry.conversationKey === conversationKey);
    // A stale link (e.g. an old URL after re-indexing a rewritten export) falls
    // back to re-parsing the source file below.
    if (!row) return null;
    return {
      sourceFile,
      conversationKey: row.conversationKey,
      title: row.title || undefined,
      createdInThreadShelf: row.createdInThreadShelf,
      threadCreatedAt: row.threadCreatedAt || undefined,
      turns: sanitizeTurns(parseStoredTurns(row.turnsJson)),
    };
  }

  if (stored.length === 1) {
    const row = stored[0]!;
    return {
      sourceFile,
      conversationKey: row.conversationKey,
      title: row.title || undefined,
      createdInThreadShelf: row.createdInThreadShelf,
      threadCreatedAt: row.threadCreatedAt || undefined,
      turns: sanitizeTurns(parseStoredTurns(row.turnsJson)),
    };
  }

  // No key against a multi-conversation export: flatten in original file order,
  // mirroring the legacy parseExport behaviour.
  return {
    sourceFile,
    conversationKey: '',
    createdInThreadShelf: stored.every((row) => row.createdInThreadShelf),
    threadCreatedAt: stored.find((row) => row.threadCreatedAt)?.threadCreatedAt,
    turns: sanitizeTurns(stored.flatMap((row) => parseStoredTurns(row.turnsJson))),
  };
};

const loadThreadFromSourceFile = async (
  sourceFile: string,
  collection: string,
  conversationKey?: string,
  maxSourceBytes?: number,
): Promise<ThreadResult> => {
  const allowedFiles =
    collection === 'all'
      ? (
          await Promise.all((await getAllCollections()).map((n) => listSourceFilesInCollection(n)))
        ).flat()
      : await listSourceFilesInCollection(collection);

  if (!allowedFiles.includes(sourceFile)) {
    throw new NotFoundError('File not in collection or not found');
  }

  let raw: string;
  try {
    raw =
      maxSourceBytes === undefined
        ? await readFile(sourceFile, 'utf-8')
        : await readSourceFileBounded(sourceFile, maxSourceBytes);
  } catch (error) {
    if (error instanceof BadRequestError) throw error;
    throw new NotFoundError(
      'Source file is no longer readable and this collection predates stored threads — re-index to restore thread view',
    );
  }

  const parsed = conversationKey
    ? getConversationFromExport(raw, conversationKey)
    : { conversation: null, error: null };

  if (parsed.error) throw new NotFoundError(parsed.error);

  const fallback = !conversationKey ? parseExport(raw) : null;
  if (fallback?.error) throw new BadRequestError(fallback.error);

  const selected = parsed.conversation;
  const turns = sanitizeTurns(selected?.turns || fallback?.turns || []);

  return {
    sourceFile,
    conversationKey: selected?.key || conversationKey || '',
    title: selected?.title,
    createdInThreadShelf: false,
    turns,
  };
};

const readSourceFileBounded = async (sourceFile: string, maxBytes: number): Promise<string> => {
  const file = await open(sourceFile, 'r');
  try {
    if ((await file.stat()).size > maxBytes) {
      throw new BadRequestError(
        `Source file exceeds the ${Math.floor(maxBytes / 1024 / 1024)} MiB limit`,
      );
    }
    const chunkSize = 64 * 1024;
    const chunks: Buffer[] = [];
    let total = 0;
    while (total <= maxBytes) {
      const buffer = Buffer.allocUnsafe(Math.min(chunkSize, maxBytes + 1 - total));
      const { bytesRead } = await file.read(buffer, 0, buffer.length, total);
      if (!bytesRead) break;
      chunks.push(buffer.subarray(0, bytesRead));
      total += bytesRead;
    }
    if (total > maxBytes) {
      throw new BadRequestError(
        `Source file exceeds the ${Math.floor(maxBytes / 1024 / 1024)} MiB limit`,
      );
    }
    return Buffer.concat(chunks).toString('utf8');
  } finally {
    await file.close();
  }
};

export const loadThread = async (
  sourceFile: string,
  collection: string,
  conversationKey?: string,
  options: { readonly maxSourceBytes?: number } = {},
): Promise<ThreadResult> => {
  const stored = await loadStoredThread(sourceFile, collection, conversationKey);
  if (stored) return stored;
  return loadThreadFromSourceFile(sourceFile, collection, conversationKey, options.maxSourceBytes);
};

export class NotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NotFoundError';
  }
}

export class BadRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BadRequestError';
  }
}
