import {
  searchCollection,
  keywordSearchCollection,
  keywordResultComparator,
  assertCollectionIndexProfile,
  type SearchResult,
  type SearchOptions,
} from '../store.js';
import { embedOne } from '../embedding.js';
import { getAllCollections } from './collections.js';
import type { SearchMode } from '../validation.js';

export interface MultiSearchOptions extends SearchOptions {
  readonly model?: string;
  readonly mode?: SearchMode;
}

export const searchAcrossCollections = async (
  query: string,
  collection: string,
  opts: MultiSearchOptions = {},
): Promise<SearchResult[]> => {
  const n = opts.n ?? 15;

  if (opts.mode === 'keyword') {
    return keywordSearchAcrossCollections(query, collection, { ...opts, n });
  }

  const collections = collection === 'all' ? await getAllCollections() : [collection];
  await Promise.all(collections.map((name) => assertCollectionIndexProfile(name)));

  // Validate every target before sending query text to a remote embedding
  // provider. Then generate the vector once for the selected collections.
  const queryEmbedding = await embedOne(query);

  if (collection === 'all') {
    const merged = await Promise.all(
      collections.map(async (name) => {
        const rows = await searchCollection(name, query, { ...opts, n }, queryEmbedding);
        return rows.map((row) => ({
          ...row,
          metadata: { ...row.metadata, collection: name },
        }));
      }),
    );

    // Each collection already applied the exact role/model/date filters.
    const results: SearchResult[] = merged.flat();
    results.sort(searchResultComparator(query, opts.keywordBoost === true));
    return results.slice(0, n);
  }

  const rawResults = await searchCollection(collection, query, { ...opts, n }, queryEmbedding);
  return rawResults.slice(0, n);
};

// Exact substring search. Model/date/role filtering happens inside
// keywordSearchCollection; here we only fan out and merge-rank.
const keywordSearchAcrossCollections = async (
  query: string,
  collection: string,
  opts: MultiSearchOptions,
): Promise<SearchResult[]> => {
  const n = opts.n ?? 15;

  if (collection !== 'all') {
    return keywordSearchCollection(collection, query, opts);
  }

  const collections = await getAllCollections();
  const merged = await Promise.all(
    collections.map(async (name) => {
      const rows = await keywordSearchCollection(name, query, opts);
      return rows.map((row) => ({
        ...row,
        metadata: { ...row.metadata, collection: name },
      }));
    }),
  );

  const results: SearchResult[] = merged.flat();
  results.sort(keywordResultComparator(query));
  return results.slice(0, n);
};

export const searchResultComparator =
  (query: string, keywordBoost: boolean) =>
  (a: SearchResult, b: SearchResult): number => {
    if (keywordBoost) {
      const needle = query.trim().toLowerCase();
      const aHas = needle.length > 0 && a.document.toLowerCase().includes(needle);
      const bHas = needle.length > 0 && b.document.toLowerCase().includes(needle);
      if (aHas !== bHas) return aHas ? -1 : 1;
    }
    return (a.distance ?? Number.POSITIVE_INFINITY) - (b.distance ?? Number.POSITIVE_INFINITY);
  };
