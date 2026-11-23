/**
 * Durable identity and progress shared by every indexing workflow.
 *
 * Storage adapters may persist entries beside their source records or in a
 * checkpoint file, but they must use the same identity: collection, model,
 * method, and scope. This keeps retries and resumable backfills from colliding
 * when a collection is indexed with a different model or method.
 */
export interface IndexLedgerIdentity {
  readonly collection: string;
  readonly model: string;
  readonly method: string;
}

export interface IndexLedgerCheckpoint extends IndexLedgerIdentity {
  readonly scope: string;
  readonly cursor: number;
  readonly complete: boolean;
  readonly attempts: number;
  readonly retryAt: number;
  readonly error: string;
  readonly updatedAt: string;
}

export const INDEX_METHOD_SEMANTIC = 'semantic';
export const INDEX_METHOD_ARCHIVE_COPY = 'archive-copy';
export const INDEX_COLLECTION_ARCHIVE = '*';

export const indexLedgerKey = (identity: IndexLedgerIdentity, scope: string): string =>
  JSON.stringify([identity.collection, identity.model, identity.method, scope]);

export const createIndexLedgerCheckpoint = (
  identity: IndexLedgerIdentity,
  scope: string,
  progress: Partial<
    Pick<IndexLedgerCheckpoint, 'cursor' | 'complete' | 'attempts' | 'retryAt' | 'error'>
  > = {},
): IndexLedgerCheckpoint => ({
  ...identity,
  scope,
  cursor: Math.max(0, Math.floor(progress.cursor ?? 0)),
  complete: progress.complete ?? false,
  attempts: Math.max(0, Math.floor(progress.attempts ?? 0)),
  retryAt: Math.max(0, progress.retryAt ?? 0),
  error: progress.error ?? '',
  updatedAt: new Date().toISOString(),
});

