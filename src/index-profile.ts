import type { Connection, Table } from '@lancedb/lancedb';
import { LOCAL_EMBEDDING_MODEL_IDENTITY } from './embedding.js';

export const INDEX_PROFILES_TABLE = '__index_profiles';

export interface IndexProfile {
  readonly collection: string;
  readonly model: string;
  readonly dimension: number;
  readonly method: string;
  readonly state: 'building' | 'ready';
  readonly updatedAt: string;
}

const escapeSql = (value: string): string => value.replace(/'/g, "''");

const profileTable = async (db: Connection, create: boolean): Promise<Table | undefined> => {
  const names = await db.tableNames();
  if (names.includes(INDEX_PROFILES_TABLE)) return db.openTable(INDEX_PROFILES_TABLE);
  if (!create) return undefined;
  try {
    return await db.createTable(
      INDEX_PROFILES_TABLE,
      [
        {
          collection: '',
          model: '',
          dimension: 0,
          method: '',
          state: 'building',
          updatedAt: new Date().toISOString(),
        },
      ],
      { mode: 'create' },
    );
  } catch (error) {
    const refreshed = await db.tableNames();
    if (!refreshed.includes(INDEX_PROFILES_TABLE)) throw error;
    return db.openTable(INDEX_PROFILES_TABLE);
  }
};

export const readIndexProfile = async (
  db: Connection,
  collection: string,
): Promise<IndexProfile | undefined> => {
  const table = await profileTable(db, false);
  if (!table) return undefined;
  const rows = await table
    .query()
    .where(`collection = '${escapeSql(collection)}'`)
    .limit(Number.MAX_SAFE_INTEGER)
    .toArray();
  if (rows.length === 0) return undefined;
  const profiles = new Map(
    rows.map((row) => [
      `${String(row.model)}\0${Number(row.dimension)}\0${String(row.method)}\0${String(row.state)}`,
      row,
    ]),
  );
  if (profiles.size !== 1) {
    throw new Error(`Collection ${collection} has conflicting embedding index profiles`);
  }
  const row = rows[0]!;
  return {
    collection,
    model: String(row.model),
    dimension: Number(row.dimension),
    method: String(row.method),
    state: String(row.state) === 'ready' ? 'ready' : 'building',
    updatedAt: String(row.updatedAt || ''),
  };
};

export const writeIndexProfile = async (
  db: Connection,
  profile: Omit<IndexProfile, 'updatedAt'>,
): Promise<void> => {
  const table = await profileTable(db, true);
  if (!table) throw new Error('Could not open the embedding index profile table');
  await table
    .mergeInsert('collection')
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute([{ ...profile, updatedAt: new Date().toISOString() }]);
};

export const deleteIndexProfile = async (db: Connection, collection: string): Promise<void> => {
  const table = await profileTable(db, false);
  await table?.delete(`collection = '${escapeSql(collection)}'`);
};

const vectorDimension = async (table: Table): Promise<number | undefined> => {
  const field = (await table.schema()).fields.find((candidate) => candidate.name === 'vector');
  const listSize = (field?.type as { listSize?: unknown } | undefined)?.listSize;
  return typeof listSize === 'number' && Number.isInteger(listSize) ? listSize : undefined;
};

export const ensureIndexProfile = async (
  db: Connection,
  collection: string,
  expected: Omit<IndexProfile, 'collection' | 'state' | 'updatedAt'>,
  options: { readonly createIfMissing?: boolean; readonly allowBuilding?: boolean } = {},
): Promise<IndexProfile | undefined> => {
  let profile = await readIndexProfile(db, collection);
  if (profile) {
    if (
      profile.model !== expected.model ||
      profile.dimension !== expected.dimension ||
      profile.method !== expected.method
    ) {
      throw new Error(
        `Collection ${collection} uses a different embedding index profile. Configure the matching provider/database or reindex into a separate LanceDB path.`,
      );
    }
    if (profile.state === 'building' && !options.allowBuilding) {
      throw new Error(
        `Collection ${collection} has an incomplete embedding generation and is not searchable yet. Resume or finish its indexing job.`,
      );
    }
    return profile;
  }

  const names = await db.tableNames();
  if (names.includes(collection)) {
    const vectors = await db.openTable(collection);
    const dimension = await vectorDimension(vectors);
    const rowCount = await vectors.countRows();
    if (rowCount > 0 && dimension !== expected.dimension) {
      throw new Error(
        `Collection ${collection} contains ${dimension ?? 'unknown-dimension'}-dimensional vectors, but the active embedder produces ${expected.dimension}. Reindex into a separate LanceDB path before switching providers.`,
      );
    }
    if (rowCount > 0 && dimension === 384 && expected.model !== LOCAL_EMBEDDING_MODEL_IDENTITY) {
      throw new Error(
        `Collection ${collection} is an unprofiled legacy MiniLM index. Reindex it before using a different embedding model, even if that model also produces 384-dimensional vectors.`,
      );
    }
  }

  if (!options.createIfMissing && !names.includes(collection)) return undefined;
  await writeIndexProfile(db, { collection, ...expected, state: 'ready' });
  profile = await readIndexProfile(db, collection);
  if (!profile || profile.model !== expected.model || profile.dimension !== expected.dimension) {
    throw new Error(`Could not establish a consistent embedding profile for ${collection}`);
  }
  return profile;
};
