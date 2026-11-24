import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { connect } from '@lancedb/lancedb';
import { ensureIndexProfile, writeIndexProfile } from '../src/index-profile.ts';

const profile = {
  model: 'cloudflare/bge-m3',
  dimension: 1024,
  method: 'semantic',
};

test('index profile prevents querying vectors with another model identity', async () => {
  const root = await mkdtemp(join(tmpdir(), 'threadshelf-index-profile-'));
  try {
    const db = await connect(join(root, 'db'));
    await writeIndexProfile(db, { collection: 'conversations', ...profile, state: 'ready' });
    await assert.rejects(
      ensureIndexProfile(db, 'conversations', {
        ...profile,
        model: 'local/minilm',
        dimension: 384,
      }),
      /different embedding index profile/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('building profiles remain unavailable until indexing completes', async () => {
  const root = await mkdtemp(join(tmpdir(), 'threadshelf-index-building-'));
  try {
    const db = await connect(join(root, 'db'));
    await writeIndexProfile(db, { collection: 'conversations', ...profile, state: 'building' });
    await assert.rejects(ensureIndexProfile(db, 'conversations', profile), /incomplete/);
    const allowed = await ensureIndexProfile(db, 'conversations', profile, { allowBuilding: true });
    assert.equal(allowed?.state, 'building');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('unprofiled legacy 384-dimensional vectors cannot be mistaken for Cloudflare vectors', async () => {
  const root = await mkdtemp(join(tmpdir(), 'threadshelf-index-legacy-'));
  try {
    const db = await connect(join(root, 'db'));
    await db.createTable('conversations', [
      { id: 'one', vector: Array(384).fill(0), document: 'legacy content' },
    ]);
    await assert.rejects(
      ensureIndexProfile(db, 'conversations', {
        model: 'cloudflare/another-384-model',
        dimension: 384,
        method: 'semantic',
      }),
      /unprofiled legacy MiniLM index/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
