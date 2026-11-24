import { randomUUID } from 'node:crypto';
import { mkdir, open, readFile, rm, stat } from 'node:fs/promises';
import { dirname } from 'node:path';

interface LockOwner {
  readonly pid: number;
  readonly token: string;
}

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

const processExists = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== 'ESRCH';
  }
};

const readOwner = async (path: string): Promise<LockOwner | undefined> => {
  try {
    const value = JSON.parse(await readFile(path, 'utf8')) as Partial<LockOwner>;
    if (Number.isInteger(value.pid) && typeof value.token === 'string') return value as LockOwner;
  } catch {
    // The process may have stopped after creating the exclusive file but
    // before writing its owner record. Reclaim only after the file is stale.
  }
  return undefined;
};

const reapStaleLock = async (path: string): Promise<void> => {
  const reaperPath = `${path}.reaper`;
  let reaper: Awaited<ReturnType<typeof open>> | undefined;
  try {
    reaper = await open(reaperPath, 'wx', 0o600);
    await reaper.writeFile(JSON.stringify({ pid: process.pid, token: randomUUID() }));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
    const staleOwner = await readOwner(reaperPath);
    const reaperInfo = await stat(reaperPath).catch(() => undefined);
    const stale = staleOwner
      ? !processExists(staleOwner.pid)
      : Boolean(reaperInfo && Date.now() - reaperInfo.mtimeMs > 2_000);
    if (stale) await rm(reaperPath, { force: true });
    return;
  } finally {
    await reaper?.close();
  }

  try {
    const owner = await readOwner(path);
    const info = await stat(path).catch(() => undefined);
    const stale = owner
      ? !processExists(owner.pid)
      : Boolean(info && Date.now() - info.mtimeMs > 2_000);
    if (!stale) return;
    const confirmed = await readOwner(path);
    if ((owner && confirmed?.token === owner.token) || (!owner && confirmed === undefined)) {
      await rm(path, { force: true });
    }
  } finally {
    await rm(reaperPath, { force: true });
  }
};

/** Serialize same-host processes that mutate one local index scope. */
export const withPersistentFileLock = async <T>(
  path: string,
  operation: () => Promise<T>,
): Promise<T> => {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const owner: LockOwner = { pid: process.pid, token: randomUUID() };
  let released: (() => Promise<void>) | undefined;
  for (;;) {
    try {
      const handle = await open(path, 'wx', 0o600);
      try {
        await handle.writeFile(JSON.stringify(owner));
      } catch (error) {
        await rm(path, { force: true }).catch(() => {});
        throw error;
      } finally {
        await handle.close();
      }
      released = async () => {
        const current = await readOwner(path);
        if (current?.token === owner.token) await rm(path, { force: true });
      };
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      await reapStaleLock(path);
      await delay(50);
    }
  }

  try {
    return await operation();
  } finally {
    await released?.();
  }
};
