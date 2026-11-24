import { randomUUID } from 'node:crypto';
import { mkdir, readFile, rename, chmod, rm, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { dataPath } from '../paths.js';
import { ingestFiles, listExportFiles, type IngestResult, type ProgressEvent } from '../ingest.js';

export type IngestJobState =
  'running' | 'stopped' | 'interrupted' | 'completed' | 'completed_with_errors';
export type IngestJobFileState = 'pending' | 'running' | 'completed' | 'failed';

export interface IngestJob {
  readonly id: string;
  readonly collection: string;
  readonly folderPath: string;
  readonly state: IngestJobState;
  readonly files: readonly {
    readonly path: string;
    readonly state: IngestJobFileState;
    readonly error?: string;
  }[];
  readonly progress?: ProgressEvent;
  readonly result?: IngestResult;
}

type StoredJobs = Record<string, IngestJob>;
const controllers = new Map<string, AbortController>();
const workers = new Map<string, Promise<void>>();
let jobs: StoredJobs | undefined;
let loadPromise: Promise<void> | undefined;
let writes = Promise.resolve();
let startingJob = false;

class JobPersistenceError extends Error {
  constructor(cause: unknown) {
    super('Could not persist indexing job state', { cause });
    this.name = 'JobPersistenceError';
  }
}

const storagePath = (): string =>
  resolve(process.env.INGEST_JOBS_PATH?.trim() || dataPath('ingestJobs'));

const persist = async (snapshot: StoredJobs): Promise<void> => {
  const path = storagePath();
  const tempPath = `${path}.${randomUUID()}.tmp`;
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  try {
    await writeFile(tempPath, JSON.stringify(snapshot), { mode: 0o600 });
    await chmod(tempPath, 0o600);
    await rename(tempPath, path);
  } catch (error) {
    await rm(tempPath, { force: true }).catch(() => {});
    throw error;
  }
};

const save = async (job: IngestJob): Promise<void> => {
  const operation = writes
    .catch(() => {})
    .then(async () => {
      const next = { ...jobs!, [job.collection]: job };
      try {
        await persist(next);
      } catch (error) {
        throw new JobPersistenceError(error);
      }
      jobs = next;
    });
  writes = operation.then(
    () => {},
    () => {},
  );
  await operation;
};

const load = async (): Promise<void> => {
  if (!loadPromise) {
    loadPromise = (async () => {
      try {
        const parsed: unknown = JSON.parse(await readFile(storagePath(), 'utf8'));
        jobs =
          parsed && typeof parsed === 'object' && !Array.isArray(parsed)
            ? (parsed as StoredJobs)
            : {};
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
        jobs = {};
      }
      for (const [collection, job] of Object.entries(jobs)) {
        if (job?.state === 'running') {
          jobs[collection] = {
            ...job,
            state: 'interrupted',
            files: job.files.map((file) =>
              file.state === 'running' ? { path: file.path, state: 'pending' } : file,
            ),
          };
        }
      }
      if (Object.values(jobs).some((job) => job.state === 'interrupted')) await persist(jobs);
    })();
  }
  await loadPromise;
};

const requireJob = async (id: string): Promise<IngestJob> => {
  await load();
  const job = Object.values(jobs!).find((candidate) => candidate.id === id);
  if (!job) throw Object.assign(new Error('Ingest job not found'), { status: 404 });
  return job;
};

const resultSummary = (current: IngestResult | undefined, next: IngestResult): IngestResult => ({
  conversations: (current?.conversations ?? 0) + next.conversations,
  ingested: (current?.ingested ?? 0) + next.ingested,
  totalTokens: (current?.totalTokens ?? 0) + next.totalTokens,
  files: [...(current?.files ?? []), ...next.files],
  errors: [...(current?.errors ?? []), ...next.errors],
  skippedFiles: [...(current?.skippedFiles ?? []), ...(next.skippedFiles ?? [])],
  providers: Object.fromEntries(
    [...new Set([...Object.keys(current?.providers ?? {}), ...Object.keys(next.providers)])].map(
      (provider) => [
        provider,
        (current?.providers[provider] ?? 0) + (next.providers[provider] ?? 0),
      ],
    ),
  ),
  elapsedMs: (current?.elapsedMs ?? 0) + next.elapsedMs,
});

const runJob = async (id: string): Promise<void> => {
  const controller = controllers.get(id)!;
  try {
    let job = await requireJob(id);
    while (!controller.signal.aborted) {
      const nextIndex = job.files.findIndex((file) => file.state === 'pending');
      if (nextIndex < 0) break;
      const current = job.files[nextIndex]!;
      job = {
        ...job,
        files: job.files.map((file, index) =>
          index === nextIndex ? { path: file.path, state: 'running' } : file,
        ),
      };
      await save(job);
      try {
        const result = await ingestFiles(job.collection, [current.path], {
          signal: controller.signal,
          onProgress: (progress) => {
            const active = jobs![job.collection]!;
            const done = active.files.filter(
              (file) => file.state === 'completed' || file.state === 'failed',
            ).length;
            const mapped: ProgressEvent = {
              ...progress,
              totalFiles: active.files.length,
              processedFiles: done,
              progressPercent:
                (100 * (done + (progress.progressPercent ?? 0) / 100)) /
                Math.max(active.files.length, 1),
              currentFile: current.path,
            };
            void save({ ...active, progress: mapped }).catch(() => {});
          },
        });
        const failed = result.errors.length > 0 || result.replacementSkipped === true;
        const active = jobs![job.collection]!;
        const latest: IngestJob = {
          ...active,
          files: active.files.map((file) =>
            file.path === current.path
              ? failed
                ? {
                    path: file.path,
                    state: 'failed',
                    error: result.errors.join('; ') || 'Replacement was skipped',
                  }
                : { path: file.path, state: 'completed' }
              : file,
          ),
          result: resultSummary(active.result, result),
        };
        await save(latest);
        job = latest;
      } catch (error) {
        if (error instanceof JobPersistenceError) throw error;
        if (controller.signal.aborted) break;
        const active = jobs![job.collection]!;
        const message = error instanceof Error ? error.message : String(error);
        job = {
          ...active,
          files: active.files.map((file) =>
            file.path === current.path
              ? { path: file.path, state: 'failed', error: message }
              : file,
          ),
        };
        await save(job);
      }
    }
  } finally {
    try {
      const job = await requireJob(id);
      const stopped = controller.signal.aborted;
      const files = job.files.map((file) =>
        file.state === 'running'
          ? {
              path: file.path,
              state: stopped ? ('pending' as const) : ('failed' as const),
              ...(!stopped ? { error: 'Indexing ended before this file completed' } : {}),
            }
          : file,
      );
      const state: IngestJobState = stopped
        ? 'stopped'
        : files.some((file) => file.state === 'pending')
          ? 'interrupted'
          : files.some((file) => file.state === 'failed')
            ? 'completed_with_errors'
            : 'completed';
      const finalProgress = job.progress
        ? {
            ...job.progress,
            progressPercent:
              state === 'completed' || state === 'completed_with_errors'
                ? 100
                : job.progress.progressPercent,
          }
        : undefined;
      try {
        await save({ ...job, files, state, ...(finalProgress ? { progress: finalProgress } : {}) });
      } catch (error) {
        const current = jobs?.[job.collection];
        if (current?.id === id) {
          jobs = {
            ...jobs,
            [job.collection]: {
              ...current,
              state: 'interrupted',
              files: current.files.map((file) =>
                file.state === 'running' ? { path: file.path, state: 'pending' } : file,
              ),
            },
          };
        }
        console.error('[ingest-job:persist]', error);
      }
    } finally {
      controllers.delete(id);
      workers.delete(id);
    }
  }
};

export const getIngestJob = async (collection: string): Promise<IngestJob | null> => {
  await load();
  return jobs![collection] ?? null;
};

export const createIngestJob = async (
  folderPath: string,
  collection: string,
  selectedFiles: readonly string[],
): Promise<IngestJob> => {
  await load();
  if (workers.size > 0 || startingJob)
    throw Object.assign(new Error('Another saved indexing job is already running'), {
      status: 409,
    });
  startingJob = true;
  try {
    if (
      !Array.isArray(selectedFiles) ||
      selectedFiles.length === 0 ||
      selectedFiles.length > 10_000 ||
      selectedFiles.some((file) => typeof file !== 'string' || file.length > 4096)
    ) {
      throw Object.assign(new Error('selectedFiles must contain 1 to 10000 file paths'), {
        status: 400,
      });
    }
    const resolvedFolder = resolve(folderPath);
    const available = await listExportFiles(resolvedFolder);
    const availableSet = new Set(available.map((file) => resolve(file)));
    const unique = [...new Set(selectedFiles.map((file) => resolve(file)))];
    if (unique.length !== selectedFiles.length || unique.some((file) => !availableSet.has(file))) {
      throw Object.assign(new Error('selectedFiles must be unique export files from folderPath'), {
        status: 400,
      });
    }
    const previous = jobs![collection];
    if (previous?.state === 'running')
      throw Object.assign(new Error('An indexing job for this collection is already running'), {
        status: 409,
      });
    const job: IngestJob = {
      id: randomUUID(),
      collection,
      folderPath: resolvedFolder,
      state: 'running',
      files: unique.map((path) => ({ path, state: 'pending' })),
    };
    await save(job);
    const controller = new AbortController();
    controllers.set(job.id, controller);
    const worker = runJob(job.id);
    workers.set(job.id, worker);
    void worker.catch((error) => console.error('[ingest-job]', error));
    return job;
  } finally {
    startingJob = false;
  }
};

export const stopIngestJob = async (id: string): Promise<IngestJob> => {
  const job = await requireJob(id);
  if (job.state !== 'running') return job;
  controllers.get(id)?.abort(new DOMException('Indexing stopped', 'AbortError'));
  await workers.get(id);
  return requireJob(id);
};

export const continueIngestJob = async (id: string): Promise<IngestJob> => {
  const job = await requireJob(id);
  if (workers.size > 0)
    throw Object.assign(new Error('Another saved indexing job is already running'), {
      status: 409,
    });
  if (job.state === 'running')
    throw Object.assign(new Error('Indexing job is already running'), { status: 409 });
  const files = job.files.map((file) =>
    file.state === 'failed' ? { path: file.path, state: 'pending' as const } : file,
  );
  if (!files.some((file) => file.state === 'pending'))
    throw Object.assign(new Error('No files remain to index'), { status: 409 });
  const continued = { ...job, state: 'running' as const, files };
  await save(continued);
  const controller = new AbortController();
  controllers.set(id, controller);
  const worker = runJob(id);
  workers.set(id, worker);
  void worker.catch((error) => console.error('[ingest-job]', error));
  return continued;
};
