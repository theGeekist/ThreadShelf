import { useState, useRef, useMemo, useCallback, useEffect } from 'react';
import type {
  FilePreview,
  IngestJob,
  IngestProgress,
  IngestStreamEvent,
  StatusMessage,
} from '../types';
import { Icons } from '../icons';
import { api } from '../api';
import { collLabel, fmtTime, isPotentialExport } from '../utils';
import { NewCollectionModal } from './NewCollectionModal';

function formatProgressStep(event: IngestStreamEvent): string {
  const total = event.totalFiles || 1;
  const done = event.processedFiles || 0;
  const phase = (event.phase || '').toLowerCase();

  if (phase === 'uploading') return `Uploading ${done} / ${total} files`;
  if (phase === 'reading') return `Reading ${done} / ${total} files`;
  if (phase === 'embedding')
    return event.embeddingTotal !== undefined
      ? `Embedding ${event.embeddingDone ?? 0} / ${event.embeddingTotal} chunks`
      : `Embedding ${event.embeddingDone ?? 0} chunks`;
  if (event.status === 'starting') return `Starting 0 / ${total} files`;
  return `${done} / ${total} files`;
}

function progressFromEvent(event: IngestStreamEvent): IngestProgress {
  const total = event.totalFiles || 1;
  const done = event.processedFiles || 0;

  return {
    pct: Math.round(event.progressPercent ?? (done / total) * 100),
    step: formatProgressStep(event),
    chunks: event.totalChunks,
    time: event.elapsedMs,
    file: event.currentFile,
  };
}

function formatCompletedMessage(event: IngestStreamEvent): string {
  const result = event.result;
  if (result?.replacementSkipped)
    return 'Nothing was saved. The existing collection was kept because the folder contains empty or invalid exports, or no conversations. Fix the exports and retry.';
  const fileCount = result?.files?.length ?? 0;
  const conversations = result?.conversations ?? fileCount;
  const errorCount = result?.errors?.length ?? 0;

  const summary = `Indexed ${conversations} conversations as ${result?.ingested ?? 0} chunks from ${fileCount} files in ${fmtTime(result?.elapsedMs ?? 0)}.`;
  return errorCount
    ? `${summary} ${errorCount} file${errorCount === 1 ? '' : 's'} failed.`
    : summary;
}

interface IndexingViewProps {
  readonly collections: readonly string[];
  readonly onRefresh: () => Promise<void>;
}

type RelativeFile = File & { readonly webkitRelativePath?: string };
type DragFileSystemEntry =
  | {
      readonly isFile: true;
      readonly isDirectory: false;
      readonly name: string;
      file: (success: (file: File) => void, failure: (error: DOMException) => void) => void;
    }
  | {
      readonly isFile: false;
      readonly isDirectory: true;
      readonly name: string;
      createReader: () => {
        readEntries: (
          success: (entries: DragFileSystemEntry[]) => void,
          failure: (error: DOMException) => void,
        ) => void;
      };
    };
type DataTransferItemWithEntry = DataTransferItem & {
  readonly webkitGetAsEntry?: () => unknown;
};

const relativePathOf = (file: RelativeFile): string => file.webkitRelativePath || file.name;

const withRelativePath = (file: File, relativePath: string): RelativeFile => {
  if ((file as RelativeFile).webkitRelativePath) return file as RelativeFile;
  try {
    Object.defineProperty(file, 'webkitRelativePath', {
      configurable: true,
      value: relativePath,
    });
  } catch {
    // Some browsers expose immutable File objects; the filename fallback still works.
  }
  return file as RelativeFile;
};

const readAllEntries = async (entry: Extract<DragFileSystemEntry, { isDirectory: true }>) => {
  const reader = entry.createReader();
  const all: DragFileSystemEntry[] = [];
  while (true) {
    const batch = await new Promise<DragFileSystemEntry[]>((resolve, reject) => {
      reader.readEntries(resolve, reject);
    });
    if (!batch.length) break;
    all.push(...batch);
  }
  return all;
};

const filesFromEntry = async (entry: DragFileSystemEntry, prefix = ''): Promise<RelativeFile[]> => {
  if (entry.isFile) {
    const file = await new Promise<File>((resolve, reject) => entry.file(resolve, reject));
    return [withRelativePath(file, `${prefix}${file.name}`)];
  }
  const children = await readAllEntries(entry);
  const nested = await Promise.all(
    children.map((child) => filesFromEntry(child, `${prefix}${entry.name}/`)),
  );
  return nested.flat();
};

export function IndexingView({ collections, onRefresh }: IndexingViewProps) {
  const [dragging, setDragging] = useState(false);
  const [path, setPath] = useState('');
  const [uploadFiles, setUploadFiles] = useState<RelativeFile[] | null>(null);
  const [pickedLabel, setPickedLabel] = useState('');
  const [clearFirst, setClearFirst] = useState(false);
  const [targetColl, setTargetColl] = useState(collections.find((c) => c !== 'all') ?? 'chunks');
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState<IngestProgress | null>(null);
  const [statusMsg, setStatusMsg] = useState<StatusMessage | null>(null);
  const [preview, setPreview] = useState<FilePreview[] | null>(null);
  const [selectedPaths, setSelectedPaths] = useState<Set<string>>(new Set());
  const [job, setJob] = useState<IngestJob | null>(null);
  const [jobBusy, setJobBusy] = useState(false);
  const [newCollOpen, setNewCollOpen] = useState(false);
  const [copied, setCopied] = useState('');
  const pickerRef = useRef<HTMLInputElement>(null);
  const indexingController = useRef<AbortController | null>(null);
  const restoredJob = useRef(false);

  const refreshJob = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const response = await api.ingestJob(targetColl, signal);
        setJob(response.job);
        if (response.job && !restoredJob.current) {
          restoredJob.current = true;
          setPath(response.job.folderPath);
          setPreview(
            response.job.files.map((file) => ({
              name: file.path.replace(/\\/g, '/').split('/').pop() ?? file.path,
              ok: true,
              sourcePath: file.path,
              note: file.state === 'failed' ? 'failed' : '',
            })),
          );
          setSelectedPaths(new Set(response.job.files.map((file) => file.path)));
        }
      } catch (e) {
        const status =
          typeof e === 'object' && e !== null && 'status' in e ? Number(e.status) : undefined;
        if (!signal?.aborted && status !== 404) {
          setStatusMsg({
            type: 'err',
            text: `Could not load saved indexing progress: ${e instanceof Error ? e.message : 'Unknown error'}`,
          });
        }
      }
    },
    [targetColl],
  );

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => refreshJob(controller.signal);
    void load();
    return () => controller.abort();
  }, [refreshJob]);

  useEffect(() => {
    if (job?.state !== 'running') return;
    let active = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        const response = await api.ingestJob(job.collection);
        if (!active) return;
        setJob(response.job);
      } catch (e) {
        const status =
          typeof e === 'object' && e !== null && 'status' in e ? Number(e.status) : undefined;
        if (active && status !== 404)
          setStatusMsg({
            type: 'err',
            text: `Progress update failed: ${e instanceof Error ? e.message : 'Unknown error'}`,
          });
      }
      if (active) timer = setTimeout(() => void poll(), 1500);
    };
    timer = setTimeout(() => void poll(), 1000);
    return () => {
      active = false;
      if (timer) clearTimeout(timer);
    };
  }, [job?.id, job?.collection, job?.state]);

  const copyScript = useCallback(async (file: string) => {
    try {
      const res = await fetch(`/scripts/${file}`);
      if (!res.ok) throw new Error('not found');
      await navigator.clipboard.writeText(await res.text());
      setCopied(file);
      setTimeout(() => setCopied(''), 2500);
    } catch {
      setStatusMsg({ type: 'err', text: `Could not copy script — open scripts/${file} manually.` });
    }
  }, []);

  const supported = useMemo(
    () => (uploadFiles ? uploadFiles.filter((f) => isPotentialExport(relativePathOf(f))) : []),
    [uploadFiles],
  );

  const applyPickedFiles = useCallback((files: readonly RelativeFile[]) => {
    if (!files.length) return;

    setUploadFiles([...files]);
    setPath('');
    const first = relativePathOf(files[0]!).replace(/\\/g, '/');
    const folder = first.split('/')[0] ?? 'folder';
    const sup = files.filter((f) => isPotentialExport(relativePathOf(f)));
    setPickedLabel(`${sup.length} files from "${folder}"`);
    setPreview(
      files.map((f) => {
        const relPath = relativePathOf(f);
        return {
          name: relPath.replace(/\\/g, '/').split('/').pop() ?? '',
          ok: isPotentialExport(relPath),
          sourcePath: relPath,
        };
      }),
    );
    setSelectedPaths(
      new Set(files.filter((f) => isPotentialExport(relativePathOf(f))).map(relativePathOf)),
    );
  }, []);

  const onPickFolder = useCallback(() => {
    const files = pickerRef.current?.files;
    if (!files?.length) return;
    applyPickedFiles(Array.from(files) as RelativeFile[]);
  }, [applyPickedFiles]);

  const onDropFiles = useCallback(
    async (dataTransfer: DataTransfer) => {
      try {
        const entries = Array.from(dataTransfer.items)
          .map(
            (item) =>
              (item as DataTransferItemWithEntry).webkitGetAsEntry?.() as
                DragFileSystemEntry | null | undefined,
          )
          .filter((entry): entry is DragFileSystemEntry => !!entry);
        const files = entries.length
          ? (await Promise.all(entries.map((entry) => filesFromEntry(entry)))).flat()
          : (Array.from(dataTransfer.files) as RelativeFile[]);
        if (!files.length) {
          setPickedLabel('Drop did not include readable files. Use Choose folder instead.');
          return;
        }
        applyPickedFiles(files);
      } catch (e) {
        setPickedLabel(`Drop failed: ${e instanceof Error ? e.message : 'Unknown error'}`);
      }
    },
    [applyPickedFiles],
  );

  const onUsePath = useCallback(async () => {
    if (!path.trim()) return;
    setPickedLabel('Checking…');
    try {
      const d = await api.ingestPreview(path, targetColl);
      setUploadFiles(null);
      const duplicatePaths = new Set(d.duplicates.map((item) => item.sourceFile));
      setPickedLabel(
        `${d.files.length} export files found${
          duplicatePaths.size ? `, ${duplicatePaths.size} will be re-indexed` : ''
        }`,
      );
      setPreview(
        d.files.map((f) => {
          const name = String(f).replace(/\\/g, '/').split('/').pop() ?? '';
          return {
            name,
            ok: true,
            note: duplicatePaths.has(f) ? 're-index' : '',
            sourcePath: f,
          };
        }),
      );
      setSelectedPaths(new Set(d.files));
    } catch (e) {
      setPickedLabel(`Error: ${e instanceof Error ? e.message : 'Unknown'}`);
    }
  }, [path, targetColl]);

  const doIndex = useCallback(async () => {
    const useUpload = uploadFiles && supported.length > 0;
    let coll = targetColl;

    if (useUpload && coll === 'chunks') {
      const first = relativePathOf(uploadFiles[0]!).replace(/\\/g, '/');
      coll = first.split('/').filter(Boolean)[0] ?? coll;
    }

    if (!useUpload && !path.trim()) {
      setStatusMsg({ type: 'err', text: 'Choose a folder or enter a path.' });
      return;
    }

    if (!useUpload && !clearFirst) {
      const selected = Array.from(selectedPaths);
      if (!preview || selected.length === 0) {
        setStatusMsg({
          type: 'err',
          text: 'Preview the folder and select at least one supported file.',
        });
        return;
      }
      setJobBusy(true);
      setStatusMsg(null);
      try {
        const response = await api.startIngestJob(path, coll, selected);
        setJob(response.job);
        setTargetColl(coll);
        setStatusMsg({
          type: 'info',
          text: 'Indexing was accepted and will continue if you leave this page.',
        });
        void onRefresh();
      } catch (e) {
        const status =
          typeof e === 'object' && e !== null && 'status' in e ? Number(e.status) : undefined;
        const supportedCount = preview?.filter((file) => file.ok && file.sourcePath).length ?? 0;
        if (status === 404 && selected.length === supportedCount) {
          setRunning(true);
          setProgress({ pct: 0, step: 'Starting…' });
          setStatusMsg({
            type: 'info',
            text: 'Saved jobs are not available on this server yet. Using the existing indexer; this run will not resume after a reload.',
          });
          const controller = new AbortController();
          indexingController.current = controller;
          try {
            await api.ingestPathProgress(
              path,
              coll,
              false,
              (event) => {
                if (event.status === 'completed') {
                  setStatusMsg({
                    type: event.result?.errors?.length ? 'err' : 'info',
                    text: formatCompletedMessage(event),
                  });
                } else if (event.status === 'error') {
                  setStatusMsg({ type: 'err', text: `Error: ${event.error ?? 'Unknown'}` });
                } else {
                  setProgress(progressFromEvent(event));
                }
              },
              controller.signal,
            );
          } catch (fallbackError) {
            setStatusMsg({
              type: 'err',
              text: `Existing indexer failed: ${fallbackError instanceof Error ? fallbackError.message : 'Unknown error'}`,
            });
          } finally {
            if (indexingController.current === controller) indexingController.current = null;
            setRunning(false);
            setProgress(null);
            void onRefresh();
          }
        } else {
          setStatusMsg({
            type: 'err',
            text: `Could not start saved indexing: ${e instanceof Error ? e.message : 'Unknown error'}`,
          });
        }
      } finally {
        setJobBusy(false);
      }
      return;
    }

    setRunning(true);
    setProgress({ pct: 0, step: 'Starting…' });
    setStatusMsg(null);
    const controller = new AbortController();
    indexingController.current = controller;

    const onProgress = (event: IngestStreamEvent) => {
      if (event.status === 'completed') {
        setStatusMsg({
          type: event.result?.errors?.length || event.result?.replacementSkipped ? 'err' : 'info',
          text: formatCompletedMessage(event),
        });
        return;
      }
      if (event.status === 'error') {
        setStatusMsg({ type: 'err', text: `Error: ${event.error ?? 'Unknown'}` });
        return;
      }

      setProgress(progressFromEvent(event));
    };

    try {
      if (useUpload) {
        await api.ingestUploadProgress(
          supported.filter((f) => selectedPaths.has(relativePathOf(f))),
          coll,
          clearFirst,
          onProgress,
          controller.signal,
        );
      } else {
        await api.ingestPathProgress(path, coll, clearFirst, onProgress, controller.signal);
      }
    } catch (e) {
      if (controller.signal.aborted) {
        setStatusMsg({ type: 'info', text: 'Indexing stopped. Completed files remain indexed.' });
      } else {
        setStatusMsg({ type: 'err', text: `Error: ${e instanceof Error ? e.message : 'Unknown'}` });
      }
    } finally {
      if (indexingController.current === controller) indexingController.current = null;
      setRunning(false);
      setProgress(null);
      void onRefresh();
    }
  }, [uploadFiles, supported, targetColl, path, clearFirst, onRefresh, selectedPaths, preview]);

  const controlJob = useCallback(
    async (action: 'stop' | 'continue') => {
      if (!job) return;
      setJobBusy(true);
      setStatusMsg(null);
      try {
        const response =
          action === 'stop' ? await api.stopIngestJob(job.id) : await api.continueIngestJob(job.id);
        setJob(response.job);
        if (action === 'continue')
          setStatusMsg({ type: 'info', text: 'Continuing the saved indexing job.' });
        void onRefresh();
      } catch (e) {
        setStatusMsg({
          type: 'err',
          text: `Could not ${action} indexing: ${e instanceof Error ? e.message : 'Unknown error'}`,
        });
      } finally {
        setJobBusy(false);
      }
    },
    [job, onRefresh],
  );

  const togglePath = (filePath: string) => {
    setSelectedPaths((current) => {
      const next = new Set(current);
      if (next.has(filePath)) next.delete(filePath);
      else next.add(filePath);
      return next;
    });
  };

  const selectablePreview = preview?.filter((file) => file.ok && file.sourcePath) ?? [];
  const allPreviewSelected =
    selectablePreview.length > 0 &&
    selectablePreview.every((file) => selectedPaths.has(file.sourcePath!));
  const jobFileCounts = job?.files.reduce(
    (counts, file) => {
      counts[file.state] = (counts[file.state] ?? 0) + 1;
      return counts;
    },
    {} as Record<string, number>,
  );
  const jobProgress = job?.progress;

  const realCollections = collections.filter((c) => c !== 'all');

  return (
    <div className="view">
      <div className="section-h">
        <h2>Index a folder</h2>
        <span className="desc">
          Local parse → chunk → embed → write to LanceDB. Your chat data stays on this machine.
        </span>
      </div>

      <details className="panel" style={{ marginBottom: 16 }}>
        <summary
          style={{ cursor: 'pointer', padding: '12px 14px', fontWeight: 500, listStyle: 'revert' }}
        >
          {Icons.info} Where to get your data (export guides)
        </summary>
        <div
          style={{
            padding: '4px 14px 16px',
            fontSize: 13,
            color: 'var(--text-2)',
            display: 'grid',
            gap: 16,
          }}
        >
          <div>
            <b style={{ color: 'var(--text-1)' }}>OpenRouter</b> — no built-in export. Run a
            browser-console script, then index the downloaded folder:
            <ol style={{ margin: '6px 0', paddingLeft: 20, lineHeight: 1.7 }}>
              <li>Open openrouter.ai signed in, with your chat list visible.</li>
              <li>
                Open DevTools → Console (<kbd>F12</kbd>).
              </li>
              <li>Copy a script below, paste into the console, press Enter.</li>
              <li>Allow “multiple downloads” if asked; then index the folder here.</li>
            </ol>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              <button
                className="btn sm"
                onClick={() => void copyScript('openrouter-export-all.js')}
              >
                {Icons.copy}{' '}
                {copied === 'openrouter-export-all.js'
                  ? 'Copied!'
                  : 'Copy “export all chats” script'}
              </button>
              <button
                className="btn sm"
                onClick={() => void copyScript('openrouter-export-browser.js')}
              >
                {Icons.copy}{' '}
                {copied === 'openrouter-export-browser.js'
                  ? 'Copied!'
                  : 'Copy “single chat” script'}
              </button>
            </div>
          </div>

          <div>
            <b style={{ color: 'var(--text-1)' }}>LM Studio</b> — point at its local conversation
            folder (paste into “paste path” below):
            <ul style={{ margin: '6px 0', paddingLeft: 20, lineHeight: 1.8 }}>
              <li>
                Windows: <code>%USERPROFILE%\.lmstudio\conversations\</code>
              </li>
              <li>
                macOS / Linux: <code>~/.lmstudio/conversations/</code>
              </li>
              <li>
                Older builds: <code>~/.cache/lm-studio/conversations/</code>
              </li>
            </ul>
          </div>

          <div>
            <b style={{ color: 'var(--text-1)' }}>Google AI Studio</b> — download your Google Drive
            “Google AI Studio” folder, unzip it, and index that folder (files may have no extension
            — that’s fine). Text chats and Imagen prompt histories are supported.
          </div>

          <div>
            <b style={{ color: 'var(--text-1)' }}>ChatGPT / Claude</b> — use the app’s official data
            export, then index the <code>conversations.json</code> it produces.
          </div>
        </div>
      </details>

      <div
        className="drop-zone"
        data-dragging={dragging}
        onDragOver={(e) => {
          e.preventDefault();
          setDragging(true);
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={(e) => {
          e.preventDefault();
          setDragging(false);
          void onDropFiles(e.dataTransfer);
        }}
      >
        <div className="ico-big">{Icons.folder}</div>
        <h3>Drop a folder of exports here</h3>
        <p>
          Supported: Google AI Studio, OpenRouter, ChatGPT/OpenAI, Claude/Anthropic, LM Studio, and
          Grok JSON exports. Subfolders are recursed; account metadata files are skipped
          automatically.
        </p>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          <input
            id="folderFileInput"
            ref={pickerRef}
            type="file"
            style={{ display: 'none' }}
            {...({
              webkitdirectory: '',
              directory: '',
            } as React.InputHTMLAttributes<HTMLInputElement>)}
            multiple
            onChange={onPickFolder}
          />
          <button className="btn primary" onClick={() => pickerRef.current?.click()}>
            {Icons.folder} Choose folder
          </button>
          {pickedLabel && (
            <span
              id="folderChosenLabel"
              className="mono"
              style={{ fontSize: 12, color: 'var(--text-2)' }}
            >
              {pickedLabel}
            </span>
          )}
        </div>
        <div style={{ fontSize: 11, color: 'var(--text-3)', margin: '4px 0' }}>or paste path</div>
        <div className="path-input">
          <input
            type="text"
            value={path}
            onChange={(e) => setPath(e.target.value)}
            placeholder="/absolute/path/to/exports"
          />
          <button className="btn" onClick={onUsePath} disabled={!path}>
            {Icons.arrowRight} Use path
          </button>
        </div>
      </div>

      <div className="ingest-options">
        <label style={{ color: 'var(--text-3)' }}>Target collection</label>
        <select
          id="targetCollectionSelect"
          value={targetColl}
          onChange={(e) => setTargetColl(e.target.value)}
          className="btn sm"
          style={{ padding: '0 8px', background: 'var(--bg-2)' }}
        >
          {realCollections.map((c) => (
            <option key={c} value={c}>
              {collLabel(c)}
            </option>
          ))}
        </select>
        <button
          id="newCollectionBtnIndexing"
          className="btn sm"
          onClick={() => setNewCollOpen(true)}
          title="Create a new collection"
        >
          {Icons.plus} New
        </button>
        <label>
          <input
            type="checkbox"
            checked={clearFirst}
            onChange={(e) => setClearFirst(e.target.checked)}
          />{' '}
          Clear collection first
        </label>
        <button
          id="ingestBtn"
          className="btn primary"
          onClick={doIndex}
          disabled={running || jobBusy || job?.state === 'running'}
        >
          {Icons.zap}{' '}
          {running || jobBusy || job?.state === 'running' ? 'Indexing…' : 'Index folder'}
        </button>
        {running && (
          <button
            id="stopIngestBtn"
            className="btn danger"
            onClick={() => indexingController.current?.abort()}
          >
            Stop indexing
          </button>
        )}
      </div>

      {preview && preview.length > 0 && (
        <div className="panel">
          <div className="panel-head">
            <h3>Ingest preview</h3>
            <span className="sub">
              {selectedPaths.size} selected · {preview.filter((f) => f.ok).length} supported ·{' '}
              {preview.filter((f) => !f.ok).length} skipped
            </span>
          </div>
          {selectablePreview.length > 0 && (
            <label className="preview-select-all">
              <input
                type="checkbox"
                checked={allPreviewSelected}
                onChange={() =>
                  setSelectedPaths(
                    allPreviewSelected
                      ? new Set()
                      : new Set(selectablePreview.map((f) => f.sourcePath!)),
                  )
                }
              />{' '}
              Select all supported files
            </label>
          )}
          <div style={{ maxHeight: 260, overflow: 'auto' }}>
            {preview.map((f, i) => (
              <div
                key={i}
                style={{
                  display: 'grid',
                  gridTemplateColumns: '20px 20px 1fr auto',
                  alignItems: 'center',
                  gap: 8,
                  padding: '6px 14px',
                  borderBottom: '1px solid var(--border-0)',
                  fontSize: 12,
                }}
              >
                <input
                  type="checkbox"
                  aria-label={`Select ${f.name}`}
                  disabled={!f.ok || !f.sourcePath}
                  checked={!!f.sourcePath && selectedPaths.has(f.sourcePath)}
                  onChange={() => f.sourcePath && togglePath(f.sourcePath)}
                />
                <span style={{ color: f.ok ? 'var(--success)' : 'var(--text-4)' }}>
                  {f.ok ? Icons.check : '·'}
                </span>
                <span
                  style={{
                    fontFamily: 'var(--font-mono)',
                    color: f.ok ? 'var(--text-1)' : 'var(--text-3)',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {f.name}
                </span>
                <span
                  style={{ fontFamily: 'var(--font-mono)', fontSize: 10.5, color: 'var(--text-3)' }}
                >
                  {f.note || (f.ok ? '' : 'skipped')}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {progress && (
        <div className="progress-panel">
          <div className="progress-head">
            <span className="step">
              <b>{progress.step}</b>
            </span>
            <span className="pct">{progress.pct}%</span>
          </div>
          <div className="progress-bar-new">
            <i style={{ width: `${progress.pct}%` }} />
          </div>
          {progress.chunks != null && (
            <div className="progress-details-new">
              Chunks: <strong>{progress.chunks.toLocaleString()}</strong>
              <span style={{ color: 'var(--text-4)' }}>·</span>
              Time: <strong>{fmtTime(progress.time ?? 0)}</strong>
            </div>
          )}
          {progress.file && (
            <div className="progress-file">{progress.file.replace(/^.*[\\/]/, '')}</div>
          )}
        </div>
      )}

      {job && (
        <div className="panel saved-job-panel" aria-live="polite">
          <div className="panel-head">
            <h3>Saved indexing job</h3>
            <span className={`job-state ${job.state}`}>{job.state.replace(/_/g, ' ')}</span>
          </div>
          <div className="job-meta">
            <span>{job.collection}</span>
            <code title={job.folderPath}>{job.folderPath}</code>
          </div>
          {jobProgress && (
            <div className="job-progress">
              <div className="progress-head">
                <span className="step">
                  <b>{formatProgressStep(jobProgress)}</b>
                </span>
                <span className="pct">{Math.round(jobProgress.progressPercent ?? 0)}%</span>
              </div>
              <div className="progress-bar-new">
                <i
                  style={{
                    width: `${Math.max(0, Math.min(100, jobProgress.progressPercent ?? 0))}%`,
                  }}
                />
              </div>
            </div>
          )}
          <div className="job-meta">
            Files: <strong>{job.files.length} accepted</strong>
            <span>{jobFileCounts?.completed ?? 0} completed</span>
            <span>{jobFileCounts?.failed ?? 0} failed</span>
            <span>{jobFileCounts?.pending ?? 0} pending</span>
          </div>
          {(job.state === 'interrupted' || job.state === 'stopped') && (
            <p className="job-note">
              Saved progress is available. Continue retries unfinished files and keeps completed
              work.
            </p>
          )}
          {job.state === 'completed_with_errors' && (
            <p className="job-note">
              The job finished with file failures. Review the file list below or retry the failed
              files.
            </p>
          )}
          {(job.state === 'interrupted' ||
            job.state === 'stopped' ||
            job.state === 'completed_with_errors') && (
            <button
              className="btn primary"
              disabled={jobBusy}
              onClick={() => void controlJob('continue')}
            >
              {Icons.arrowRight} Continue
            </button>
          )}
          {job.state === 'running' && (
            <button
              className="btn danger"
              disabled={jobBusy}
              onClick={() => void controlJob('stop')}
            >
              Pause indexing
            </button>
          )}
          <div className="job-files">
            {job.files.map((file) => (
              <div key={file.path} className="job-file">
                <span className={`file-state ${file.state}`}>{file.state}</span>
                <code title={file.path}>{file.path}</code>
                {file.error && (
                  <span className="file-error" title={file.error}>
                    {file.error}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {statusMsg && (
        <div id="ingestStatus" className={`banner ${statusMsg.type}`}>
          <span className="ico">{statusMsg.type === 'err' ? Icons.warn : Icons.check}</span>
          <div className="grow">{statusMsg.text}</div>
        </div>
      )}

      {newCollOpen && (
        <NewCollectionModal
          open
          onClose={() => setNewCollOpen(false)}
          onCreated={(name) => {
            setTargetColl(name);
            void onRefresh();
          }}
        />
      )}
    </div>
  );
}
