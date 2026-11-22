import { dataPath } from './paths.js';
import {
  CLOUDFLARE_EMBEDDING_DIMENSION,
  CLOUDFLARE_MAX_INPUT_TOKENS,
  countCloudflareInputTokens,
  embedWithCloudflare,
} from './cloudflare-embedding.js';

interface PipelineOutput {
  readonly data: Float32Array;
  readonly dims?: number[];
}

interface EmbeddingTokenizer {
  readonly model_max_length?: number;
  encode: (text: string) => number[];
}

type Pipeline = ((
  text: string | string[],
  options: Record<string, unknown>,
) => Promise<PipelineOutput>) & {
  readonly tokenizer?: EmbeddingTokenizer;
  readonly model?: { readonly config?: { readonly max_position_embeddings?: number } };
};

const MODEL = 'Xenova/paraphrase-multilingual-MiniLM-L12-v2';
const DIMENSION = 384;
let loadedPipeline: Promise<Pipeline> | undefined;

export const getDimension = (): number => {
  return process.env.THREADSHELF_EMBEDDING_PROVIDER === 'cloudflare'
    ? CLOUDFLARE_EMBEDDING_DIMENSION
    : DIMENSION;
};

const loadPipeline = async (): Promise<Pipeline> => {
  const { env, pipeline } = await import('@huggingface/transformers');
  // Transformers.js caches model weights inside its own node_modules folder by
  // default. For an `npx threadshelf` install that directory is disposable, so
  // the ~50 MB model would be re-downloaded on every run. Keep it with the
  // user's data instead.
  env.cacheDir = process.env.THREADSHELF_MODEL_CACHE || dataPath('modelCache');
  return (await pipeline('feature-extraction', MODEL, {
    dtype: 'q8',
    progress_callback: (p: unknown) => {
      const info = p as { status?: string; file?: string; progress?: number };
      if (info.status === 'progress') console.error('[embedding]', info.file, info.progress);
    },
  })) as Pipeline;
};

const getPipeline = (): Promise<Pipeline> => {
  loadedPipeline ??= loadPipeline().catch((error) => {
    loadedPipeline = undefined;
    throw error;
  });
  return loadedPipeline;
};

export const createEmbedder = (load: () => Promise<Pipeline>) => {
  let pipeline: Promise<Pipeline> | undefined;
  return async (texts: string[]): Promise<number[][]> => {
    if (texts.length === 0) return [];
    pipeline ??= load().catch((error) => {
      pipeline = undefined;
      throw error;
    });
    const pipe = await pipeline;
    const options = { pooling: 'mean', normalize: true };

    try {
      const out = await pipe(texts, options);
      const data = Array.from(out.data);
      const batchSize = out.dims?.[0] === texts.length ? texts.length : texts.length === 1 ? 1 : 0;
      if (batchSize > 0 && data.length % batchSize === 0) {
        const stride = data.length / batchSize;
        return texts.map((_, index) => data.slice(index * stride, (index + 1) * stride));
      }
    } catch (e) {
      if (texts.length === 1) throw e;
    }

    const results: number[][] = [];
    for (const text of texts) {
      const out = await pipe(text, options);
      results.push(Array.from(out.data));
    }
    return results;
  };
};

const embedLocal = createEmbedder(getPipeline);
export const embed = (texts: string[]): Promise<number[][]> =>
  process.env.THREADSHELF_EMBEDDING_PROVIDER === 'cloudflare'
    ? embedWithCloudflare(texts)
    : embedLocal(texts);

export interface EmbeddingLimits {
  readonly maxInputTokens: number;
  readonly countTokens: (text: string) => number;
}

/** Use the active model's tokenizer and context limit; do not guess from characters. */
export const getEmbeddingLimits = async (): Promise<EmbeddingLimits> => {
  if (process.env.THREADSHELF_EMBEDDING_PROVIDER === 'cloudflare') {
    return {
      maxInputTokens: CLOUDFLARE_MAX_INPUT_TOKENS,
      countTokens: countCloudflareInputTokens,
    };
  }
  const pipe = await getPipeline();
  const tokenizer = pipe.tokenizer;
  if (!tokenizer) throw new Error('Embedding pipeline did not expose its tokenizer');

  const limits = [tokenizer.model_max_length, pipe.model?.config?.max_position_embeddings].filter(
    (value): value is number => typeof value === 'number' && Number.isFinite(value) && value > 0,
  );
  if (limits.length === 0) throw new Error('Embedding model did not expose its input token limit');

  return {
    maxInputTokens: Math.floor(Math.min(...limits)),
    countTokens: (text) => tokenizer.encode(text).length,
  };
};

export const embedOne = async (text: string): Promise<number[]> => {
  const [v] = await embed([text]);
  return v!;
};
