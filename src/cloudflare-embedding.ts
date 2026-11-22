const MODEL_ID = '@cf/baai/bge-m3';
export const CLOUDFLARE_EMBEDDING_DIMENSION = 1024;
export const CLOUDFLARE_MAX_INPUT_TOKENS = 60_000;

const getAccountId = (): string => process.env.CLOUDFLARE_ACCOUNT_ID?.trim() || '';
const getApiToken = (): string => process.env.CLOUDFLARE_API_TOKEN?.trim() || '';

interface WorkersAiResponse {
  readonly success?: boolean;
  readonly errors?: readonly { readonly message?: string }[];
  readonly data?: readonly (readonly number[])[];
  readonly embeddings?: readonly (readonly number[])[];
  readonly result?: {
    readonly data?: readonly (readonly number[])[];
    readonly embeddings?: readonly (readonly number[])[];
  };
}

const endpoint = (): string => {
  const accountId = getAccountId();
  if (!accountId || !getApiToken()) {
    throw new Error('Cloudflare embeddings require CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN');
  }
  return `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(accountId)}/ai/run/${MODEL_ID}`;
};

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

export const parseCloudflareVectors = (payload: WorkersAiResponse): number[][] => {
  if (payload.success === false) {
    throw new Error(
      payload.errors
        ?.map((error) => error.message)
        .filter(Boolean)
        .join('; ') || 'Workers AI request failed',
    );
  }
  const vectors =
    payload.result?.data ?? payload.result?.embeddings ?? payload.data ?? payload.embeddings;
  if (!Array.isArray(vectors) || vectors.length === 0) {
    throw new Error('Workers AI returned no BGE-M3 vectors');
  }
  return vectors.map((vector, index) => {
    if (!Array.isArray(vector) || vector.length !== CLOUDFLARE_EMBEDDING_DIMENSION) {
      throw new Error(
        `Workers AI returned an invalid vector at index ${index}; expected ${CLOUDFLARE_EMBEDDING_DIMENSION} dimensions`,
      );
    }
    let magnitude = 0;
    for (const value of vector) {
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        throw new Error(`Workers AI returned a non-numeric vector value at index ${index}`);
      }
      magnitude += value * value;
    }
    const norm = Math.sqrt(magnitude);
    if (norm === 0) throw new Error(`Workers AI returned a zero vector at index ${index}`);
    return vector.map((value) => value / norm);
  });
};

export const embedWithCloudflare = async (texts: readonly string[]): Promise<number[][]> => {
  if (texts.length === 0) return [];
  const url = endpoint();
  for (let attempt = 0; attempt < 5; attempt++) {
    let response: Response;
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${getApiToken()}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ text: texts, truncate_inputs: false }),
      });
    } catch (error) {
      if (attempt === 4) throw error;
      await delay(Math.min(1_000 * 2 ** attempt, 15_000));
      continue;
    }

    if (response.ok) {
      const vectors = parseCloudflareVectors((await response.json()) as WorkersAiResponse);
      if (vectors.length !== texts.length) {
        throw new Error(`Workers AI returned ${vectors.length} vectors for ${texts.length} inputs`);
      }
      return vectors;
    }

    const details = (await response.text()).slice(0, 1000);
    if ((response.status === 429 || response.status >= 500) && attempt < 4) {
      const retryAfter = Number(response.headers.get('retry-after'));
      await delay(
        Number.isFinite(retryAfter) && retryAfter > 0
          ? Math.min(retryAfter * 1000, 30_000)
          : Math.min(1_000 * 2 ** attempt, 15_000),
      );
      continue;
    }
    throw new Error(`Workers AI ${MODEL_ID} failed (${response.status}): ${details}`);
  }
  throw new Error(`Workers AI ${MODEL_ID} request exhausted retries`);
};

/** UTF-8 bytes conservatively bound tokenizer output for chunk splitting. */
export const countCloudflareInputTokens = (text: string): number => Buffer.byteLength(text, 'utf8');
