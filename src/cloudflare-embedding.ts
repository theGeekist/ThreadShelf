const MODEL_ID = '@cf/baai/bge-m3';
export const CLOUDFLARE_EMBEDDING_DIMENSION = 1024;
export const CLOUDFLARE_MAX_INPUT_TOKENS = 60_000;

const getAccountId = (): string => process.env.CLOUDFLARE_ACCOUNT_ID?.trim() || '';
const getApiToken = (): string => process.env.CLOUDFLARE_API_TOKEN?.trim() || '';
const getModelId = (): string => process.env.THREADSHELF_EMBEDDING_MODEL?.trim() || MODEL_ID;
const normalizedBaseUrl = (): string =>
  process.env.THREADSHELF_EMBEDDING_BASE_URL?.trim().replace(/\/+$/, '') || '';
export const getCloudflareModelId = getModelId;
export const getCloudflareEndpointIdentity = (): string => {
  const configuredBaseUrl = normalizedBaseUrl();
  if (configuredBaseUrl) {
    try {
      const endpoint = new URL(configuredBaseUrl);
      endpoint.username = '';
      endpoint.password = '';
      endpoint.search = '';
      endpoint.hash = '';
      return endpoint.toString().replace(/\/+$/, '');
    } catch {
      return 'custom-endpoint';
    }
  }
  const accountId = getAccountId();
  return accountId
    ? `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(accountId)}/ai/v1`
    : 'cloudflare';
};
export const getCloudflareEmbeddingDimension = (): number => {
  const configured = Number(process.env.THREADSHELF_EMBEDDING_DIMENSION);
  return Number.isInteger(configured) && configured > 0
    ? configured
    : CLOUDFLARE_EMBEDDING_DIMENSION;
};
export const getCloudflareMaxInputTokens = (): number => {
  const configured = Number(process.env.THREADSHELF_EMBEDDING_MAX_INPUT_TOKENS);
  return Number.isInteger(configured) && configured > 0 ? configured : CLOUDFLARE_MAX_INPUT_TOKENS;
};

interface OpenAiEmbeddingResponse {
  readonly data?: readonly {
    readonly index?: number;
    readonly embedding?: readonly number[];
  }[];
  readonly errors?: readonly { readonly code?: number; readonly message?: string }[];
  readonly error?: { readonly code?: number | string; readonly message?: string };
}

export class CloudflareRateLimitError extends Error {
  constructor(
    message: string,
    readonly retryAt: number,
    readonly code?: number | string,
  ) {
    super(message);
    this.name = 'CloudflareRateLimitError';
  }
}

const apiConfig = (): { endpoint: string; model: string; token: string } => {
  const token = getApiToken();
  if (!token) throw new Error('Cloudflare embeddings require CLOUDFLARE_API_TOKEN');
  const configuredBaseUrl = normalizedBaseUrl();
  const accountId = getAccountId();
  const baseUrl =
    configuredBaseUrl ||
    (accountId
      ? `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(accountId)}/ai/v1`
      : '');
  if (!baseUrl) {
    throw new Error(
      'Set CLOUDFLARE_ACCOUNT_ID or THREADSHELF_EMBEDDING_BASE_URL for the OpenAI-compatible embeddings API',
    );
  }
  return { endpoint: `${baseUrl}/embeddings`, model: getModelId(), token };
};

export const getCloudflareEmbeddingProfile = (): {
  readonly endpoint: string;
  readonly model: string;
  readonly dimension: number;
  readonly maxInputTokens: number;
} => {
  const config = apiConfig();
  return {
    endpoint: config.endpoint,
    model: config.model,
    dimension: getCloudflareEmbeddingDimension(),
    maxInputTokens: getCloudflareMaxInputTokens(),
  };
};

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

// Cloudflare documents 3,000 text-embedding requests per minute. Pace below
// that ceiling to leave room for other account activity and small timing drift.
const MIN_REQUEST_INTERVAL_MS = 50;
let nextRequestAt = 0;
const waitForRequestSlot = async (): Promise<void> => {
  const now = Date.now();
  const scheduledAt = Math.max(now, nextRequestAt);
  nextRequestAt = scheduledAt + MIN_REQUEST_INTERVAL_MS;
  await delay(scheduledAt - now);
};

const errorDetails = (
  payload: OpenAiEmbeddingResponse,
  fallbackMessage?: string,
): { message: string; code?: number | string } => {
  const error = payload.error;
  const cloudflareError = payload.errors?.[0];
  return {
    message:
      error?.message ||
      cloudflareError?.message ||
      fallbackMessage ||
      'Workers AI returned an unsuccessful embeddings response',
    code: error?.code ?? cloudflareError?.code,
  };
};

const retryAfterAt = (headerValue: string | null, now = Date.now()): number | undefined => {
  if (!headerValue) return undefined;
  const seconds = Number(headerValue);
  if (Number.isFinite(seconds) && seconds >= 0) return now + seconds * 1000;
  const date = Date.parse(headerValue);
  return Number.isFinite(date) ? Math.max(now, date) : undefined;
};

const nextUtcMidnight = (now = Date.now()): number => {
  const date = new Date(now);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() + 1);
};

const rateLimitRetryAt = (
  response: Response,
  payload: OpenAiEmbeddingResponse,
): { retryAt: number; code?: number | string; message: string } => {
  const details = errorDetails(payload);
  const now = Date.now();
  const dailyQuota =
    String(details.code) === '3036' || /daily free allocation|daily neuron/i.test(details.message);
  const retryAt =
    retryAfterAt(response.headers.get('retry-after'), now) ??
    (dailyQuota ? nextUtcMidnight(now) : now + 30_000);
  return { ...details, retryAt };
};

const parseVectors = (payload: OpenAiEmbeddingResponse, expected: number): number[][] => {
  const rows = payload.data;
  if (!Array.isArray(rows) || rows.length !== expected) {
    throw new Error(`Workers AI returned ${rows?.length ?? 0} embeddings for ${expected} inputs`);
  }
  const sorted: (typeof rows)[number][] = Array(expected);
  for (const row of rows) {
    if (
      !Number.isInteger(row.index) ||
      row.index! < 0 ||
      row.index! >= expected ||
      sorted[row.index!]
    ) {
      throw new Error('Workers AI returned invalid or duplicate embedding indexes');
    }
    sorted[row.index!] = row;
  }
  return sorted.map((row, index) => {
    const vector = row.embedding;
    const dimension = getCloudflareEmbeddingDimension();
    if (!Array.isArray(vector) || vector.length !== dimension) {
      throw new Error(
        `Workers AI returned an invalid vector at index ${index}; expected ${dimension} dimensions`,
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
  const { endpoint, model, token } = apiConfig();
  for (let attempt = 0; attempt < 4; attempt++) {
    await waitForRequestSlot();
    let response: Response;
    try {
      response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ model, input: texts }),
      });
    } catch (error) {
      if (attempt === 3) throw error;
      await delay(Math.min(1_000 * 2 ** attempt, 15_000));
      continue;
    }

    if (response.status === 429) {
      let body = '';
      try {
        body = await response.text();
      } catch {
        // Retry-After remains available even if the body stream fails.
      }
      let payload: OpenAiEmbeddingResponse = {};
      try {
        payload = JSON.parse(body) as OpenAiEmbeddingResponse;
      } catch {
        // Retry-After remains authoritative even when a proxy returns plain text.
      }
      const { retryAt, code, message } = rateLimitRetryAt(response, payload);
      throw new CloudflareRateLimitError(message, retryAt, code);
    }

    let body: string;
    try {
      body = await response.text();
    } catch {
      body = '';
    }
    let payload: OpenAiEmbeddingResponse = {};
    try {
      payload = JSON.parse(body) as OpenAiEmbeddingResponse;
    } catch {
      if (response.ok) throw new Error(`Workers AI returned invalid JSON (${response.status})`);
    }
    if (response.ok) return parseVectors(payload, texts.length);

    const { message, code } = errorDetails(payload, body.trim().slice(0, 500));
    if (response.status >= 500 && attempt < 3) {
      await delay(Math.min(1_000 * 2 ** attempt, 15_000));
      continue;
    }
    throw new Error(
      `Embedding API ${model} failed (${response.status}${code ? `, ${code}` : ''}): ${message}`,
    );
  }
  throw new Error(`Embedding API ${model} request exhausted retries`);
};

/** UTF-8 bytes conservatively bound tokenizer output for chunk splitting. */
export const countCloudflareInputTokens = (text: string): number => Buffer.byteLength(text, 'utf8');
