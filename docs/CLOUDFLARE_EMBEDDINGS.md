# Cloudflare Workers AI embeddings

ThreadShelf uses BAAI BGE-M3 hosted by Workers AI (`@cf/baai/bge-m3`) by default.
Every indexed chunk and semantic search query is sent to Cloudflare. Keyword
search does not make an embedding request. Set
`THREADSHELF_EMBEDDING_PROVIDER=local` to use local
`Xenova/paraphrase-multilingual-MiniLM-L12-v2` instead.

The provider uses Cloudflare's OpenAI-compatible `/ai/v1/embeddings` endpoint
with the standard `{ model, input }` request and `{ data: [{ index, embedding }] }`
response. `THREADSHELF_EMBEDDING_BASE_URL` can point to another OpenAI-compatible
embeddings service, while model name, dimension, and input limit are configurable.

Cloudflare lists BGE-M3 as multilingual, with a 60,000-token context window,
zero data retention, and published pricing of $0.0118 per million input tokens.
Check [the current model page](https://developers.cloudflare.com/workers-ai/models/bge-m3/)
for current limits and pricing. BGE-M3 vectors have 1,024 dimensions in this
integration. They cannot be mixed with the existing 384-dimensional MiniLM
vectors, even though both models embed the same text.

## Configure

Set these server environment variables (for example in the ignored root `.env`):

```dotenv
THREADSHELF_EMBEDDING_PROVIDER=cloudflare
CLOUDFLARE_ACCOUNT_ID=your-account-id
CLOUDFLARE_API_TOKEN=your-workers-ai-token
```

Create an API token with permission to run Workers AI in the intended account.
Keep it private and out of source control. The application uses this token for
new imports and semantic queries. If you set the local provider, ThreadShelf
uses MiniLM and its matching local index.

`CLOUDFLARE_EMBED_BATCH_SIZE` controls the reindex command's request batch size
(default 64, maximum 128). Normal ingestion also uses the repository's bounded
`EMBED_BATCH_SIZE` and token budget settings. Requests are paced at no more than
1,200 per minute in one process, below Cloudflare's published 3,000
text-embedding requests per minute. A 429 response defers the stored index job until `Retry-After`, or until
the next UTC midnight when Cloudflare identifies daily free-allocation exhaustion.

Each LanceDB collection has an internal embedding profile recording its model,
dimension, method, and generation state. ThreadShelf checks this profile before
semantic search or writes. Older unprofiled indexes are checked against their
stored vector dimension; a mismatch stops the operation with a migration
message instead of mixing vector spaces. Rebuild targets remain marked as
`building` and cannot be searched until every collection and the archive copy
are complete.

## Build a separate BGE-M3 index

The migration streams LanceDB record batches, sends only each collection's
stored `document` text to Workers AI, writes replacement vectors to a new local
LanceDB database, and copies the authoritative `__threads` archive table after
the vector tables finish. It does not modify the source database.

Stop ThreadShelf and any importer using the source database before starting, so
the archive cannot change while its index is being copied. With
`CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN` configured, run:

```sh
npm run reindex:cloudflare
```

By default, the source is `LANCEDB_PATH` (or ThreadShelf's usual data path), and
the target is the source path with `-bge-m3` appended. Paths and request size can
be passed explicitly:

```sh
npm run reindex:cloudflare -- \
  --source /path/to/current-lancedb \
  --target /path/to/new-bge-m3-lancedb \
  --batch-size 64
```

Index work uses one ledger contract: collection, model, method and scope identify
each job; its checkpoint records progress, completion, attempts, retry time and
the last error. Normal indexing stores pending state with the archived thread
rows in LanceDB, so archive publication and recovery state stay together. A
rebuild writes the same checkpoint contract to a mode-0600 sidecar at
`<target>.reindex.json`, alongside the separate destination database.

LanceDB does not promise a stable order for an unqualified table scan, so the
migration builds a bounded external-sort manifest from stable row keys before
using a cursor. It checkpoints each committed, idempotent batch against that
manifest. An interrupted run resumes without skipping rows due to scan order.
The private manifest is kept beside the ledger so it can be reused after a
restart. On HTTP 429 the command persists Cloudflare's exact `Retry-After` time
before waiting; if the process stops, the next invocation waits until that time
before resuming. Keep the source unchanged and stop ThreadShelf until migration
completes. The command refuses unrelated target tables or a ledger whose
source/model identity no longer matches, and stops if the archive has a pending
index job.

ThreadShelf paces requests to 1,200 per minute. Cloudflare currently documents
3,000 text-embedding requests per minute; 429 codes distinguish temporary
capacity from daily free-allocation exhaustion. The latter schedules the retry
for 00:00 UTC. See [Cloudflare limits](https://developers.cloudflare.com/workers-ai/platform/limits/)
and [Workers AI errors](https://developers.cloudflare.com/workers-ai/platform/errors/).

After the command completes successfully, set the running server's configuration
to the new index and provider:

```dotenv
LANCEDB_PATH=/path/to/new-bge-m3-lancedb
THREADSHELF_EMBEDDING_PROVIDER=cloudflare
```

Keep the old database. Rollback is setting `LANCEDB_PATH` back to the old path
and `THREADSHELF_EMBEDDING_PROVIDER=local`. All processes using a given database
must use the same embedding profile. A cross-process per-collection lock
serialises archive publication and vector rebuilds, preventing a recovery worker
from replacing a newer index revision. Restart ThreadShelf after changing either
setting. New imports and semantic queries will then use BGE-M3; exact keyword
search remains local.

The migration uses the already stored chunk text, so it does not need the
original export files. It preserves collection rows and metadata. It does not
re-chunk text: existing chunks remain intact and are embedded as stored. New
chunks use ThreadShelf's conservative UTF-8 byte bound for Cloudflare's published
context limit. The model must return one 1,024-dimensional vector for every
input; non-rate-limit failures stop migration without advancing the checkpoint.
