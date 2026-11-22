export interface MarkdownTurn {
  readonly user?: string;
  readonly thinking?: string;
  readonly ai?: string;
  readonly createdAt?: string;
  readonly model?: string;
}

export interface ThreadMarkdownMetadata {
  readonly model?: string;
  readonly sourceFile?: string;
  readonly collection?: string;
}

export const collLabel = (collection: string): string => {
  if (collection === 'all' || collection === '__all__') return 'All collections';
  if (collection === 'chunks') return 'Chunks';
  if (collection === 'threadshelf_conversations') return 'ThreadShelf conversations';
  return collection;
};

export const fmtModel = (model: string | undefined): string => {
  if (!model) return '';
  const value = String(model).replace(/^models\//, '');
  if (/^[a-z]:[\\/]/i.test(value) || value.startsWith('/') || value.includes('\\')) {
    const pathParts = value.replace(/\\/g, '/').split('/');
    return pathParts[pathParts.length - 1]!.replace(/\.gguf$/i, '');
  }
  return value;
};

export const fmtDate = (value: string | undefined): string => {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return new Intl.DateTimeFormat(undefined, {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(date);
};

const turnRole = (turn: MarkdownTurn): 'user' | 'thinking' | 'ai' => {
  if (turn.user !== undefined) return 'user';
  if (turn.thinking !== undefined) return 'thinking';
  return 'ai';
};

const turnText = (turn: MarkdownTurn): string => turn.user ?? turn.thinking ?? turn.ai ?? '';

const MD_HEADING = {
  user: '### 🧑 User',
  thinking: '### 💭 Reasoning',
  ai: '### 🤖 Response',
};

/** Shared by the thread reader's .md download and the MCP Markdown tool. */
export const buildThreadMarkdown = (
  title: string,
  turns: readonly MarkdownTurn[],
  metadata?: ThreadMarkdownMetadata,
): string => {
  const lines: string[] = [`# ${title || 'Conversation'}`, ''];
  const facts: string[] = [];
  if (metadata?.model) facts.push(`**Model:** ${fmtModel(metadata.model)}`);
  facts.push(`**Turns:** ${turns.length}`);
  if (metadata?.collection) facts.push(`**Collection:** ${metadata.collection}`);
  if (metadata?.sourceFile) facts.push(`**Source:** \`${metadata.sourceFile}\``);
  if (facts.length) lines.push(facts.join('  ·  '), '', '---', '');

  for (const turn of turns) {
    lines.push(MD_HEADING[turnRole(turn)]);
    const date = fmtDate(turn.createdAt);
    if (date) lines.push(`*${date}*`, '');
    else lines.push('');
    lines.push(turnText(turn).trim(), '');
  }

  return (
    lines
      .join('\n')
      .replace(/\n{3,}/g, '\n\n')
      .trim() + '\n'
  );
};

const shortPath = (path: string): string => {
  if (!path) return '';
  const parts = path.replace(/\\/g, '/').split('/');
  return parts.length > 2 ? parts.slice(-2).join('/') : path;
};

const fallbackTitle = (sourceFile: string, turns: readonly MarkdownTurn[]): string => {
  const firstUser = turns.find((turn) => turn.user !== undefined);
  if (firstUser) {
    const title = firstUser.user!.trim();
    return title.length > 80 ? `${title.slice(0, 77)}…` : title || shortPath(sourceFile);
  }
  return shortPath(sourceFile) || 'Conversation';
};

/** Render an archived thread using the same format as the existing UI export. */
export const renderThreadMarkdown = (
  thread: {
    readonly sourceFile: string;
    readonly title?: string;
    readonly turns: readonly MarkdownTurn[];
  },
  collection = 'all',
): string => {
  const title = thread.title?.trim() || fallbackTitle(thread.sourceFile, thread.turns);
  const model = thread.turns.find((turn) => typeof turn.model === 'string')?.model;
  return buildThreadMarkdown(title, thread.turns, {
    model,
    sourceFile: thread.sourceFile,
    collection: collLabel(collection),
  });
};
