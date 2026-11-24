import { getOpenAIConversationPath } from './parser.js';

type ObjectValue = Record<string, unknown>;
export interface ChatGptAttachmentReference {
  readonly id: string;
  readonly messageId: string;
  readonly source: 'image' | 'attachment';
  readonly name?: string;
  readonly mimeType?: string;
}
const object = (value: unknown): ObjectValue | undefined =>
  value && typeof value === 'object' && !Array.isArray(value) ? (value as ObjectValue) : undefined;
const fileId = (value: unknown): string | undefined => {
  if (typeof value !== 'string') return undefined;
  const match = /^(?:file-service:\/\/)?(file-[a-zA-Z0-9_-]{1,240})$/.exec(value);
  return match?.[1];
};

/** Extract references on the same current conversation branch used by the parser. */
export const chatGptAttachmentReferences = (
  data: unknown,
  conversationKey: string,
): ChatGptAttachmentReference[] => {
  if (!conversationKey.startsWith('openai:'))
    throw new Error('A ChatGPT conversation key is required');
  const id = conversationKey.slice('openai:'.length);
  const candidates = (Array.isArray(data) ? data : [data])
    .map(object)
    .filter((item) => item !== undefined);
  const matches = candidates.filter((item) => String(item.id ?? item.conversation_id ?? '') === id);
  if (matches.length !== 1) throw new Error('ChatGPT conversation not found uniquely in source');
  const found: ChatGptAttachmentReference[] = [];
  const seen = new Set<string>();
  const add = (ref: ChatGptAttachmentReference) => {
    const key = JSON.stringify([ref.messageId, ref.id, ref.source]);
    if (!seen.has(key)) {
      seen.add(key);
      found.push(ref);
    }
  };
  for (const node of getOpenAIConversationPath(matches[0]!)) {
    const message = object(node.message);
    if (!message) continue;
    const messageId = typeof message.id === 'string' ? message.id : String(node.id ?? '');
    const parts = object(message.content)?.parts;
    if (Array.isArray(parts))
      for (const part of parts) {
        const id = fileId(object(part)?.asset_pointer);
        if (id) add({ id, messageId, source: 'image' });
      }
    const attachments = object(message.metadata)?.attachments;
    if (Array.isArray(attachments))
      for (const entry of attachments) {
        const item = object(entry);
        const id = fileId(item?.id);
        if (id)
          add({
            id,
            messageId,
            source: 'attachment',
            ...(typeof item?.name === 'string' ? { name: item.name } : {}),
            ...(typeof item?.mime_type === 'string' ? { mimeType: item.mime_type } : {}),
          });
      }
  }
  return found;
};
