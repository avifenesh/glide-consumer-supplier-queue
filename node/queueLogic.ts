import type { GlideString } from "@valkey/valkey-glide";

export interface ShouldProcessBatchOptions {
  streamLength: number;
  threshold: number;
  producerFinished: boolean;
}

export interface SummarizeOptions {
  taskField?: string;
}

export interface BatchMessage {
  ackId: string;
  tasks: string[];
}

export interface BatchSummary {
  ackIds: string[];
  tasks: string[];
  messages: BatchMessage[];
}

export type StreamEntryValues = readonly [GlideString, GlideString][];

const DEFAULT_TASK_FIELD = "task";

export function shouldProcessBatch({
  streamLength,
  threshold,
  producerFinished,
}: ShouldProcessBatchOptions): boolean {
  if (threshold > 0 && streamLength >= threshold) {
    return true;
  }

  if (producerFinished && streamLength > 0) {
    return true;
  }

  return false;
}

export function summarizeStreamEntries(
  entries: Record<string, StreamEntryValues | null>,
  options: SummarizeOptions = {}
): BatchSummary {
  const taskField = options.taskField ?? DEFAULT_TASK_FIELD;

  const ackIds = Object.keys(entries ?? {})
    .sort(compareStreamIds);

  const tasks: string[] = [];
  const messages: BatchMessage[] = [];

  for (const messageId of ackIds) {
    const kvPairs = entries[messageId];

    const messageTasks: string[] = [];

    if (kvPairs) {
      for (const [field, value] of kvPairs) {
        if (matchesTaskField(field, taskField)) {
          const task = asTaskString(value);
          messageTasks.push(task);
          tasks.push(task);
        }
      }
    }

    messages.push({ ackId: messageId, tasks: messageTasks });
  }

  return { ackIds, tasks, messages };
}

function matchesTaskField(field: GlideString, taskField: string): boolean {
  if (Buffer.isBuffer(field)) {
    return field.toString() === taskField;
  }
  return field === taskField;
}

function asTaskString(value: GlideString): string {
  return Buffer.isBuffer(value) ? value.toString() : (value as string);
}

function compareStreamIds(a: string, b: string): number {
  const [timeA, seqA] = parseStreamId(a);
  const [timeB, seqB] = parseStreamId(b);

  if (timeA === null || timeB === null) {
    return a.localeCompare(b);
  }

  if (timeA !== timeB) {
    return timeA - timeB;
  }

  if (seqA === null || seqB === null) {
    return a.localeCompare(b);
  }

  return seqA - seqB;
}

function parseStreamId(id: string): [number | null, number | null] {
  const parts = id.split("-");
  if (parts.length !== 2) {
    return [null, null];
  }

  const [timePart, sequencePart] = parts;

  const time = Number(timePart);
  const sequence = Number(sequencePart);

  if (!Number.isFinite(time) || !Number.isFinite(sequence)) {
    return [null, null];
  }

  return [time, sequence];
}
