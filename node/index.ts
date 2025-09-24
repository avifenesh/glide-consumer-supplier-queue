//*
// This code sets up a Valkey stream–based queue with a threshold for batch processing.
// The producer adds tasks to the stream (using `XADD`).
// The consumer check the stream length (using `XLEN`) and reads tasks from the stream via a consumer group (using `XREADGROUP`).
// Once the threshold is reached, tasks are processed in a single batch and acknowledged (using `XACK`) to remove them from the stream.
// The code will be split into two parts: producer and consumer.
// The producer will add tasks to the stream.
// The consumer will read tasks from the stream and process them in batches.
// The code is implemented using valkey-glide, a multi lingual client for Valkey.
//*

import { GlideClusterClient } from "@valkey/valkey-glide";
import { summarizeStreamEntries, shouldProcessBatch } from "./queueLogic.js";

const THRESHOLD = 5;
const STREAM_KEY = "tasks";
const CONSUMER_GROUP = "workers";
const CONSUMER_NAME = "worker-1";
const BATCH_BLOCK_MS = 5000;

let producerFinished = false;

function wait(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ** Helpers **

// Helpers - Generate tasks
function generateTasks(count: number): string[] {
  return Array.from({ length: count }, (_, i) => `task-${i}`);
}

// Helpers - Create Valkey client
async function createClient() {
  return await GlideClusterClient.createClient({
    addresses: [
      { host: "127.0.0.1", port: 6379 },
    ],
  });
}

async function ensureConsumerGroup(client: GlideClusterClient) {
  try {
    await client.xgroupCreate(STREAM_KEY, CONSUMER_GROUP, "$", {
      mkStream: true,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (!message.includes("BUSYGROUP")) {
      throw error;
    }
  }
}

// ** Main **

// Producer using xadd to add tasks to the stream
async function producer(glideProducer: GlideClusterClient) {
  const tasksToProcess = generateTasks(1000);

  while (tasksToProcess.length > 0) {
    await glideProducer.xadd(STREAM_KEY, [
      ["task", tasksToProcess.shift()!],
    ]);
  }

  await glideProducer.close();
  producerFinished = true;
}

// Consumer using xreadgroup to read tasks from the stream
// and xack to acknowledge them once processed and remove them from the stream
async function consumer(glideConsumer: GlideClusterClient) {
  while (true) {
    const streamLength = await glideConsumer.xlen(STREAM_KEY);

    if (!shouldProcessBatch({ streamLength, threshold: THRESHOLD, producerFinished })) {
      if (producerFinished && streamLength === 0) {
        break;
      }

      await wait(10);
      continue;
    }

    const desiredCount = Math.max(1, Math.min(streamLength, THRESHOLD));

    const tasksResult = await glideConsumer.xreadgroup(
      CONSUMER_GROUP,
      CONSUMER_NAME,
      { [STREAM_KEY]: ">" },
      { count: desiredCount, block: BATCH_BLOCK_MS },
    );

    if (!tasksResult?.length) {
      if (producerFinished) {
        const remaining = await glideConsumer.xlen(STREAM_KEY);
        if (remaining === 0) {
          break;
        }
      }
      continue;
    }

    const streamData = tasksResult.find(stream => stream.key === STREAM_KEY);
    if (!streamData) {
      continue;
    }

    const summary = summarizeStreamEntries(streamData.value, { taskField: "task" });

    if (summary.ackIds.length > 0) {
      await glideConsumer.xack(STREAM_KEY, CONSUMER_GROUP, summary.ackIds);
    }

    if (summary.tasks.length > 0) {
      console.log(`Processing batch of ${summary.tasks.length} tasks`, summary.tasks);
    }
  }

  await glideConsumer.flushall();
  await glideConsumer.close();
}

async function main() {
  const glideProducer = await createClient();
  const glideConsumer = await createClient();

  await ensureConsumerGroup(glideProducer);

  await Promise.all([producer(glideProducer), consumer(glideConsumer)]);
}

await main();
