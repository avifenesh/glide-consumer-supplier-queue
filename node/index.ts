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

const THRESHOLD = 5
const STREAM_KEY = "tasks"
const CONSUMER_GROUP = "workers"
const CONSUMER_NAME = "worker-1"

let producerFinished = false;

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

// ** Main **

// Producer using xadd to add tasks to the stream
async function producer(glideProducer: GlideClusterClient) {
    const tasksToProcess = generateTasks(1000);

    while (tasksToProcess.length > 0) {
        await glideProducer.xadd(STREAM_KEY, [
            ["task", tasksToProcess.shift()!]
        ]);
    }

    glideProducer.close();
    producerFinished = true;
}


// Consumer using xreadgroup to read tasks from the stream
// and xack to acknowledge them once processed and remove them from the stream
async function consumer(glideConsumer: GlideClusterClient) {
    while (true) {
        // Get stream length
        const streamLength = await glideConsumer.xlen(STREAM_KEY);

        if (streamLength >= THRESHOLD || producerFinished) {
            // Get tasks from the stream
            const tasksResult = await glideConsumer.xreadgroup(
                CONSUMER_GROUP,
                CONSUMER_NAME,
                { [STREAM_KEY]: ">" },
                { count: THRESHOLD, block: 5000 }
            );

            if (!tasksResult?.length) {
                if (producerFinished) break;
                continue;
            }

            const streamData = tasksResult.find(stream => stream.key === STREAM_KEY);
            if (!streamData) continue;

            // Acknowledge tasks once pulled and remove them from the stream
            const messageIds = Object.keys(streamData.value);
            await glideConsumer.xack(STREAM_KEY, CONSUMER_GROUP, messageIds);

            console.log("Processing batch:", streamData.value);
        }

        // Wait before checking again if threshold not met
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    await glideConsumer.flushall();
    glideConsumer.close();
}

async function main() {
    const glideProducer = await createClient();
    const glideConsumer = await createClient();
    // Create the stream and consumer group
    await glideProducer.xgroupCreate(STREAM_KEY, CONSUMER_GROUP, "$", {
        mkStream: true
    });
    // Start producer and consumer
    Promise.all([producer(glideProducer), consumer(glideConsumer)]);
}

await main();
