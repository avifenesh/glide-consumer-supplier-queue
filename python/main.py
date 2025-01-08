import asyncio
from glide import (
    GlideClusterClient,
    NodeAddress,
    GlideClusterClientConfiguration,
    StreamReadGroupOptions,
    StreamGroupOptions,
)
from typing import List, Mapping, Optional, Sequence, Union

THRESHOLD = 5
STREAM_KEY = b"tasks"
CONSUMER_GROUP = b"workers"
CONSUMER_NAME = b"worker-1"

producer_finished = False


# Helpers - Generate tasks
def generate_tasks(count: int) -> List[bytes]:
    return [f"task-{i}".encode() for i in range(count)]


# Helpers - Create Valkey client
async def create_client() -> GlideClusterClient:
    config = GlideClusterClientConfiguration([NodeAddress(host="127.0.0.1", port=6379)])
    return await GlideClusterClient.create(config)


# Producer using xadd to add tasks to the stream
async def producer(glide_producer: GlideClusterClient):
    global producer_finished
    tasks_to_process = generate_tasks(1000)

    for task in tasks_to_process:
        await glide_producer.xadd(STREAM_KEY, [(b"task", task)])

    await glide_producer.close()
    producer_finished = True


# Consumer using xreadgroup to read tasks from the stream
async def consumer(glide_consumer: GlideClusterClient):
    while True:
        # Get stream length
        stream_length = await glide_consumer.xlen(STREAM_KEY)

        if stream_length >= THRESHOLD or producer_finished:
            # Get tasks from the stream
            tasks_result = await glide_consumer.xreadgroup(
                {STREAM_KEY: b">"},
                CONSUMER_GROUP,
                CONSUMER_NAME,
                StreamReadGroupOptions(count=THRESHOLD, block_ms=5000),
            )

            if not tasks_result:
                if producer_finished:
                    break
                continue

            stream_entries = tasks_result.get(STREAM_KEY, {})
            if not stream_entries:
                continue

            # Acknowledge tasks once pulled and remove them from the stream
            message_ids: List[Union[str, bytes]] = []
            batch_data = []
            for msg_id, entries in stream_entries.items():
                message_ids.append(msg_id)
                if entries:
                    batch_data.extend(entries)

            # Acknowledge messages
            if message_ids:
                await glide_consumer.xack(STREAM_KEY, CONSUMER_GROUP, message_ids)
                print("Processing batch:", batch_data)

        # Wait before checking again if threshold not met
        await asyncio.sleep(0.01)

    await glide_consumer.flushall()
    await glide_consumer.close()


async def main():
    glide_producer = await create_client()
    glide_consumer = await create_client()

    # Create the stream and consumer group
    await glide_producer.xgroup_create(
        key=STREAM_KEY,
        group_name=CONSUMER_GROUP,
        group_id="$",
        options=StreamGroupOptions(make_stream=True),
    )

    # Start producer and consumer
    await asyncio.gather(producer(glide_producer), consumer(glide_consumer))


if __name__ == "__main__":
    asyncio.run(main())
