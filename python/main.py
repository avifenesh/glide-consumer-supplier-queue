import asyncio
from typing import List

from glide import (
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
    StreamGroupOptions,
    StreamReadGroupOptions,
)

from queue_logic import summarize_stream_entries, should_process_batch

THRESHOLD = 5
STREAM_KEY = b"tasks"
CONSUMER_GROUP = b"workers"
CONSUMER_NAME = b"worker-1"
BATCH_BLOCK_MS = 5000

producer_finished = False


# Helpers - Generate tasks
def generate_tasks(count: int) -> List[bytes]:
    return [f"task-{i}".encode() for i in range(count)]


# Helpers - Create Valkey client
async def create_client() -> GlideClusterClient:
    config = GlideClusterClientConfiguration([NodeAddress(host="127.0.0.1", port=6379)])
    return await GlideClusterClient.create(config)


async def ensure_consumer_group(client: GlideClusterClient) -> None:
    try:
        await client.xgroup_create(
            key=STREAM_KEY,
            group_name=CONSUMER_GROUP,
            group_id=b"$",
            options=StreamGroupOptions(make_stream=True),
        )
    except Exception as error:  # noqa: BLE001 - GLIDE raises generic exceptions
        message = str(error)
        if "BUSYGROUP" not in message:
            raise


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
        stream_length = await glide_consumer.xlen(STREAM_KEY)

        if not should_process_batch(
            stream_length=stream_length,
            threshold=THRESHOLD,
            producer_finished=producer_finished,
        ):
            if producer_finished and stream_length == 0:
                break

            await asyncio.sleep(0.01)
            continue

        desired_count = max(1, min(stream_length, THRESHOLD))

        tasks_result = await glide_consumer.xreadgroup(
            {STREAM_KEY: b">"},
            CONSUMER_GROUP,
            CONSUMER_NAME,
            StreamReadGroupOptions(count=desired_count, block_ms=BATCH_BLOCK_MS),
        )

        if not tasks_result:
            if producer_finished:
                remaining = await glide_consumer.xlen(STREAM_KEY)
                if remaining == 0:
                    break
            continue

        stream_entries = tasks_result.get(STREAM_KEY, {})
        summary = summarize_stream_entries(stream_entries, task_field=b"task")

        if summary["ack_ids"]:
            await glide_consumer.xack(STREAM_KEY, CONSUMER_GROUP, summary["ack_ids"])

        if summary["tasks"]:
            print(f"Processing batch of {len(summary['tasks'])} tasks", summary["tasks"])

    await glide_consumer.flushall()
    await glide_consumer.close()


async def main():
    glide_producer = await create_client()
    glide_consumer = await create_client()

    await ensure_consumer_group(glide_producer)

    await asyncio.gather(producer(glide_producer), consumer(glide_consumer))


if __name__ == "__main__":
    asyncio.run(main())
