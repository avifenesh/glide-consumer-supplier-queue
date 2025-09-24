from __future__ import annotations

import sys
from typing import Dict, List, Mapping, Optional, Sequence, Tuple, Union, TypedDict

StreamFieldValue = Union[str, bytes]
StreamEntry = Sequence[StreamFieldValue]
StreamEntries = Mapping[Union[str, bytes], Optional[Sequence[StreamEntry]]]


class BatchMessage(TypedDict):
    ack_id: str
    tasks: List[bytes]


class BatchSummary(TypedDict):
    ack_ids: List[str]
    tasks: List[bytes]
    messages: List[BatchMessage]


def should_process_batch(*, stream_length: int, threshold: int, producer_finished: bool) -> bool:
    if threshold > 0 and stream_length >= threshold:
        return True

    if producer_finished and stream_length > 0:
        return True

    return False


def summarize_stream_entries(
    entries: StreamEntries,
    *,
    task_field: Union[str, bytes] = "task",
) -> BatchSummary:
    normalized_entries: Dict[str, Optional[Sequence[StreamEntry]]] = {
        _normalize_stream_id(message_id): value for message_id, value in entries.items()
    }

    ack_ids = sorted(normalized_entries.keys(), key=_stream_id_sort_key)

    tasks: List[bytes] = []
    messages: List[BatchMessage] = []
    expected_field = _ensure_bytes(task_field)

    for message_id in ack_ids:
        message_values = normalized_entries[message_id]
        message_tasks: List[bytes] = []

        if message_values:
            for pair in message_values:
                if len(pair) < 2:
                    continue

                field_raw, value_raw = pair[0], pair[1]
                field = _ensure_bytes(field_raw)

                if field == expected_field:
                    task_value = _ensure_bytes(value_raw)
                    message_tasks.append(task_value)
                    tasks.append(task_value)

        messages.append({"ack_id": message_id, "tasks": message_tasks})

    return {"ack_ids": ack_ids, "tasks": tasks, "messages": messages}


def _ensure_bytes(value: StreamFieldValue) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def _normalize_stream_id(stream_id: Union[str, bytes]) -> str:
    if isinstance(stream_id, bytes):
        return stream_id.decode()
    return str(stream_id)


def _stream_id_sort_key(stream_id: str) -> Tuple[int, int, str]:
    try:
        time_part, sequence_part = stream_id.split("-", 1)
        return int(time_part), int(sequence_part), stream_id
    except (ValueError, TypeError):
        return sys.maxsize, sys.maxsize, stream_id
