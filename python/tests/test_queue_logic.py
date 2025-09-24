import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))

from queue_logic import summarize_stream_entries, should_process_batch


def test_should_process_batch_threshold_not_met():
    assert not should_process_batch(stream_length=2, threshold=5, producer_finished=False)


def test_should_process_batch_threshold_met():
    assert should_process_batch(stream_length=5, threshold=5, producer_finished=False)


def test_should_process_batch_producer_finished_with_remaining_items():
    assert should_process_batch(stream_length=1, threshold=5, producer_finished=True)


def test_should_process_batch_nothing_left():
    assert not should_process_batch(stream_length=0, threshold=5, producer_finished=True)


def test_summarize_stream_entries_collects_tasks_and_ack_ids():
    summary = summarize_stream_entries(
        {
            "2-0": [(b"task", b"task-2"), (b"meta", b"ignored")],
            "1-0": [(b"task", b"task-1")],
        },
        task_field=b"task",
    )

    assert summary["ack_ids"] == ["1-0", "2-0"]
    assert summary["tasks"] == [b"task-1", b"task-2"]


def test_summarize_stream_entries_handles_missing_task_field():
    summary = summarize_stream_entries(
        {
            "3-0": [(b"note", b"value")],
            "4-0": None,
        },
        task_field=b"task",
    )

    assert summary["ack_ids"] == ["3-0", "4-0"]
    assert summary["tasks"] == []
