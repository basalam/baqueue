"""Tests for JobPayload serialization and class-path helpers."""

from __future__ import annotations

import json

import pytest

from baqueue.serializer import (
    JobPayload,
    _now_ts,
    get_class_path,
    resolve_job_class,
)


class TestJobPayload:
    def test_defaults(self):
        p = JobPayload()
        assert isinstance(p.id, str) and len(p.id) == 32
        assert p.job_class == ""
        assert p.data == {}
        assert p.queue == "default"
        assert p.attempts == 0
        assert p.max_attempts == 3
        assert p.backoff == "exponential"
        assert p.timeout == 60
        assert p.tags == []
        assert p.batch_id is None
        assert p.delay_until is None
        assert p.status == "pending"
        assert p.error is None
        # timestamps populated
        assert p.created_at > 0
        assert p.updated_at == p.created_at
        assert p.started_at is None
        assert p.completed_at is None
        assert p.failed_at is None

    def test_explicit_fields(self):
        p = JobPayload(
            id="abc123",
            job_class="myapp.SendEmail",
            data={"to": "x@y.com"},
            queue="emails",
            attempts=2,
            max_attempts=5,
            backoff=[1, 2, 3],
            timeout=30,
            tags=["email"],
            batch_id="batch-1",
            delay_until=1234567890.0,
            status="processing",
            error="boom",
        )
        assert p.id == "abc123"
        assert p.job_class == "myapp.SendEmail"
        assert p.data == {"to": "x@y.com"}
        assert p.backoff == [1, 2, 3]
        assert p.delay_until == 1234567890.0
        assert p.status == "processing"
        assert p.error == "boom"

    def test_to_dict_roundtrip(self):
        p = JobPayload(
            id="r1",
            job_class="x.Y",
            data={"k": [1, 2]},
            queue="q",
            tags=["a", "b"],
            delay_until=42.0,
            started_at=10.0,
        )
        d = p.to_dict()
        # All __slots__ keys must appear in to_dict
        for key in JobPayload.__slots__:
            assert key in d, f"missing {key} in to_dict"
        restored = JobPayload.from_dict(d)
        assert restored.id == p.id
        assert restored.data == p.data
        assert restored.tags == p.tags
        assert restored.delay_until == p.delay_until
        assert restored.started_at == p.started_at

    def test_json_roundtrip(self):
        p = JobPayload(
            job_class="x.Y",
            data={"n": 1, "nested": {"a": [1, 2, 3]}},
            tags=["t1"],
            delay_until=1700000000.5,
        )
        raw = p.to_json()
        # Must be valid JSON
        json.loads(raw)
        restored = JobPayload.from_json(raw)
        assert restored.data == p.data
        assert restored.tags == p.tags
        assert restored.delay_until == p.delay_until

    def test_unique_ids(self):
        ids = {JobPayload().id for _ in range(100)}
        assert len(ids) == 100, "JobPayload IDs must be unique"


class TestJobPayloadHistory:
    def test_history_defaults_to_empty_list(self):
        p = JobPayload()
        assert p.history == []
        # distinct instances must not share the same list object
        assert JobPayload().history is not p.history

    def test_history_roundtrips_through_dict_and_json(self):
        entry = {
            "attempt": 1, "started_at": 1.0, "finished_at": 2.0,
            "status": "failed", "error": "boom", "will_retry": True,
            "next_retry_at": 7.0,
        }
        p = JobPayload(job_class="x.Y", history=[entry])
        assert p.to_dict()["history"] == [entry]
        restored = JobPayload.from_json(p.to_json())
        assert restored.history == [entry]

    def test_from_dict_without_history_is_backward_compatible(self):
        # A payload serialized by an older version has no "history" key.
        legacy = JobPayload(job_class="x.Y").to_dict()
        legacy.pop("history")
        restored = JobPayload.from_dict(legacy)
        assert restored.history == []

    def test_to_dict_can_exclude_history(self):
        p = JobPayload(job_class="x.Y", history=[{"attempt": 1}])
        assert "history" not in p.to_dict(include_history=False)
        assert "history" in p.to_dict()


class TestClassPath:
    def test_get_class_path(self):
        path = get_class_path(JobPayload)
        assert path == "baqueue.serializer.JobPayload"

    def test_resolve_job_class_roundtrip(self):
        path = get_class_path(JobPayload)
        cls = resolve_job_class(path)
        assert cls is JobPayload

    def test_resolve_invalid_module(self):
        with pytest.raises(ModuleNotFoundError):
            resolve_job_class("nope.NoModule.X")

    def test_resolve_invalid_attribute(self):
        with pytest.raises(AttributeError):
            resolve_job_class("baqueue.serializer.DoesNotExist")


class TestNowTs:
    def test_returns_float_in_seconds(self):
        ts = _now_ts()
        assert isinstance(ts, float)
        # Anything after year 2020 is "now" enough for sanity
        assert ts > 1_577_836_800.0

    def test_monotonic_within_call_window(self):
        a = _now_ts()
        b = _now_ts()
        assert b >= a
