"""Proof-over-prose for LOAD-10's pre-pod-launch integrity gate (08-02-PLAN.md Task 2).

Every check path -- extension, empty-file, two-HEAD stability (D-21), real
GET+hash (D-22) -- and the D-20 sentinel-hash rejection write are exercised
here with `S3Hook`/`psycopg` fully mocked; nothing in this module performs
real network I/O or a real 5-second `time.sleep`.

Calling convention: `@task`-decorated functions build an XCom-template
string when invoked directly outside a DAG context (confirmed empirically
against the pinned `airflow.sdk`); `.function(...)` is the documented way to
invoke the raw, undecorated Python callable for a unit test, matching how
`tracing_kpo`'s own test module bypasses `DagBag`/`dag.test()` entirely for
tests that do not need a real DAG.
"""

from __future__ import annotations

import hashlib
from typing import Any, Self
from unittest.mock import MagicMock

import pytest

from _common import integrity_gate as gate

_BUCKET = "raw"
_KEY = "customers/2026-08-17.csv"
_DATASET = "customers"


class _FakeCursor:
    """Captures every `execute(sql, params)` call for later assertion."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.calls.append((sql, params))

    def fetchone(self) -> tuple[int]:
        # Only the dataset-upsert's RETURNING dataset_id is ever fetched by
        # _reject_file -- a fixed, arbitrary dataset_id is fine for every test.
        return (1,)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc_info: object) -> bool:
        return False


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc_info: object) -> bool:
        return False


@pytest.fixture
def fake_cursor(monkeypatch: pytest.MonkeyPatch) -> _FakeCursor:
    """Patch `psycopg.connect`/`BaseHook.get_connection` so `_reject_file` never
    touches a real database or a real Airflow Connection.
    """
    cursor = _FakeCursor()
    monkeypatch.setattr(gate.psycopg, "connect", lambda _dsn: _FakeConnection(cursor))
    fake_connection = MagicMock()
    fake_connection.get_uri.return_value = "postgresql://fake-user:fake-pass@fake-host/fake-db"
    monkeypatch.setattr(gate.BaseHook, "get_connection", lambda _conn_id: fake_connection)
    return cursor


def _make_s3_client(
    *,
    head_object_responses: list[dict[str, Any]],
    get_object_chunks: list[bytes] | None = None,
    get_object_side_effect: Exception | None = None,
) -> MagicMock:
    client = MagicMock()
    client.head_object.side_effect = head_object_responses
    if get_object_side_effect is not None:
        client.get_object.side_effect = get_object_side_effect
    elif get_object_chunks is not None:
        body = MagicMock()
        body.read.side_effect = [*get_object_chunks, b""]
        client.get_object.return_value = {"Body": body}
    return client


def _patch_s3_hook(monkeypatch: pytest.MonkeyPatch, client: MagicMock) -> MagicMock:
    hook_cls = MagicMock()
    hook_cls.return_value.get_conn.return_value = client
    monkeypatch.setattr(gate, "S3Hook", hook_cls)
    return hook_cls


# --- Test 1: happy path -----------------------------------------------------


def test_happy_path_returns_dict_and_never_rejects(monkeypatch: pytest.MonkeyPatch) -> None:
    head_response = {"ContentLength": 42, "ETag": '"abc123"'}
    client = _make_s3_client(
        head_object_responses=[head_response, head_response],
        get_object_chunks=[b"col_a,col_b\n1,2\n"],
    )
    _patch_s3_hook(monkeypatch, client)
    monkeypatch.setattr(gate.time, "sleep", MagicMock())
    reject_mock = MagicMock()
    monkeypatch.setattr(gate, "_reject_file", reject_mock)

    result = gate.integrity_gate.function(bucket=_BUCKET, key=_KEY, dataset_name=_DATASET)

    expected_hex = hashlib.sha256(b"col_a,col_b\n1,2\n").hexdigest()
    assert result == {
        "content_length": 42,
        "etag": '"abc123"',
        "content_sha256_hex": expected_hex,
    }
    reject_mock.assert_not_called()
    assert client.head_object.call_count == 2


# --- Test 2: extension -------------------------------------------------------


def test_wrong_extension_rejects_without_any_s3_call(monkeypatch: pytest.MonkeyPatch) -> None:
    hook_cls = MagicMock()
    monkeypatch.setattr(gate, "S3Hook", hook_cls)
    reject_mock = MagicMock()
    monkeypatch.setattr(gate, "_reject_file", reject_mock)

    with pytest.raises(gate.AirflowFailException):
        gate.integrity_gate.function(
            bucket=_BUCKET, key="customers/2026-08-17.txt", dataset_name=_DATASET
        )

    hook_cls.assert_not_called()
    reject_mock.assert_called_once()
    assert reject_mock.call_args.kwargs["content_sha256"] is None


# --- Test 3: empty file -------------------------------------------------------


def test_empty_file_rejects_with_real_empty_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_s3_client(head_object_responses=[{"ContentLength": 0, "ETag": '"empty"'}])
    _patch_s3_hook(monkeypatch, client)
    sleep_mock = MagicMock()
    monkeypatch.setattr(gate.time, "sleep", sleep_mock)
    reject_mock = MagicMock()
    monkeypatch.setattr(gate, "_reject_file", reject_mock)

    with pytest.raises(gate.AirflowFailException):
        gate.integrity_gate.function(bucket=_BUCKET, key=_KEY, dataset_name=_DATASET)

    sleep_mock.assert_not_called()
    assert client.head_object.call_count == 1
    reject_mock.assert_called_once()
    assert reject_mock.call_args.kwargs["content_sha256"] == hashlib.sha256(b"").digest()
    assert reject_mock.call_args.kwargs["size_bytes"] == 0


# --- Test 4: instability (D-21) ----------------------------------------------


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ({"ContentLength": 100, "ETag": '"a"'}, {"ContentLength": 200, "ETag": '"a"'}),
        ({"ContentLength": 100, "ETag": '"a"'}, {"ContentLength": 100, "ETag": '"b"'}),
    ],
)
def test_unstable_object_rejects_before_any_get(
    monkeypatch: pytest.MonkeyPatch, first: dict[str, Any], second: dict[str, Any]
) -> None:
    client = _make_s3_client(head_object_responses=[first, second])
    _patch_s3_hook(monkeypatch, client)
    monkeypatch.setattr(gate.time, "sleep", MagicMock())
    reject_mock = MagicMock()
    monkeypatch.setattr(gate, "_reject_file", reject_mock)

    with pytest.raises(gate.AirflowFailException):
        gate.integrity_gate.function(bucket=_BUCKET, key=_KEY, dataset_name=_DATASET)

    client.get_object.assert_not_called()
    reject_mock.assert_called_once()
    assert reject_mock.call_args.kwargs["content_sha256"] is None


# --- Test 5: unreadable (D-22) ------------------------------------------------


def test_unreadable_object_rejects_with_none_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    head_response = {"ContentLength": 50, "ETag": '"stable"'}
    client = _make_s3_client(
        head_object_responses=[head_response, head_response],
        get_object_side_effect=OSError("connection reset"),
    )
    _patch_s3_hook(monkeypatch, client)
    monkeypatch.setattr(gate.time, "sleep", MagicMock())
    reject_mock = MagicMock()
    monkeypatch.setattr(gate, "_reject_file", reject_mock)

    with pytest.raises(gate.AirflowFailException):
        gate.integrity_gate.function(bucket=_BUCKET, key=_KEY, dataset_name=_DATASET)

    reject_mock.assert_called_once()
    assert reject_mock.call_args.kwargs["content_sha256"] is None
    assert reject_mock.call_args.kwargs["size_bytes"] == 50


# --- Test 6: parameterized SQL, never string-interpolated --------------------


def test_reject_file_sql_uses_parameters_never_interpolation(fake_cursor: _FakeCursor) -> None:
    reason = "the-real-reason-42"
    gate._reject_file(  # noqa: SLF001 -- exercising the private function T-08-04's mitigation covers
        bucket=_BUCKET,
        key=_KEY,
        dataset_name=_DATASET,
        reason=reason,
        content_sha256=None,
        size_bytes=123,
    )

    assert len(fake_cursor.calls) == 2
    dataset_sql, dataset_params = fake_cursor.calls[0]
    files_sql, files_params = fake_cursor.calls[1]

    # The dataset name and object_uri/reason must appear only as bound
    # parameters, never interpolated into the SQL text itself.
    assert _DATASET not in dataset_sql
    assert dataset_params == (_DATASET,)

    assert _KEY not in files_sql
    assert reason not in files_sql
    object_uri = f"s3://{_BUCKET}/{_KEY}"
    assert object_uri in files_params
    assert any(isinstance(p, bytes) for p in files_params), "resolved hash must be bound as bytes"


# --- Test 7: sentinel-hash determinism / distinctness -------------------------


def test_sentinel_hash_deterministic_for_identical_triple_distinct_for_different_reason(
    fake_cursor: _FakeCursor,
) -> None:
    def _resolved_hash_from_last_call() -> bytes:
        _files_sql, files_params = fake_cursor.calls[-1]
        assert files_params is not None
        return files_params[2]

    gate._reject_file(  # noqa: SLF001 -- exercising the private function's sentinel-hash resolution
        bucket=_BUCKET,
        key=_KEY,
        dataset_name=_DATASET,
        reason="reason-a",
        content_sha256=None,
        size_bytes=None,
    )
    hash_a1 = _resolved_hash_from_last_call()

    gate._reject_file(  # noqa: SLF001 -- see above
        bucket=_BUCKET,
        key=_KEY,
        dataset_name=_DATASET,
        reason="reason-a",
        content_sha256=None,
        size_bytes=None,
    )
    hash_a2 = _resolved_hash_from_last_call()

    gate._reject_file(  # noqa: SLF001 -- see above
        bucket=_BUCKET,
        key=_KEY,
        dataset_name=_DATASET,
        reason="reason-b",
        content_sha256=None,
        size_bytes=None,
    )
    hash_b = _resolved_hash_from_last_call()

    assert hash_a1 == hash_a2, "identical (bucket, key, reason) must resolve to the same hash"
    assert hash_a1 != hash_b, "a different reason for the same object_uri must be a distinct hash"


# --- Test 8: list_matched_keys -------------------------------------------------


def test_list_matched_keys_wraps_s3_hook_list_keys_with_wildcard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook_cls = MagicMock()
    hook_instance = hook_cls.return_value
    hook_instance.list_keys.return_value = ["customers/a.csv", "customers/b.csv"]
    monkeypatch.setattr(gate, "S3Hook", hook_cls)

    result = gate.list_matched_keys.function(bucket=_BUCKET, prefix="customers/")

    assert result == ["customers/a.csv", "customers/b.csv"]
    hook_instance.list_keys.assert_called_once_with(
        bucket_name=_BUCKET, prefix="customers/", apply_wildcard=True
    )
