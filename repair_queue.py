"""Durable work queue for aggregate corrections after delayed comment sync.

Each downstream repository owns one or more ``consumer`` names.  A producer
only UPSERTs work; a consumer deletes its own row after a successful rebuild.
That makes retries idempotent and avoids relying on cross-repository workflow
timing.
"""
from __future__ import annotations

import time


TABLE_SQL = """
CREATE TABLE IF NOT EXISTS aggregate_repair_queue (
  consumer TEXT NOT NULL,
  work_key TEXT NOT NULL,
  detected_at INTEGER NOT NULL,
  generation INTEGER NOT NULL DEFAULT 1,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  PRIMARY KEY (consumer, work_key)
)
"""

UPSERT_SQL = """
INSERT INTO aggregate_repair_queue
  (consumer, work_key, detected_at, generation, attempts, last_error)
VALUES (?, ?, ?, 1, 0, NULL)
ON CONFLICT(consumer, work_key) DO UPDATE SET
  detected_at = excluded.detected_at,
  generation = aggregate_repair_queue.generation + 1,
  attempts = 0,
  last_error = NULL
"""

_BATCH = 100


def ensure_table(turso) -> None:
    turso.execute(TABLE_SQL)


def enqueue(turso, work: dict[str, set[str]], detected_at: int | None = None) -> int:
    """Idempotently enqueue ``{consumer: {work_key, ...}}`` and verify writes."""
    ensure_table(turso)
    stamp = int(time.time()) if detected_at is None else int(detected_at)
    items = sorted(
        (consumer, str(key))
        for consumer, keys in work.items()
        for key in keys
        if consumer and key is not None and str(key)
    )
    for offset in range(0, len(items), _BATCH):
        chunk = items[offset:offset + _BATCH]
        turso.batch([
            {"sql": UPSERT_SQL, "args": [consumer, key, stamp]}
            for consumer, key in chunk
        ])
        # TursoClient.batch intentionally does not expose per-statement errors.
        # Verify every chunk before reporting it as durable.
        for consumer in sorted({item[0] for item in chunk}):
            expected = {key for c, key in chunk if c == consumer}
            marks = ",".join("?" for _ in expected)
            rows = turso.query(
                "SELECT work_key FROM aggregate_repair_queue "
                f"WHERE consumer = ? AND work_key IN ({marks})",
                [consumer, *sorted(expected)],
            )
            got = {str(row["work_key"]) for row in rows}
            if got != expected:
                raise RuntimeError(
                    f"aggregate repair queue verification failed for {consumer}: "
                    f"missing {sorted(expected - got)}"
                )
    return len(items)


def load(turso, consumer: str, limit: int = 1000) -> dict[str, int]:
    ensure_table(turso)
    rows = turso.query(
        "SELECT work_key,generation FROM aggregate_repair_queue WHERE consumer=? "
        "ORDER BY detected_at,work_key LIMIT ?", [consumer, limit],
    )
    return {str(row["work_key"]): int(row["generation"]) for row in rows}


def complete(turso, consumer: str, claims: dict[str, int]) -> None:
    """Delete only the generations actually rebuilt by this consumer run."""
    for key, generation in claims.items():
        turso.execute(
            "DELETE FROM aggregate_repair_queue "
            "WHERE consumer=? AND work_key=? AND generation=?",
            [consumer, str(key), int(generation)],
        )


def fail(turso, consumer: str, keys, error: Exception) -> None:
    message = str(error)[:1000]
    for key in keys:
        turso.execute(
            "UPDATE aggregate_repair_queue SET attempts=attempts+1,last_error=? "
            "WHERE consumer=? AND work_key=?",
            [message, consumer, str(key)],
        )
