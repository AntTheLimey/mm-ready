"""Collects pg_stat_statements snapshots over a time window."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field


@dataclass
class StatementSnapshot:
    query: str
    calls: int
    total_exec_time: float
    rows: int
    queryid: int | None = None


@dataclass
class StatsDelta:
    """Difference between two snapshots — represents activity during observation."""

    new_queries: list[StatementSnapshot] = field(default_factory=list)
    changed_queries: list[dict] = field(default_factory=list)  # {query, delta_calls, delta_time}
    duration_seconds: float = 0.0


def is_available(conn) -> bool:
    """Check if pg_stat_statements is queryable."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_stat_statements LIMIT 1;")
            return True
    except Exception:
        return False


def take_snapshot(conn) -> dict[str, StatementSnapshot]:
    """Take a snapshot of pg_stat_statements, keyed by queryid or query text."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT queryid, query, calls, total_exec_time, rows
            FROM pg_stat_statements
            ORDER BY calls DESC;
        """)
        rows = cur.fetchall()

    snapshots = {}
    for queryid, query, calls, total_time, row_count in rows:
        key = str(queryid) if queryid else query[:200]
        snapshots[key] = StatementSnapshot(
            query=query,
            calls=calls,
            total_exec_time=total_time,
            rows=row_count,
            queryid=queryid,
        )
    return snapshots


def collect_over_duration(conn, duration: int, verbose: bool = False) -> StatsDelta:
    """Collect two snapshots separated by `duration` seconds and compute the delta."""
    if verbose:
        print("  Taking initial pg_stat_statements snapshot...", file=sys.stderr)

    snap_before = take_snapshot(conn)
    before_time = time.time()

    if verbose:
        print(f"  Waiting {duration} seconds for observation window...", file=sys.stderr)

    # Wait, checking periodically that connection is alive
    elapsed = 0
    interval = min(60, duration)
    while elapsed < duration:
        sleep_time = min(interval, duration - elapsed)
        time.sleep(sleep_time)
        elapsed = int(time.time() - before_time)
        if verbose and elapsed < duration:
            remaining = duration - elapsed
            print(f"    {remaining}s remaining...", file=sys.stderr)

    if verbose:
        print("  Taking final pg_stat_statements snapshot...", file=sys.stderr)

    snap_after = take_snapshot(conn)
    actual_duration = time.time() - before_time

    # Compute delta
    delta = StatsDelta(duration_seconds=actual_duration)

    for key, after in snap_after.items():
        if key not in snap_before:
            delta.new_queries.append(after)
        else:
            before = snap_before[key]
            call_diff = after.calls - before.calls
            time_diff = after.total_exec_time - before.total_exec_time
            if call_diff > 0:
                delta.changed_queries.append(
                    {
                        "query": after.query,
                        "delta_calls": call_diff,
                        "delta_time": time_diff,
                        "delta_rows": after.rows - before.rows,
                    }
                )

    # Sort by activity
    delta.changed_queries.sort(key=lambda x: x["delta_calls"], reverse=True)

    return delta
