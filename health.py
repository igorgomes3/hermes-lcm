"""Database health probe for the LCM store (stdlib only).

Written after W62: ``lcm.db`` had its page 1 overwritten by a b-tree page, the
plugin failed to load with ``file is not a database`` 2,396 times over 13 days,
and Hermes fell back to the lossy built-in compressor without anyone noticing.
The plugin now refuses such a file at open time; this probe is the outside
counterpart that notices when the plugin is not running at all.

Checks:
    header      first 16 bytes must be the SQLite magic
    quick_check PRAGMA quick_check == "ok"
    wal_stall   a non-empty WAL that has not moved for --max-wal-age seconds,
                only while a session is active
    growth      messages/summary_nodes must advance while sessions are active

"Active" means state.db (--state-db) shows a session touched within --window
seconds. Idle nights are not failures: gating on activity removes the false
"LCM may be dead" alerts the ungated version produced.

Usage:
    python3 health.py [--db PATH] [--state PATH] [--state-db PATH] [--max-wal-age S] [--window S]

Exit status 0 and no output when healthy; one line plus the log path and exit
status 1 when something is wrong. Designed for a cron job with no_agent=true.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

HEADER_MAGIC = b"SQLite format 3\x00"
DEFAULT_DB = Path.home() / ".hermes" / "lcm.db"
DEFAULT_STATE = Path.home() / ".hermes" / "cache" / "lcm-health-state.json"
DEFAULT_STATE_DB = Path.home() / ".hermes" / "state.db"
DEFAULT_LOG = Path.home() / ".hermes" / "logs" / "lcm-health.log"


def _log(line: str, log_path: Path = DEFAULT_LOG) -> None:
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {line}\n")
    except OSError:
        pass


def check_header(db: Path) -> str | None:
    try:
        with db.open("rb") as handle:
            head = handle.read(16)
    except FileNotFoundError:
        return f"database missing: {db}"
    except OSError as exc:
        return f"cannot read database: {exc}"
    if not head:
        return f"database is empty (0 bytes): {db}"
    if head != HEADER_MAGIC:
        return f"page 1 is not a SQLite header (first bytes {head.hex()}) — file stomped, restore a backup"
    return None


def check_quick_check(db: Path) -> str | None:
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        return f"cannot open database: {exc}"
    try:
        row = conn.execute("PRAGMA quick_check").fetchone()
    except sqlite3.Error as exc:
        return f"quick_check failed: {exc}"
    finally:
        conn.close()
    result = row[0] if row else "no result"
    if result != "ok":
        return f"quick_check reports corruption: {str(result)[:160]}"
    return None


def check_wal_stall(db: Path, max_age_seconds: int, active: bool = True) -> str | None:
    if not active:
        return None
    wal = db.with_name(db.name + "-wal")
    if not wal.exists():
        return None
    try:
        size = wal.stat().st_size
        age = time.time() - wal.stat().st_mtime
    except OSError:
        return None
    if size > 0 and age > max_age_seconds:
        return f"WAL has {size} bytes untouched for {int(age // 60)} min (writer stalled or checkpoint blocked)"
    return None


def last_session_activity(state_db: Path | None) -> float | None:
    """Most recent session activity in state.db, or None when unavailable.

    A stalled DB is only a problem while sessions are running: at 3am with no
    traffic the LCM legitimately writes nothing. Without this gate the growth
    check fired a false "LCM may be dead" alert at 02:01 on 2026-11-17.
    """
    if state_db is None:
        return None
    try:
        conn = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    try:
        row = conn.execute("SELECT max(last_activity_at) FROM sessions").fetchone()
    except sqlite3.Error:
        return None
    finally:
        conn.close()
    if not row or not row[0]:
        return None
    try:
        return float(row[0])
    except (TypeError, ValueError):
        return None


def _counts(db: Path) -> tuple[int, int] | None:
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    try:
        messages = conn.execute("SELECT count(*) FROM messages").fetchone()[0]
        summaries = conn.execute("SELECT count(*) FROM summary_nodes").fetchone()[0]
    except sqlite3.Error:
        return None
    finally:
        conn.close()
    return messages, summaries


def check_growth(db: Path, state_path: Path, window_seconds: int, active: bool = True) -> str | None:
    current = _counts(db)
    if current is None:
        return None
    previous = None
    try:
        previous = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        previous = None
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(
            json.dumps({"ts": time.time(), "messages": current[0], "summary_nodes": current[1]}),
            encoding="utf-8",
        )
    except OSError:
        pass
    if not active:
        return None
    if not previous or "ts" not in previous:
        return None
    elapsed = time.time() - float(previous["ts"])
    if elapsed < window_seconds:
        return None
    if previous.get("messages") == current[0] and previous.get("summary_nodes") == current[1]:
        return (
            f"no rows written in {int(elapsed // 60)} min "
            f"(messages={current[0]}, summary_nodes={current[1]}) — LCM may be dead"
        )
    return None


def check_database(
    db: Path = DEFAULT_DB,
    *,
    state_path: Path = DEFAULT_STATE,
    max_wal_age: int = 3600,
    window: int = 3600,
    state_db: Path | None = DEFAULT_STATE_DB,
) -> dict[str, object]:
    activity = last_session_activity(state_db)
    active = bool(activity and (time.time() - activity) <= window)
    problems = [
        problem
        for problem in (
            check_header(db),
            check_quick_check(db),
            check_wal_stall(db, max_wal_age, active),
            check_growth(db, state_path, window, active),
        )
        if problem
    ]
    return {
        "ok": not problems,
        "problems": problems,
        "db": str(db),
        "sessions_active": active,
        "last_session_activity": activity,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="LCM database health probe")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--max-wal-age", type=int, default=3600)
    parser.add_argument("--window", type=int, default=3600)
    parser.add_argument(
        "--state-db",
        default=str(DEFAULT_STATE_DB),
        help="gateway state.db used to tell whether sessions are active ('' to disable)",
    )
    parser.add_argument("--json", action="store_true", help="print the full report")
    args = parser.parse_args(argv)

    report = check_database(
        Path(args.db),
        state_path=Path(args.state),
        max_wal_age=args.max_wal_age,
        window=args.window,
        state_db=Path(args.state_db) if args.state_db else None,
    )
    if args.json:
        print(json.dumps(report, indent=2))
        return 0 if report["ok"] else 1
    if report["ok"]:
        return 0
    problems = report["problems"]
    detail = "; ".join(str(p) for p in problems) if isinstance(problems, list) else str(problems)
    line = "LCM DB HEALTH FAIL: " + detail
    _log(line)
    print(f"{line} | log: {DEFAULT_LOG}", file=sys.stdout)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
