"""
月次スイープ。直近1ヶ月ぶんのスレッドを一気に Pass1/Pass2 で確認し、整合性を取る。

sync.py の巡回チェックには「同じスレッドで同じチェック間隔内に1件削除＋1件追加が
起きると totalReplyCount が一致して見逃す」という原理的な弱点がある
(件数比較でしか差分を検知していないため)。コールド層が全履歴を約20日で
一周するのでいずれ拾い直せるが、そのタイミングでも同じ数だった場合は
すり抜け続ける。ここで月1回まとめて見直すことでその取りこぼしを回収する。

sync.py の Pass1/Pass2 をそのまま再利用するので、コストは
  スレッド数 ÷ RECHECK_ID_CHUNK + 食い違い分
で済む。直近1ヶ月 ≒ 57,600スレッドなら Pass1 は約1,152 units。
1スレッド=1 unit でフル再取得すると57,600 units(1プロジェクトの5.7日分)に
なって成立しないので、この方式であることが前提。

毎分動く sync.py とは別ワークフロー・別 event_type にしてある。
同じジョブに相乗りさせると、その分の実行時間ぶん毎分同期が止まるため。

実行:
  python monthly_sweep.py                # 直近31日
  python monthly_sweep.py --days 60      # 範囲を変える
  python monthly_sweep.py --dry-run      # Pass1 の対象件数と概算コストだけ出す

環境変数: sync.py と同じ
"""

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from turso_client import TursoClient
import repair_queue
import sync as sync_module
from sync import (
    RECHECK_ID_CHUNK,
    _recheck_threads,
)

MONTHLY_API_KEY = os.getenv("API_KEY_FOR_MONTHLY_SWEEP", "").strip()
DEFAULT_DAYS = 31

# GitHub Actions の timeout-minutes より短く切り上げ、途中で強制終了されて
# ログを失うのを避ける。打ち切っても次の月次実行でやり直せる
# (カーソルを持たない = 常に「直近N日」を見るだけなので状態の引き継ぎは不要)。
TIME_BUDGET_SEC = 110 * 60

# 1回の _recheck_threads に渡すスレッド数。全件を一度に渡すと Pass1 が
# 全部終わるまで1件も書き込まれず、タイムアウト時に成果がゼロになる。
CHUNK_THREADS = 5000
SNAPSHOT_IN_CHUNK = 200
JST = ZoneInfo("Asia/Tokyo")
STATE_KEY = "monthly"
INCOMPLETE_EXIT_CODE = 2

STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS monthly_sweep_state (
  state_key TEXT PRIMARY KEY,
  run_month TEXT NOT NULL,
  window_start INTEGER NOT NULL,
  window_end INTEGER NOT NULL,
  cursor_published_at INTEGER,
  cursor_comment_id TEXT,
  processed_count INTEGER NOT NULL,
  total_count INTEGER NOT NULL,
  status TEXT NOT NULL,
  updated_at INTEGER NOT NULL
)
"""


def _snapshot_thread_rows(client: TursoClient, thread_ids: list[str]) -> dict[str, dict]:
    """Read the rows whose active membership can change during Pass 2."""
    snapshot: dict[str, dict] = {}
    for offset in range(0, len(thread_ids), SNAPSHOT_IN_CHUNK):
        chunk = thread_ids[offset:offset + SNAPSHOT_IN_CHUNK]
        marks = ",".join("?" for _ in chunk)
        rows = client.query(
            "SELECT comment_id,parent_id,author_channel_id,handle,text,published_at,is_deleted "
            f"FROM comments WHERE comment_id IN ({marks}) OR parent_id IN ({marks})",
            [*chunk, *chunk],
        )
        for row in rows:
            snapshot[row["comment_id"]] = row
    return snapshot


def _changed_rows(before: dict[str, dict], after: dict[str, dict]) -> list[dict]:
    """Return both sides of every insert/delete or aggregate-relevant edit."""
    changed: list[dict] = []
    for comment_id in set(before) | set(after):
        old = before.get(comment_id)
        new = after.get(comment_id)
        fields = (
            "parent_id", "author_channel_id", "handle", "text", "published_at", "is_deleted",
        )
        if old is None or new is None or any(old.get(field) != new.get(field) for field in fields):
            if old is not None:
                changed.append(old)
            if new is not None:
                changed.append(new)
    return changed


def _repair_work(rows: list[dict]) -> dict[str, set[str]]:
    work: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        stamp = row.get("published_at")
        if stamp is None:
            continue
        dt = datetime.fromtimestamp(int(stamp), JST)
        day = dt.date().isoformat()
        hour_start = (int(stamp) // 3600) * 3600
        for consumer in (
            "ranking_date", "daily_stats_date", "analytics_date",
            "wordcloud_recent7d_date", "wordcloud_recent30d_date",
        ):
            work[consumer].add(day)
        work["hourly_bucket"].add(str(hour_start))
        author_id = row.get("author_channel_id")
        if author_id:
            work["account_profile"].add(str(author_id))
    if rows:
        work["network_full"].add("required")
        work["calendar_wordcloud_full"].add("required")
        work["account_map"].add("required")
    return work


def _persist_chunk_repairs(
    client: TursoClient, thread_ids: list[str], before: dict[str, dict],
) -> tuple[int, int]:
    after = _snapshot_thread_rows(client, thread_ids)
    changed = _changed_rows(before, after)
    work = _repair_work(changed)
    queued = repair_queue.enqueue(client, work) if work else 0
    return len(changed), queued


def ensure_state_table(client: TursoClient) -> None:
    client.execute(STATE_TABLE_SQL)


def load_state(client: TursoClient) -> dict | None:
    ensure_state_table(client)
    rows = client.query(
        "SELECT state_key,run_month,window_start,window_end,cursor_published_at,"
        "cursor_comment_id,processed_count,total_count,status,updated_at "
        "FROM monthly_sweep_state WHERE state_key=?",
        [STATE_KEY],
    )
    return rows[0] if rows else None


def fetch_threads(
    client: TursoClient,
    window_start: int,
    window_end: int,
    cursor_published_at: int | None = None,
    cursor_comment_id: str | None = None,
) -> list[dict]:
    """Return a stable, fixed-window thread snapshot after an optional cursor."""
    sql = (
        "SELECT comment_id, published_at FROM comments "
        "WHERE parent_id IS NULL AND published_at >= ? AND published_at <= ?"
    )
    args: list = [window_start, window_end]
    if cursor_published_at is not None and cursor_comment_id is not None:
        sql += " AND (published_at > ? OR (published_at = ? AND comment_id > ?))"
        args.extend([cursor_published_at, cursor_published_at, cursor_comment_id])
    sql += " ORDER BY published_at ASC, comment_id ASC"
    return client.query(sql, args, timeout=120)


def initialize_state(
    client: TursoClient,
    days: int,
    window_end: int,
    resume_offset: int,
) -> tuple[dict, list[dict]]:
    window_start = window_end - days * 86400
    threads = fetch_threads(client, window_start, window_end)
    if resume_offset < 0 or resume_offset > len(threads):
        raise ValueError(
            f"resume offset {resume_offset} is outside 0..{len(threads)}"
        )

    cursor = threads[resume_offset - 1] if resume_offset else None
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    run_month = datetime.fromtimestamp(window_end, JST).strftime("%Y-%m")
    state = {
        "state_key": STATE_KEY,
        "run_month": run_month,
        "window_start": window_start,
        "window_end": window_end,
        "cursor_published_at": cursor["published_at"] if cursor else None,
        "cursor_comment_id": cursor["comment_id"] if cursor else None,
        "processed_count": resume_offset,
        "total_count": len(threads),
        "status": "in_progress",
        "updated_at": now_epoch,
    }
    ensure_state_table(client)
    client.execute(
        "INSERT INTO monthly_sweep_state "
        "(state_key,run_month,window_start,window_end,cursor_published_at,"
        "cursor_comment_id,processed_count,total_count,status,updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(state_key) DO UPDATE SET "
        "run_month=excluded.run_month,window_start=excluded.window_start,"
        "window_end=excluded.window_end,"
        "cursor_published_at=excluded.cursor_published_at,"
        "cursor_comment_id=excluded.cursor_comment_id,"
        "processed_count=excluded.processed_count,total_count=excluded.total_count,"
        "status=excluded.status,updated_at=excluded.updated_at",
        [
            STATE_KEY, run_month, window_start, window_end,
            state["cursor_published_at"], state["cursor_comment_id"],
            resume_offset, len(threads), "in_progress", now_epoch,
        ],
    )
    return state, threads[resume_offset:]


def checkpoint_state(client: TursoClient, state: dict, chunk: list[dict]) -> None:
    last = chunk[-1]
    state["cursor_published_at"] = int(last["published_at"])
    state["cursor_comment_id"] = str(last["comment_id"])
    state["processed_count"] += len(chunk)
    state["updated_at"] = int(datetime.now(timezone.utc).timestamp())
    client.execute(
        "UPDATE monthly_sweep_state SET cursor_published_at=?,cursor_comment_id=?,"
        "processed_count=?,updated_at=? WHERE state_key=? AND status='in_progress'",
        [
            state["cursor_published_at"], state["cursor_comment_id"],
            state["processed_count"], state["updated_at"], STATE_KEY,
        ],
    )


def complete_state(client: TursoClient, state: dict) -> None:
    state["status"] = "completed"
    state["processed_count"] = state["total_count"]
    state["updated_at"] = int(datetime.now(timezone.utc).timestamp())
    client.execute(
        "UPDATE monthly_sweep_state SET processed_count=?,status='completed',updated_at=? "
        "WHERE state_key=?",
        [state["processed_count"], state["updated_at"], STATE_KEY],
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"遡る日数 (既定: {DEFAULT_DAYS})")
    parser.add_argument("--window-end", type=int,
                        help="固定スキャン窓の終了epoch秒")
    parser.add_argument("--resume-offset", type=int, default=0,
                        help="初回状態作成時に処理済みとして飛ばす件数")
    parser.add_argument("--restart", action="store_true",
                        help="既存状態を破棄して新しい固定窓を開始する")
    parser.add_argument("--dry-run", action="store_true",
                        help="対象件数と概算コストだけ出して終了する")
    args = parser.parse_args()

    if not MONTHLY_API_KEY:
        print("ERROR: API_KEY_FOR_MONTHLY_SWEEP を設定してください")
        sys.exit(1)
    sync_module.configure_api_keys([MONTHLY_API_KEY], allow_rotation=False)

    url = os.getenv("TURSO_URL")
    token = os.getenv("TURSO_AUTH_TOKEN")
    if not url or not token:
        print("ERROR: TURSO_URL と TURSO_AUTH_TOKEN を設定してください")
        sys.exit(1)

    client = TursoClient(url, token)
    started = time.monotonic()
    requested_end = args.window_end or int(datetime.now(timezone.utc).timestamp())
    requested_start = requested_end - args.days * 86400
    requested_month = datetime.fromtimestamp(requested_end, JST).strftime("%Y-%m")

    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] "
          f"月次スイープ開始（直近{args.days}日）", flush=True)

    if args.dry_run:
        preview = fetch_threads(client, requested_start, requested_end)
        if args.resume_offset < 0 or args.resume_offset > len(preview):
            raise ValueError(
                f"resume offset {args.resume_offset} is outside 0..{len(preview)}"
            )
        remaining = len(preview) - args.resume_offset
        pass1_units = -(-remaining // RECHECK_ID_CHUNK)
        print(
            f"  固定窓: {requested_start}..{requested_end} / "
            f"全{len(preview)}件 / 再開後{remaining}件 / "
            f"Pass1概算{pass1_units} units",
            flush=True,
        )
        print("  --dry-run のため API は叩かずに終了", flush=True)
        return

    state = load_state(client)
    if state and state["status"] == "in_progress" and not args.restart:
        threads = fetch_threads(
            client,
            int(state["window_start"]),
            int(state["window_end"]),
            state.get("cursor_published_at"),
            state.get("cursor_comment_id"),
        )
        expected_remaining = int(state["total_count"]) - int(state["processed_count"])
        if len(threads) != expected_remaining:
            raise RuntimeError(
                "fixed-window thread count changed while resuming: "
                f"expected {expected_remaining}, got {len(threads)}"
            )
        print(
            f"  再開: {state['processed_count']}/{state['total_count']}件完了済み / "
            f"固定窓 {state['window_start']}..{state['window_end']}",
            flush=True,
        )
    elif (
        state and state["status"] == "completed"
        and state["run_month"] == requested_month and not args.restart
    ):
        print(
            f"  {requested_month} の月次スイープは既に完了済み "
            f"({state['processed_count']}/{state['total_count']})",
            flush=True,
        )
        return
    else:
        state, threads = initialize_state(
            client, args.days, requested_end, args.resume_offset,
        )
        print(
            f"  新規状態: {state['processed_count']}/{state['total_count']}件完了済み / "
            f"固定窓 {state['window_start']}..{state['window_end']}",
            flush=True,
        )

    pass1_units = -(-len(threads) // RECHECK_ID_CHUNK)
    print(
        f"  今回対象: {len(threads)}件 / 全体{state['total_count']}件 / "
        f"Pass1概算{pass1_units} units",
        flush=True,
    )

    youtube = sync_module.get_youtube()
    total_written = total_dead = total_mismatch = 0
    changed_count = queued_count = 0
    incomplete_reason = None

    for i in range(0, len(threads), CHUNK_THREADS):
        elapsed = time.monotonic() - started
        if elapsed > TIME_BUDGET_SEC:
            incomplete_reason = f"時間予算 {TIME_BUDGET_SEC}s 超過"
            break

        chunk = threads[i:i + CHUNK_THREADS]
        thread_ids = [row["comment_id"] for row in chunk]
        before = _snapshot_thread_rows(client, thread_ids)
        # pass2_cap は指定しない(無制限) — 月次スイープは TIME_BUDGET_SEC +
        # チャンク分割で実行時間を管理しており、Pass2 を人為的に絞ると
        # 「毎月一括で確実に整合性を取る」という役割そのものが弱まるため。
        try:
            written, dead, mismatch, aborted, _deferred = _recheck_threads(
                client, youtube, chunk, "月次スイープ",
            )
        except Exception:
            # Writes may already have succeeded. Persist their repair work
            # before propagating the failure so aggregates cannot drift.
            changed, queued = _persist_chunk_repairs(client, thread_ids, before)
            changed_count += changed
            queued_count += queued
            raise
        total_written += written
        total_dead += dead
        total_mismatch += mismatch
        changed, queued = _persist_chunk_repairs(client, thread_ids, before)
        changed_count += changed
        queued_count += queued

        if aborted:
            incomplete_reason = "クォータ枯渇またはAPIレスポンス切り詰め"
            print(
                f"  チャンク未完了のためカーソル維持: "
                f"{state['processed_count']}/{state['total_count']}",
                flush=True,
            )
            break

        checkpoint_state(client, state, chunk)
        print(
            f"  進捗 {state['processed_count']}/{state['total_count']}: "
            f"食い違い{mismatch}件、返信{written}件書込み、消滅{dead}件",
            flush=True,
        )

    print(f"  集計修復キュー: 変更行{changed_count}件 / work {queued_count}件", flush=True)
    if incomplete_reason:
        print(
            f"月次スイープ未完了: {state['processed_count']}/{state['total_count']}件 / "
            f"理由={incomplete_reason} / 次回は保存カーソルから再開 / "
            f"所要 {time.monotonic() - started:.0f}s",
            flush=True,
        )
        raise SystemExit(INCOMPLETE_EXIT_CODE)

    if state["processed_count"] != state["total_count"]:
        raise RuntimeError(
            f"completion count mismatch: {state['processed_count']} != {state['total_count']}"
        )
    complete_state(client, state)
    print(
        f"月次スイープ完了: {state['processed_count']}/{state['total_count']}件確認、"
        f"食い違い{total_mismatch}件、返信{total_written}件書込み、"
        f"消滅{total_dead}件、所要 {time.monotonic() - started:.0f}s",
        flush=True,
    )


if __name__ == "__main__":
    main()
