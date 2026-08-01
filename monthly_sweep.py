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

from sync import (
    RECHECK_ID_CHUNK,
    _API_KEYS,
    _recheck_threads,
    get_youtube,
)

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


def fetch_threads(client: TursoClient, days: int) -> list[dict]:
    """対象期間に立ったスレッドを古い順に返す(idx_parent_published の SEARCH)。

    31日分で約6万行(2026-07-30実測)返るため、TursoClientの既定30秒では
    ReadTimeoutになりうる(2026-07-30の初回dry-run実行で発生)。
    """
    cutoff = int(datetime.now(timezone.utc).timestamp()) - days * 86400
    return client.query(
        "SELECT comment_id, published_at FROM comments "
        "WHERE parent_id IS NULL AND published_at >= ? "
        "ORDER BY published_at ASC",
        [cutoff],
        timeout=120,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"遡る日数 (既定: {DEFAULT_DAYS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="対象件数と概算コストだけ出して終了する")
    args = parser.parse_args()

    if not _API_KEYS:
        print("ERROR: API_KEY_FOR_ALL_COMMENT_GET を設定してください")
        sys.exit(1)

    url = os.getenv("TURSO_URL")
    token = os.getenv("TURSO_AUTH_TOKEN")
    if not url or not token:
        print("ERROR: TURSO_URL と TURSO_AUTH_TOKEN を設定してください")
        sys.exit(1)

    client = TursoClient(url, token)
    started = time.monotonic()

    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] "
          f"月次スイープ開始（直近{args.days}日）", flush=True)

    threads = fetch_threads(client, args.days)
    pass1_units = -(-len(threads) // RECHECK_ID_CHUNK)  # 切り上げ
    print(f"  対象スレッド: {len(threads)} 件 / Pass1 概算 {pass1_units} units", flush=True)

    if args.dry_run:
        print("  --dry-run のため API は叩かずに終了", flush=True)
        return
    if not threads:
        return

    youtube = get_youtube()
    total_written = total_dead = total_mismatch = 0
    processed = 0
    changed_count = queued_count = 0

    for i in range(0, len(threads), CHUNK_THREADS):
        elapsed = time.monotonic() - started
        if elapsed > TIME_BUDGET_SEC:
            print(f"  WARNING: 時間予算 {TIME_BUDGET_SEC}s を超過したため打ち切り"
                  f"（{processed}/{len(threads)} 件処理済み）", flush=True)
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
        processed += len(chunk)
        changed, queued = _persist_chunk_repairs(client, thread_ids, before)
        changed_count += changed
        queued_count += queued
        print(f"  進捗 {processed}/{len(threads)}: "
              f"食い違い{mismatch}件、返信{written}件書込み、消滅{dead}件", flush=True)

        if aborted:
            print("  中断（クォータ枯渇 または Pass1 の切り詰め検知）", flush=True)
            break

    print(f"  集計修復キュー: 変更行{changed_count}件 / work {queued_count}件", flush=True)
    print(f"月次スイープ完了: {processed}/{len(threads)} 件確認、"
          f"食い違い{total_mismatch}件、返信{total_written}件書込み、消滅{total_dead}件、"
          f"所要 {time.monotonic() - started:.0f}s", flush=True)


if __name__ == "__main__":
    main()
