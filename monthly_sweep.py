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
from datetime import datetime, timezone

from turso_client import TursoClient

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
TIME_BUDGET_SEC = 25 * 60

# 1回の _recheck_threads に渡すスレッド数。全件を一度に渡すと Pass1 が
# 全部終わるまで1件も書き込まれず、タイムアウト時に成果がゼロになる。
CHUNK_THREADS = 5000


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

    for i in range(0, len(threads), CHUNK_THREADS):
        elapsed = time.monotonic() - started
        if elapsed > TIME_BUDGET_SEC:
            print(f"  WARNING: 時間予算 {TIME_BUDGET_SEC}s を超過したため打ち切り"
                  f"（{processed}/{len(threads)} 件処理済み）", flush=True)
            break

        chunk = threads[i:i + CHUNK_THREADS]
        # pass2_cap は指定しない(無制限) — 月次スイープは TIME_BUDGET_SEC +
        # チャンク分割で実行時間を管理しており、Pass2 を人為的に絞ると
        # 「毎月一括で確実に整合性を取る」という役割そのものが弱まるため。
        written, dead, mismatch, aborted, _deferred = _recheck_threads(
            client, youtube, chunk, "月次スイープ",
        )
        total_written += written
        total_dead += dead
        total_mismatch += mismatch
        processed += len(chunk)
        print(f"  進捗 {processed}/{len(threads)}: "
              f"食い違い{mismatch}件、返信{written}件書込み、消滅{dead}件", flush=True)

        if aborted:
            print("  中断（クォータ枯渇 または Pass1 の切り詰め検知）", flush=True)
            break

    print(f"月次スイープ完了: {processed}/{len(threads)} 件確認、"
          f"食い違い{total_mismatch}件、返信{total_written}件書込み、消滅{total_dead}件、"
          f"所要 {time.monotonic() - started:.0f}s", flush=True)


if __name__ == "__main__":
    main()
