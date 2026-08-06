"""
スレッド巡回チェックのホット層(直近24時間のスレッド全件)だけを実行する。

2026-08-05: sync.py の毎分ジョブから分離した専用エントリポイント。
GitHub Actions .github/workflows/hot_recheck.yml から独自の cron スケジュールで
呼ばれる(毎分ジョブ sync_comments.yml とは別の concurrency group)。

分離した理由: ホット層の Pass2(食い違いスレッドの完全再取得、1スレッド=最低1
リクエストの逐次実行)はバースト時に数千件規模になり得る。以前は sync.py の
毎分ジョブ(timeout-minutes: 10)内で処理していたため、Pass2 が長引くと
GitHub Actions の concurrency(cancel-in-progress: false)下で待機中の後続の
毎分run群がまとめてcancelされ、新着コメント同期(sync_new_comments、唯一
代替のない経路)そのものが遅延する恐れがあった(年次バーストイベント耐性調査、2026-08-05)。
独立ワークフロー・独立concurrency groupに分けることで、ホット層がどれだけ
時間を使っても毎分の新着同期には一切影響しなくなる。

環境変数は sync.py と共通:
  API_KEY_FOR_ALL_COMMENT_GET, API_KEY_FOR_ALL_COMMENT_GET2, API_KEY_FOR_ALL_COMMENT_GET3
  VIDEO_ID, TURSO_URL, TURSO_AUTH_TOKEN, CRONJOB_SECRET
  SYNC_HOT_BATCH_CAP, SYNC_HOT_PASS2_RUN_CAP (バースト調整用、sync.py側で読む)
"""

import os
import sys

# sync.py が import 時点でモジュールレベルの dotenv.load_dotenv() と
# API キープールの構築を行うため、ここで重複して呼ぶ必要はない。
import sync
from turso_client import TursoClient


def main() -> None:
    if not sync.api_keys_configured():
        print("ERROR: API_KEY_FOR_ALL_COMMENT_GET を設定してください")
        sys.exit(1)

    url = os.environ.get("TURSO_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")
    if not url or not token:
        print("ERROR: TURSO_URL と TURSO_AUTH_TOKEN を設定してください")
        sys.exit(1)

    client = TursoClient(url, token)

    print("ホット層巡回チェック開始", flush=True)
    n_hot = sync.run_hot_recheck_batch(client)
    print(f"  ホット層巡回チェック書込み: {n_hot} 件", flush=True)


if __name__ == "__main__":
    main()
