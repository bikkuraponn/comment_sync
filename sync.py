"""
毎分差分同期 + スレッド巡回チェック。GitHub Actions から呼び出す。

実行する処理:
  毎分    : sync_new_comments()        新しく立ったスレッドとその返信を取得
  毎分    : check_ksk_commands()       ksk(加速)スレッドの登録/解除コマンド検出(YouTube API不使用)
  1時間おき: check_dormant_ksk_threads() ended な ksk スレッドの復活検知(YouTube API不使用)
  10分おき : run_reply_recheck_batch()  コールド層(全履歴のカーソル巡回)
  30分おき : 同上 + ホット層(直近24時間のスレッド全件)

既存スレッドに後から付いた返信・スレッド/返信の削除を検知するのは
run_reply_recheck_batch() だけ(sync_new_comments は新規スレッドしか見ない)。
詳細はその節のコメントを参照。

環境変数:
  API_KEY_FOR_ALL_COMMENT_GET, API_KEY_FOR_ALL_COMMENT_GET2, API_KEY_FOR_ALL_COMMENT_GET3
  VIDEO_ID
  TURSO_URL
  TURSO_AUTH_TOKEN
  CRONJOB_SECRET   (GitHub Actions の secret 認証用)
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import dotenv
import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

dotenv.load_dotenv(Path(__file__).parent.parent / "flaskr" / ".env")
dotenv.load_dotenv()

import time_comment_common
import ksk_common
import account_live_core
import repair_queue
from turso_client import TursoClient

VIDEO_ID = os.getenv("VIDEO_ID", "[REDACTED_VIDEO_ID]")
BATCH_SIZE = 300
JST = ZoneInfo("Asia/Tokyo")

# ------------------------------------------------------------------ #
# API キーローテーション（ダッシュボード用 YOUTUBE_API_KEY は使わない）
# ------------------------------------------------------------------ #

_API_KEYS = [k for k in [
    os.getenv("API_KEY_FOR_ALL_COMMENT_GET"),
    os.getenv("API_KEY_FOR_ALL_COMMENT_GET2"),
    os.getenv("API_KEY_FOR_ALL_COMMENT_GET3"),
] if k]
_key_idx = 0

# 日次クォータを実際に使い切ったと確認できたキーの番号。
# 以前はこれが単一のカウンタ(_exhausted_count)で、日次クォータ枯渇による
# ローテーションと、一時的なレート制限がバックオフ上限に達したことによる
# ローテーションの両方が同じ数字を加算し、しかも成功時にリセットされなかった。
# キーが3本なら、1回の実行中に無関係な一時的レート制限が3回起きただけで
# 「全キーのクォータが枯渇」と判断して処理を打ち切ってしまう
# — よりによって予備キーが一番欲しいコメント急増時に起きやすい誤判定だった。
_exhausted_keys: set[int] = set()

_rate_limit_rotations = 0
# レート制限由来のローテーション回数の上限。全キーを2周してなお解消しないなら
# 個別キーの問題ではないので諦める(無限ループ防止のためだけの値)。
MAX_RATE_LIMIT_ROTATIONS = 2 * max(1, len(_API_KEYS))
_allow_key_rotation = True


def configure_api_keys(api_keys: list[str], *, allow_rotation: bool = True) -> None:
    """Replace the process-local API key pool for a specialized entry point."""
    global _API_KEYS, _key_idx, _rate_limit_rotations
    global MAX_RATE_LIMIT_ROTATIONS, _allow_key_rotation

    _API_KEYS = [key.strip() for key in api_keys if key and key.strip()]
    _key_idx = 0
    _exhausted_keys.clear()
    _rate_limit_rotations = 0
    MAX_RATE_LIMIT_ROTATIONS = 2 * max(1, len(_API_KEYS))
    _allow_key_rotation = bool(allow_rotation)


def get_youtube():
    return build("youtube", "v3", developerKey=_API_KEYS[_key_idx], cache_discovery=False)


def rotate_key(e: Exception, daily_quota: bool) -> bool:
    """次に使えるキーへ切り替える。切り替え先が無ければ False。

    daily_quota=True のときだけ、現在のキーを「その日はもう使えない」と記録する。
    レート制限は数十〜百秒で回復する一時的なものなので、そのキーの1日の予算を
    使い切ったことにはしない(記録してしまうと、まだ予算のあるキーを二度と
    使わなくなる)。
    """
    global _key_idx, _rate_limit_rotations

    if not _allow_key_rotation:
        if daily_quota:
            _exhausted_keys.add(_key_idx)
        print(f"ERROR: API key unavailable; rotation is disabled: {e}")
        return False

    if daily_quota:
        _exhausted_keys.add(_key_idx)
        if len(_exhausted_keys) >= len(_API_KEYS):
            print(f"ERROR: 全キーの日次クォータが枯渇: {e}")
            return False
    else:
        _rate_limit_rotations += 1
        if _rate_limit_rotations > MAX_RATE_LIMIT_ROTATIONS:
            print(f"ERROR: レート制限が全キーで解消しないため中断: {e}")
            return False

    for step in range(1, len(_API_KEYS) + 1):
        candidate = (_key_idx + step) % len(_API_KEYS)
        if candidate not in _exhausted_keys:
            _key_idx = candidate
            print(f"APIキーをローテーション → キー {candidate + 1}")
            return True

    print(f"ERROR: 全キーの日次クォータが枯渇: {e}")
    return False


def is_daily_quota_error(e: HttpError) -> bool:
    """1日のプロジェクトクォータを使い切った(太平洋時間の日次リセットまで回復しない)。"""
    err = str(e).lower()
    return e.resp.status in (403, 429) and any(s in err for s in [
        "quotaexceeded", "dailylimitexceeded", "userdailylimitexceeded",
    ])


def is_rate_limit_error(e: HttpError) -> bool:
    """短時間(概ね100秒)のレート制限。1日のクォータ枯渇とは別物で、少し待てば
    同じキーのまま回復する。userRateLimitExceeded/rateLimitExceeded がこれに該当。

    以前は is_daily_quota_error と同じ扱いで即座にキーをローテーションしていたが、
    これは特にコメント急増時(バーストで短時間に大量リクエストが集中しやすい)に
    「1日の予算はまだ十分残っているキーを、一時的な混雑だけで見捨てて次のキーへ
    切り替える」誤判定を招く。よりによって複数キーの予備が一番欲しいバースト中に
    予備キーを無駄に消費してしまうため、日次枯渇とは別に扱い、まず同じキーで
    バックオフ再試行する(_handle_api_error 参照)。
    """
    err = str(e).lower()
    return e.resp.status in (403, 429) and "ratelimitexceeded" in err


MAX_RATE_LIMIT_RETRIES = 5
RATE_LIMIT_BACKOFF_BASE_SEC = 2  # 2, 4, 8, 16, 32 秒と指数バックオフ


def _handle_api_error(e: HttpError, youtube, rate_limit_retries: int):
    """YouTube API呼び出し失敗時の共通ハンドリング。4箇所のリトライループで共用する。

    レート制限(is_rate_limit_error)は MAX_RATE_LIMIT_RETRIES 回まで同じキーの
    ままバックオフして再試行し、それでも解消しなければ日次クォータ枯渇と同じく
    rotate_key() で次のキーへ切り替える。それ以外の HttpError はここでは
    処理せず、呼び出し側に再送出させる。

    戻り値: (次に使う youtube クライアント, 更新後の rate_limit_retries,
             呼び出し元で raise すべきか, 全キー枯渇で諦めるべきか)
    """
    if is_rate_limit_error(e):
        if rate_limit_retries < MAX_RATE_LIMIT_RETRIES:
            wait = RATE_LIMIT_BACKOFF_BASE_SEC * (2 ** rate_limit_retries)
            print(
                f"  レート制限、{wait}秒待って同じキーでリトライ"
                f"({rate_limit_retries + 1}/{MAX_RATE_LIMIT_RETRIES})",
                flush=True,
            )
            time.sleep(wait)
            return youtube, rate_limit_retries + 1, False, False
        print(
            f"  レート制限が{MAX_RATE_LIMIT_RETRIES}回連続で解消しないため、"
            f"キーを切り替える",
            flush=True,
        )
    elif not is_daily_quota_error(e):
        return youtube, rate_limit_retries, True, False

    if not rotate_key(e, daily_quota=is_daily_quota_error(e)):
        return youtube, rate_limit_retries, False, True
    return get_youtube(), 0, False, False


# ------------------------------------------------------------------ #
# 時刻ユーティリティ
# ------------------------------------------------------------------ #

def wait_until_next_minute() -> None:
    now = datetime.now(timezone.utc)
    wait_sec = 60 - now.second - now.microsecond / 1_000_000
    if 0 < wait_sec < 60:
        print(f":00 まで {wait_sec:.1f} 秒待機...")
        time.sleep(wait_sec)


# ------------------------------------------------------------------ #
# ヘルパ
# ------------------------------------------------------------------ #

def parse_epoch(dt_str: str) -> int:
    return int(datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
               .replace(tzinfo=timezone.utc).timestamp())


def is_deleted_sentinel(snippet: dict) -> bool:
    handle = snippet.get("authorDisplayName", "")
    return handle == "" or str(snippet.get("likeCount", "")).upper() == "DELETED"


def get_latest_thread_pub(client: TursoClient) -> int | None:
    # idx_parent_published により1行読み取りで済む(is_deleted=0を足してもEXPLAIN QUERY PLAN
    # で SEARCH USING INDEX のままであることを2026-07-29に確認済み。SCANにはならない)。
    # スレッドIDではなく published_at を停止条件にする:
    # 最新スレッドが YouTube 上で削除されると ID は二度と API 応答に
    # 現れず、全履歴をページングし続けてクォータを焼き尽くすため。
    #
    # is_deleted=0 を条件に含める理由(2026-07-29追加): comments 行は削除されても物理的に
    # 消えず is_deleted フラグが立つだけなので、この条件が無いと MAX(published_at) は
    # 削除されたスレッドの値を指したまま永久に固定される。sync_new_comments() の停止条件
    # (stop_pub)がこの値を使っており、最新スレッドが削除されると1ページ目で新着を1件も
    # 見ないまま既知データ域に達してしまい、固定コメント判定の分岐(_reconcile_pinned_comment)
    # に古いスレッドが次々と誤って落ち、is_pinned の付け替えが連鎖する事故があった
    # (詳細はそちらのコメント参照)。is_deleted=0 を足すことで、巡回チェックが該当スレッドの
    # 削除を検知してis_deleted=1を確定させた後は、次の実行から自動的に「その次に新しい
    # 生きているスレッド」に停止条件がずれ、自己修復する。
    rows = client.query(
        "SELECT MAX(published_at) AS p FROM comments WHERE parent_id IS NULL AND is_deleted = 0"
    )
    return rows[0]["p"] if rows and rows[0]["p"] is not None else None


_UPSERT_SQL = """
    INSERT INTO comments
      (comment_id, parent_id, reply_order, thread_published_at,
       author_channel_id, handle, text, original_text, published_at,
       like_count, is_pinned, is_deleted, deleted_confirmed_at, fetched_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(comment_id) DO UPDATE SET
      original_text = CASE
        WHEN is_deleted = 0
             AND excluded.text IS NOT NULL
             AND text != excluded.text
             AND original_text IS NULL
        THEN text
        ELSE original_text
      END,
      text       = CASE WHEN is_deleted = 0 THEN excluded.text       ELSE text       END,
      like_count = CASE WHEN is_deleted = 0 THEN excluded.like_count ELSE like_count END,
      is_pinned  = CASE WHEN is_deleted = 0 THEN excluded.is_pinned  ELSE is_pinned  END,
      fetched_at = excluded.fetched_at
"""


def _row_args(r: dict) -> list:
    return [
        r["comment_id"], r.get("parent_id"), r.get("reply_order"),
        r.get("thread_published_at"), r.get("author_channel_id"),
        r.get("handle"), r.get("text"), r.get("original_text"),
        r["published_at"], r.get("like_count"),
        r["is_pinned"], r["is_deleted"],
        r.get("deleted_confirmed_at"), r["fetched_at"],
    ]


def upsert_rows(client: TursoClient, rows: list[dict]) -> None:
    if not rows:
        return
    stmts = []
    for r in rows:
        stmts.append({"sql": _UPSERT_SQL, "args": _row_args(r)})
        if len(stmts) >= BATCH_SIZE:
            client.batch(stmts)
            stmts = []
    if stmts:
        client.batch(stmts)


# ------------------------------------------------------------------ #
# 著者プロフィール(表示名・アイコンURL)の副産物キャッシュ
#
# commentThreads.list / comments.list のレスポンスには authorProfileImageUrl
# が含まれているが、comments テーブルにはこれまで書き込んでいなかった
# (comments.handle 自体もリネームに追従しないスナップショットなので、
# アイコンURL列を comments 側に足しても同じ問題を抱えるだけ)。
# ここでは comments テーブルの書き込み経路(8箇所に分散)には一切触れず、
# 毎分の同期で「たまたま取得できたsnippet」から著者情報だけを別テーブル
# comment_authors に追記する。karotter_bot の週次ランキング更新が、ここで
# 貯まった鮮度の高い情報を優先的に使い、無い/古いアカウントだけ
# YouTube channels.list でバックストップ解決する(ranking_common.py 参照)。
# ------------------------------------------------------------------ #

_AUTHORS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS comment_authors (
  channel_id    TEXT PRIMARY KEY,
  handle        TEXT,
  avatar_url    TEXT,
  updated_at    INTEGER NOT NULL,
  first_seen_at INTEGER
)
"""

_AUTHORS_FIRST_SEEN_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_comment_authors_first_seen "
    "ON comment_authors(first_seen_at)"
)

_UPSERT_AUTHOR_SQL = """
INSERT INTO comment_authors (channel_id, handle, avatar_url, updated_at, first_seen_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(channel_id) DO UPDATE SET
  handle        = excluded.handle,
  avatar_url    = excluded.avatar_url,
  updated_at    = excluded.updated_at,
  first_seen_at = CASE
    WHEN comment_authors.first_seen_at IS NULL THEN excluded.first_seen_at
    WHEN excluded.first_seen_at IS NULL THEN comment_authors.first_seen_at
    WHEN excluded.first_seen_at < comment_authors.first_seen_at THEN excluded.first_seen_at
    ELSE comment_authors.first_seen_at
  END
"""


def _note_author(sightings: dict, snippet: dict, now_epoch: int) -> None:
    """snippetから著者情報を拾いsightingsに記録する(削除済み・channel_id欠如はスキップ)。

    first_seen_at候補として、このsnippet自身のpublished_at(=このコメントの投稿時刻、
    syncが動いたnow_epochではない)も記録する。同一著者を同じ実行内で複数回観測した
    場合はここでMINを取って「このバッチで見えた中での最古」に絞り、Turso側のUPSERTでも
    既存値とのMINを取る(comment_authors.first_seen_atのON CONFLICT参照)ため、
    バックフィル済みの真の初コメント時刻を上書きすることはない。
    """
    if is_deleted_sentinel(snippet):
        return
    cid = snippet.get("authorChannelId", {}).get("value")
    if not cid:
        return
    published_at = parse_epoch(snippet["publishedAt"]) if snippet.get("publishedAt") else None
    prev = sightings.get(cid)
    prev_first_seen = prev.get("first_seen_at") if prev else None
    if prev_first_seen is not None and (published_at is None or prev_first_seen < published_at):
        first_seen_at = prev_first_seen
    else:
        first_seen_at = published_at
    sightings[cid] = {
        "handle": snippet.get("authorDisplayName") or "",
        "avatar_url": snippet.get("authorProfileImageUrl") or "",
        "updated_at": now_epoch,
        "first_seen_at": first_seen_at,
    }


def upsert_author_sightings(client: TursoClient, sightings: dict) -> None:
    if not sightings:
        return
    client.execute(_AUTHORS_TABLE_SQL)
    client.execute(_AUTHORS_FIRST_SEEN_INDEX_SQL)
    stmts = [
        {"sql": _UPSERT_AUTHOR_SQL,
         "args": [cid, s["handle"], s["avatar_url"], s["updated_at"], s.get("first_seen_at")]}
        for cid, s in sightings.items()
    ]
    for i in range(0, len(stmts), BATCH_SIZE):
        client.batch(stmts[i : i + BATCH_SIZE])


# ------------------------------------------------------------------ #
# 固定コメントの自動追従（2026-07-28）
#
# YouTube API には isPinned 相当のフィールドが無く(公式ドキュメント確認済み、
# 上のis_pinned列の注記参照)、固定コメントを直接問い合わせる手段が無い。
# ただし sync_new_comments() は commentThreads.list(order="time") が
# 「固定コメントを実際の投稿時刻に関係なく常に先頭に返す」性質を利用して、
# 1ページ目でまだ新着を1件も見ていない状態で出てきた古いアイテムを
# 固定コメントとみなしてスキップする処理を毎分必ず通る(この動画には常に
# 固定コメントが1件存在するため)。つまり「今どのコメントが固定されているか」
# は追加のAPI呼び出し無しで毎分ここで分かる。これを Turso に1行だけ状態保存
# しておき、前回と異なればその場で is_pinned を付け替える。
#
# pinned_comment_state を都度読むのは1行のPK読み(cheap)だけで済むため、
# 「変化が無ければ何もしない」を毎分1440回繰り返しても実質コストはゼロに近い。
# 実際に変化した(=固定コメントが変わった)回だけ、新しい方への UPSERT と
# 古い方の is_pinned=0 更新(comment_id指定の1行更新)が走る。
# ------------------------------------------------------------------ #

_PINNED_STATE_SQL = """
CREATE TABLE IF NOT EXISTS pinned_comment_state (
  id INTEGER PRIMARY KEY CHECK(id=1),
  comment_id TEXT NOT NULL,
  updated_at INTEGER NOT NULL
)
"""


def _reconcile_pinned_comment(
    client: TursoClient, tid: str, top_snip: dict, now_epoch: int,
) -> None:
    client.execute(_PINNED_STATE_SQL)
    rows = client.query("SELECT comment_id FROM pinned_comment_state WHERE id = 1")
    known_pinned_id = rows[0]["comment_id"] if rows else None

    if known_pinned_id == tid:
        return  # 変化なし。ここが毎分の通常ケース。

    print(f"  固定コメント変更を検知: {known_pinned_id} → {tid}", flush=True)

    deleted = is_deleted_sentinel(top_snip)
    pub = parse_epoch(top_snip["publishedAt"])
    upsert_rows(client, [{
        "comment_id": tid,
        "parent_id": None,
        "reply_order": None,
        "thread_published_at": pub,
        "author_channel_id": top_snip.get("authorChannelId", {}).get("value"),
        "handle": top_snip.get("authorDisplayName") if not deleted else None,
        "text": top_snip.get("textDisplay") if not deleted else None,
        "original_text": None,
        "published_at": pub,
        "like_count": None if deleted else int(top_snip.get("likeCount", 0)),
        "is_pinned": 1,
        "is_deleted": 1 if deleted else 0,
        "deleted_confirmed_at": now_epoch if deleted else None,
        "fetched_at": now_epoch,
    }])

    if known_pinned_id:
        # comment_id指定(PK)の1行更新なのでフルスキャンにはならない。
        client.execute(
            "UPDATE comments SET is_pinned = 0 WHERE comment_id = ?", [known_pinned_id],
        )

    client.execute(
        "INSERT INTO pinned_comment_state (id, comment_id, updated_at) VALUES (1, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET comment_id = excluded.comment_id, updated_at = excluded.updated_at",
        [tid, now_epoch],
    )


def fetch_all_replies(
    youtube,
    thread_id: str,
    thread_pub: int,
    included_replies: list,
    total_reply_count: int,
    now_epoch: int,
    sightings: dict | None = None,
) -> tuple:
    """戻り値: (youtube, rows)。

    youtube を返り値に含めるのは、内部でキーローテーションが起きた場合に呼び出し元
    (sync_new_comments)がその後のスレッドでも更新済みの youtube を使うため
    (2026-07-29修正。以前は関数ローカルの再代入が呼び出し元に伝播せず、この関数の中で
    ローテーションしたキーを次のスレッドの呼び出しでは使わずに済ませてしまい、
    枯渇済みキーへの無駄打ちで枯渇判定が余分に進み、本来無傷なはずの
    次のキーまで無駄に読み飛ばすバグがあった。_pass1_reply_counts と同じ形に揃えた)。
    """
    if total_reply_count <= len(included_replies):
        return youtube, []

    rows = []
    seen_ids = {r["id"] for r in included_replies}
    order = len(included_replies) + 1
    next_page = None
    rate_limit_retries = 0

    while True:
        try:
            resp = youtube.comments().list(
                part="snippet",
                parentId=thread_id,
                maxResults=100,
                pageToken=next_page,
                textFormat="plainText",
            ).execute()
        except HttpError as e:
            youtube, rate_limit_retries, should_raise, exhausted = _handle_api_error(
                e, youtube, rate_limit_retries,
            )
            if should_raise:
                raise
            if exhausted:
                return youtube, rows
            continue

        for r in resp.get("items", []):
            if r["id"] in seen_ids:
                continue
            rs = r["snippet"]
            deleted = is_deleted_sentinel(rs)
            if sightings is not None:
                _note_author(sightings, rs, now_epoch)
            rows.append({
                "comment_id": r["id"],
                "parent_id": thread_id,
                "reply_order": order,
                "thread_published_at": thread_pub,
                "author_channel_id": rs.get("authorChannelId", {}).get("value"),
                "handle": rs.get("authorDisplayName") if not deleted else None,
                "text": rs.get("textDisplay") if not deleted else None,
                "original_text": None,
                "published_at": parse_epoch(rs["publishedAt"]),
                "like_count": None if deleted else int(rs.get("likeCount", 0)),
                "is_pinned": 0,
                "is_deleted": 1 if deleted else 0,
                "deleted_confirmed_at": now_epoch if deleted else None,
                "fetched_at": now_epoch,
            })
            seen_ids.add(r["id"])
            order += 1

        next_page = resp.get("nextPageToken")
        if not next_page:
            break

    return youtube, rows


# ------------------------------------------------------------------ #
# 新着同期（毎分）
# ------------------------------------------------------------------ #

MAX_PAGES = 30  # 安全弁: 毎分実行で30ページ(3000スレッド)を超える新着はあり得ない

# 安全弁(2026-07-29追加): 固定コメントは常に1件だけの想定(この動画では実際に1件)。
# 1回の実行で2回目の「固定コメント候補」に遭遇した場合、それは本物の固定コメント変更
# ではなく stop_pub アンカー(get_latest_thread_pub)が壊れている兆候とみなす——
# DB上「最新」とされるスレッドがYouTube側で削除されると、そのスレッドの published_at
# に一致する item が二度と応答に現れず、1ページ目のitem全てが「新着未検出のまま
# stop_pub以下」の状態に落ち、固定コメント判定が誤って連鎖してis_pinnedの付け替えを
# 繰り返す事故があった。get_latest_thread_pub側にis_deleted=0を足したことで巡回チェックが
# 削除を検知した後は自己修復するが、検知が遅れている間の被害はこのキャップで頭打ちにする。
MAX_PINNED_RECONCILES_PER_RUN = 1


def sync_new_comments(client: TursoClient) -> int:
    stop_pub = get_latest_thread_pub(client)
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    youtube = get_youtube()
    next_page_token = None
    pending: list[dict] = []
    sightings: dict = {}
    inserted = 0
    found_stop = False
    found_in_window = False  # 固定コメント(2024年投稿)が先頭に来る対策
    pages = 0
    rate_limit_retries = 0
    pinned_reconciles = 0

    while True:
        try:
            resp = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=VIDEO_ID,
                maxResults=100,
                pageToken=next_page_token,
                order="time",
                textFormat="plainText",
            ).execute()
        except HttpError as e:
            youtube, rate_limit_retries, should_raise, exhausted = _handle_api_error(
                e, youtube, rate_limit_retries,
            )
            if should_raise:
                raise
            if exhausted:
                break
            continue

        pages += 1

        for item in resp.get("items", []):
            top_snip = item["snippet"]["topLevelComment"]["snippet"]
            tid = item["snippet"]["topLevelComment"]["id"]
            pub = parse_epoch(top_snip["publishedAt"])

            if stop_pub is not None and pub <= stop_pub:
                if pub == stop_pub or found_in_window or pages > 1:
                    # 既知の最新スレッド、またはそれより古いスレッドに到達
                    # → 以降は既知データなので即座に打ち切り（再取得・再書込みしない）
                    found_stop = True
                    break
                else:
                    # 1ページ目でまだ新着を1件も見ていない → 固定コメント
                    if pinned_reconciles >= MAX_PINNED_RECONCILES_PER_RUN:
                        print(
                            f"  WARNING: 固定コメント候補が1回の実行で{pinned_reconciles + 1}件目に"
                            f"到達。stop_pub({stop_pub})のアンカーとなるスレッドが削除された"
                            f"疑いがあるため、誤ったis_pinned連鎖書き換えを避けてこの回の新着同期を"
                            f"打ち切る(次回以降、削除検知が確定すれば自己修復する)。", flush=True,
                        )
                        found_stop = True
                        break
                    _reconcile_pinned_comment(client, tid, top_snip, now_epoch)
                    pinned_reconciles += 1
                    continue
            else:
                found_in_window = True

            deleted = is_deleted_sentinel(top_snip)
            thread_pub = pub
            _note_author(sightings, top_snip, now_epoch)

            pending.append({
                "comment_id": tid,
                "parent_id": None,
                "reply_order": None,
                "thread_published_at": thread_pub,
                "author_channel_id": top_snip.get("authorChannelId", {}).get("value"),
                "handle": top_snip.get("authorDisplayName") if not deleted else None,
                "text": top_snip.get("textDisplay") if not deleted else None,
                "original_text": None,
                "published_at": pub,
                "like_count": None if deleted else int(top_snip.get("likeCount", 0)),
                "is_pinned": 1 if top_snip.get("isPinned") else 0,
                "is_deleted": 1 if deleted else 0,
                "deleted_confirmed_at": now_epoch if deleted else None,
                "fetched_at": now_epoch,
            })

            inline_replies = item.get("replies", {}).get("comments", [])
            for order, r in enumerate(inline_replies, 1):
                rs = r["snippet"]
                r_del = is_deleted_sentinel(rs)
                _note_author(sightings, rs, now_epoch)
                pending.append({
                    "comment_id": r["id"],
                    "parent_id": tid,
                    "reply_order": order,
                    "thread_published_at": thread_pub,
                    "author_channel_id": rs.get("authorChannelId", {}).get("value"),
                    "handle": rs.get("authorDisplayName") if not r_del else None,
                    "text": rs.get("textDisplay") if not r_del else None,
                    "original_text": None,
                    "published_at": parse_epoch(rs["publishedAt"]),
                    "like_count": None if r_del else int(rs.get("likeCount", 0)),
                    "is_pinned": 0,
                    "is_deleted": 1 if r_del else 0,
                    "deleted_confirmed_at": now_epoch if r_del else None,
                    "fetched_at": now_epoch,
                })

            # ここに到達するのは新着スレッドのみ（既知に達したら上で break 済み）。
            # fetch_all_replies が内部でキーローテーションした場合に備え、更新後の
            # youtube を必ず受け取って以降のスレッドに引き継ぐ(2026-07-29修正)。
            youtube, extra = fetch_all_replies(
                youtube, tid, thread_pub, inline_replies,
                item["snippet"]["totalReplyCount"], now_epoch, sightings,
            )
            pending.extend(extra)

        if len(pending) >= BATCH_SIZE:
            upsert_rows(client, pending)
            inserted += len(pending)
            pending = []

        if found_stop:
            break

        if pages >= MAX_PAGES:
            print(f"  WARNING: {MAX_PAGES}ページに達したため打ち切り（停止条件に到達せず）", flush=True)
            break

        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break

    upsert_rows(client, pending)
    upsert_author_sightings(client, sightings)
    return inserted + len(pending)


# ------------------------------------------------------------------ #
# 削除マーキング共通処理
# ------------------------------------------------------------------ #

def _mark_deleted(client: TursoClient, comment_ids: list[str], now_epoch: int) -> int:
    if not comment_ids:
        return 0
    stmts = [
        {
            "sql": """
                UPDATE comments
                SET is_deleted = 1,
                    deleted_confirmed_at = ?,
                    fetched_at = ?
                WHERE comment_id = ?
                  AND deleted_confirmed_at IS NULL
            """,
            "args": [now_epoch, now_epoch, cid],
        }
        for cid in comment_ids
    ]
    for i in range(0, len(stmts), BATCH_SIZE):
        client.batch(stmts[i : i + BATCH_SIZE])
    return len(comment_ids)


# ------------------------------------------------------------------ #
# 既存スレッドの返信再同期 + 削除検知（Pass 2 = 高い取得）
#
# 削除は「前回取得できていたスレッド/返信が、再取得したら消えている」
# ことでしか判定できない（生のYouTube APIレスポンスに削除済みを示す
# センチネル値は存在しない）。よってスレッド・返信それぞれについて
# 既知IDと再取得結果のIDを突き合わせ、消えたものだけ削除扱いにする。
#
# 1スレッド = 最低1 unit と高いので、run_reply_recheck_batch() の Pass 1 で
# 「YouTube側の totalReplyCount と Turso側の既知返信数が食い違う」と
# 判明したスレッドにだけ適用する。
# ------------------------------------------------------------------ #

def _resync_thread_replies(
    youtube, client: TursoClient, tid: str, thread_pub: int, now_epoch: int, sightings: dict,
) -> tuple:
    """スレッド1件ぶんの返信を全件再取得し、消えた返信を削除扱いにする。

    戻り値: (youtube, 書き込んだ返信件数, 削除検知した件数, クォータ枯渇で中断したか)

    youtube を返り値に含める理由は fetch_all_replies と同じ(2026-07-29修正)。
    以前は _recheck_threads の for ループが複数スレッドを処理する間、内部で起きた
    キーローテーションが呼び出し元へ伝播せず、2スレッド目以降も枯渇済みキーを
    使い続けてしまうバグがあった。
    """
    next_page = None
    order = 1
    pending: list[dict] = []
    fetched_ids: set[str] = set()
    rate_limit_retries = 0

    while True:
        try:
            resp = youtube.comments().list(
                part="snippet",
                parentId=tid,
                maxResults=100,
                pageToken=next_page,
                textFormat="plainText",
            ).execute()
        except HttpError as e:
            youtube, rate_limit_retries, should_raise, exhausted = _handle_api_error(
                e, youtube, rate_limit_retries,
            )
            if should_raise:
                raise
            if exhausted:
                upsert_rows(client, pending)
                return youtube, len(pending), 0, True
            continue

        for r in resp.get("items", []):
            fetched_ids.add(r["id"])
            rs = r["snippet"]
            _note_author(sightings, rs, now_epoch)
            pending.append({
                "comment_id": r["id"],
                "parent_id": tid,
                "reply_order": order,
                "thread_published_at": thread_pub,
                "author_channel_id": rs.get("authorChannelId", {}).get("value"),
                "handle": rs.get("authorDisplayName"),
                "text": rs.get("textDisplay"),
                "original_text": None,
                "published_at": parse_epoch(rs["publishedAt"]),
                "like_count": int(rs.get("likeCount", 0)),
                "is_pinned": 0,
                "is_deleted": 0,
                "deleted_confirmed_at": None,
                "fetched_at": now_epoch,
            })
            order += 1

        next_page = resp.get("nextPageToken")
        if not next_page:
            break

    upsert_rows(client, pending)

    known = client.query(
        "SELECT comment_id FROM comments WHERE parent_id = ? AND is_deleted = 0",
        [tid],
    )
    known_ids = {r["comment_id"] for r in known}
    deleted = _mark_deleted(client, list(known_ids - fetched_ids), now_epoch)
    return youtube, len(pending), deleted, False


# ------------------------------------------------------------------ #
# スレッド巡回チェック（Pass 1 = 安い探索、毎10分）
#
# 「既存スレッドに後から付いた返信」と「返信/スレッドの削除」を検知する
# 唯一の経路。sync_new_comments は order="time"(スレッド作成時刻の降順。
# 新しい返信が付いてもスレッドの並び順は上がらない)で走査して既知スレッドに
# 到達した時点で break するため、5分前に立ったスレッドへの返信ですら拾わない。
#
# commentThreads.list(id=50件) で totalReplyCount と生存確認を同時に安く取り
# (50件=1 unit)、Turso 側の既知返信数(is_deleted=0のCOUNT)と食い違う
# スレッドだけ _resync_thread_replies で完全再取得する
# (comments_db の refresh_replies_local.py と同じ Pass1/Pass2 の二段構え)。
#
# 対象は2階建て:
#
#   ホット層 : 直近 HOT_WINDOW_HOURS 時間のスレッド全件、HOT_INTERVAL_MIN 分おき。
#              下流の再計算ウィンドウ(comments_hourly=6時間、
#              Supabase daily_stats=2日)が閉じる前に返信を Turso へ入れるための層。
#              ここが遅れると集計から返信が恒久的に欠落する(単なる遅延では済まない)。
#   コールド層: published_at 昇順のカーソルで全履歴を少しずつ巡回。末尾まで
#              行ったら先頭へ巻き戻して永久に一周し続ける
#              (reply_recheck_state に1行だけ状態を持つ)。
#              RECHECK_BATCH_SIZE=500 × 10分おき(1日144回)で約144万スレッドを
#              約20日で一周する。ホット層が見ない古いスレッドの保険。
#
# 2026-07-28 以前はこれとは別に sync_recent_replies() が直近3時間のスレッドを
# 30分おきに「1スレッド=1 unit で全件フル再取得」していた。同じ目的(返信の
# 追加・削除検知)に対して Pass1 を使わない約50倍高い方法で、1日約11,700 units
# = YouTube API 消費全体の約8割を占めていたため廃止し、その役割をホット層に
# 統合した。ホット層の窓は24時間なので、旧実装の3時間窓では拾えず恒久欠落して
# いた「3時間より古いスレッドへの返信」も同時に塞がっている。
# ------------------------------------------------------------------ #

RECHECK_BATCH_SIZE = 500
RECHECK_INTERVAL_MIN = 10
RECHECK_ID_CHUNK = 50    # commentThreads.list の id= に渡す件数(下の _MAX_DEAD_RATIO 注記も参照)
RECHECK_IN_CHUNK = 200   # Turso IN() 節のチャンクサイズ(ranking_updaterの_IN_CHUNKと同じ考え方)

HOT_WINDOW_HOURS = 24    # ホット層が見るスレッドの範囲(comments_hourly の6時間窓に確実に間に合わせる)
HOT_INTERVAL_MIN = 30    # ホット層を回す間隔。RECHECK_INTERVAL_MIN の倍数であること

# バースト対策の安全弁(2026-07-28、年次バーストイベントのような突発的なコメント急増を想定して追加)。
#
# ホット層は「直近24時間の全スレッド」を無条件で毎回 Pass1 にかける設計だったため、
# その24時間にコメント数が急増すると Pass1 の units・Pass2 の実行時間が
# スレッド数にほぼ比例して膨らみ、GitHub Actions の timeout-minutes: 10 を
# 超えて強制終了されたり、1日のクォータを1回の巡回チェックで使い切る恐れがある。
#
# HOT_BATCH_CAP: ホット層1回で Pass1 にかけるスレッド数の上限。新しい順
#   (published_at DESC)に上位 HOT_BATCH_CAP 件だけを対象にする。溢れた分
#   (直近24h以内だが新しい方から数えて上限より外側)はこの回はスキップされ、
#   次回以降のホット層実行(バーストが収まれば拾える)・24h窓を出た後は
#   コールド層(全履歴を約20日で巡回)・月次スイープ(直近31日を毎月一括確認)
#   が最終的な保険になる。「スレッドを失う」わけではなく「返信・削除の検知が
#   遅れる」だけ(スレッド自体の発見は sync_new_comments が別途・自己修復的に
#   保証しているため無関係)。
#   通常時の直近24時間は約2,000件程度(実測)なので、この上限は通常運用では
#   一切効かない。
#
# PASS2_RUN_CAP: run_reply_recheck_batch 1回で完全再取得(Pass2)するスレッド数の
#   上限。Pass1 の入力を絞ってもバースト中は食い違いスレッド数自体が多くなり
#   得るため、Pass2(1スレッド=最低1リクエスト、逐次実行)がジョブ実行時間を
#   支配しないよう別途キャップする。溢れた食い違いはこの回では書き込まれない
#   =次回の Pass1 でも同じ食い違いとして再検知されるので、恒久的に見逃されは
#   しない。月次スイープ(monthly_sweep.py)はこのキャップを使わない
#   (pass2_cap=None のまま) — 独自の TIME_BUDGET_SEC + チャンク分割で
#   実行時間を管理しており、Pass2 を人為的に絞ると「毎月一括で確実に整合性を
#   取る」という役割そのものが弱まってしまうため。
HOT_BATCH_CAP = 5000
PASS2_RUN_CAP = 500

# 誤削除に対する安全弁。
#
# Pass1 は「id= で問い合わせたのにレスポンスに含まれなかった → 削除された」と
# 判定し、スレッド本体とぶら下がる返信を一括で is_deleted=1 にする。つまり
# APIが何らかの理由でリクエストしたIDの一部を黙って返さなかった場合
# (未文書の id= 上限、仕様変更、部分障害など)、生きているスレッドが大量に
# 削除扱いされて実データが壊れる。
#
# RECHECK_ID_CHUNK を 50 から増やす場合は特に危険で、必ず事前に
# 「N件渡して items が N件返るか」を実測してから変更すること
# (ドキュメントに id= の上限は明記されておらず、maxResults は id= との
#  併用がサポートされないと明記されているため、上限は実測でしか分からない)。
#
# ここでは保険として、1バッチ中の消滅率が異常に高い場合は削除マーキングだけを
# スキップする(Pass2 は通常通り走らせる)。取りこぼしは次の巡回で拾えるが、
# 誤削除は自動では戻せないため、迷ったら消さない側に倒す。
_MAX_DEAD_RATIO = 0.5
_MIN_BATCH_FOR_DEAD_RATIO_GUARD = 50

_RECHECK_STATE_SQL = """
CREATE TABLE IF NOT EXISTS reply_recheck_state (
  id INTEGER PRIMARY KEY CHECK(id=1),
  cursor_published_at INTEGER NOT NULL,
  cycle_count INTEGER NOT NULL,
  cycle_started_at INTEGER NOT NULL,
  threads_checked_in_cycle INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
)
"""


def _read_recheck_state(client: TursoClient, now_epoch: int) -> dict:
    client.execute(_RECHECK_STATE_SQL)
    rows = client.query("SELECT * FROM reply_recheck_state WHERE id = 1")
    if rows:
        return rows[0]
    return {
        "cursor_published_at": 0,
        "cycle_count": 0,
        "cycle_started_at": now_epoch,
        "threads_checked_in_cycle": 0,
    }


def _write_recheck_state(client: TursoClient, state: dict, now_epoch: int) -> None:
    client.execute(
        "INSERT INTO reply_recheck_state "
        "(id, cursor_published_at, cycle_count, cycle_started_at, threads_checked_in_cycle, updated_at) "
        "VALUES (1, ?, ?, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET "
        "cursor_published_at = excluded.cursor_published_at, "
        "cycle_count = excluded.cycle_count, "
        "cycle_started_at = excluded.cycle_started_at, "
        "threads_checked_in_cycle = excluded.threads_checked_in_cycle, "
        "updated_at = excluded.updated_at",
        [state["cursor_published_at"], state["cycle_count"], state["cycle_started_at"],
         state["threads_checked_in_cycle"], now_epoch],
    )


def _next_recheck_batch(client: TursoClient, cursor: int) -> tuple[list[dict], bool]:
    """カーソルより新しいスレッドを古い順にBATCH_SIZE件取る。末尾に達したら先頭から巻き戻して補充する。"""
    rows = client.query(
        "SELECT comment_id, published_at FROM comments "
        "WHERE parent_id IS NULL AND published_at > ? "
        "ORDER BY published_at ASC LIMIT ?",
        [cursor, RECHECK_BATCH_SIZE],
    )
    wrapped = False
    if len(rows) < RECHECK_BATCH_SIZE:
        wrapped = True
        remaining = RECHECK_BATCH_SIZE - len(rows)
        seen_ids = {r["comment_id"] for r in rows}
        more = client.query(
            "SELECT comment_id, published_at FROM comments "
            "WHERE parent_id IS NULL AND published_at <= ? "
            "ORDER BY published_at ASC LIMIT ?",
            [cursor, remaining + len(seen_ids)],
        )
        for r in more:
            if r["comment_id"] not in seen_ids:
                rows.append(r)
                if len(rows) >= RECHECK_BATCH_SIZE:
                    break
    return rows, wrapped


def _hot_window_threads(client: TursoClient, now_epoch: int) -> list[dict]:
    """直近 HOT_WINDOW_HOURS 時間に立ったスレッドを新しい順に最大 HOT_BATCH_CAP 件返す
    (idx_parent_published の SEARCH)。

    バースト時は HOT_BATCH_CAP で切り詰められる。新しい順に取るのは、直近の
    スレッドほど今まさに返信が付いている最中の可能性が高く優先度が高いため。
    切り詰められた古い方(直近24h以内だが上限に入らなかった分)は、次回の
    ホット層実行かコールド層/月次スイープに委ねる(HOT_BATCH_CAP の注記参照)。
    """
    cutoff = now_epoch - HOT_WINDOW_HOURS * 3600
    return client.query(
        "SELECT comment_id, published_at FROM comments "
        "WHERE parent_id IS NULL AND published_at >= ? "
        "ORDER BY published_at DESC LIMIT ?",
        [cutoff, HOT_BATCH_CAP],
    )


def _pass1_reply_counts(youtube, thread_ids: list[str]):
    """Pass 1: commentThreads.list(id=) でまとめて生存確認と totalReplyCount を取る。

    戻り値: (次に使う youtube クライアント, 生存スレッドID, {スレッドID: totalReplyCount},
             クォータ枯渇, 切り詰め検知)

    youtube を返り値に含めるのは、内部でキーローテーションが起きた場合に
    呼び出し側(_recheck_threads)がその後の Pass2 で使う youtube を確実に
    更新済みのものにするため(以前は関数ローカルの再代入が呼び出し元に
    伝播せず、Pass1でローテーション済みのキーをPass2で使わずに古いキーへ
    もう一度当ててしまい、枯渇判定が二重に進んで本来
    無傷なはずの次のキーまで無駄に読み飛ばすバグがあった)。

    切り詰め検知: レスポンスに nextPageToken があれば、渡したIDリストが
    ページ分割されている = 1回では全件返ってきていない。この状態で
    「返ってこなかった = 削除された」と判定すると生きているスレッドを
    大量に誤削除するため、呼び出し側で中断させる(_MAX_DEAD_RATIO の注記参照)。
    """
    alive_ids: set[str] = set()
    reply_counts: dict[str, int] = {}
    rate_limit_retries = 0

    for i in range(0, len(thread_ids), RECHECK_ID_CHUNK):
        chunk = thread_ids[i:i + RECHECK_ID_CHUNK]
        while True:
            try:
                resp = youtube.commentThreads().list(
                    part="snippet",
                    id=",".join(chunk),
                    textFormat="plainText",
                    maxResults=50,
                ).execute()
                break
            except HttpError as e:
                youtube, rate_limit_retries, should_raise, exhausted = _handle_api_error(
                    e, youtube, rate_limit_retries,
                )
                if should_raise:
                    raise
                if exhausted:
                    return youtube, alive_ids, reply_counts, True, False
                continue

        if resp.get("nextPageToken"):
            print(
                f"  ERROR: Pass1 のレスポンスが分割されている（{len(chunk)}件要求）。"
                f"削除の誤検知を避けるため中断する。RECHECK_ID_CHUNK を見直すこと",
                flush=True,
            )
            return youtube, alive_ids, reply_counts, False, True

        for item in resp.get("items", []):
            tid = item["id"]
            alive_ids.add(tid)
            reply_counts[tid] = item["snippet"].get("totalReplyCount", 0)

    return youtube, alive_ids, reply_counts, False, False


def _known_reply_counts(client: TursoClient, thread_ids: list[str]) -> dict[str, int]:
    """Turso 側の既知返信数(is_deleted=0)をまとめて取得する。"""
    known_counts: dict[str, int] = {}
    for i in range(0, len(thread_ids), RECHECK_IN_CHUNK):
        chunk = thread_ids[i:i + RECHECK_IN_CHUNK]
        placeholders = ",".join("?" for _ in chunk)
        rows = client.query(
            f"SELECT parent_id, COUNT(*) AS c FROM comments "
            f"WHERE parent_id IN ({placeholders}) AND is_deleted = 0 GROUP BY parent_id",
            chunk,
        )
        for r in rows:
            known_counts[r["parent_id"]] = r["c"]
    return known_counts


def _recheck_threads(
    client: TursoClient, youtube, threads: list[dict], label: str,
    pass2_cap: int | None = None,
) -> tuple[int, int, int, bool, int]:
    """スレッド群に Pass1/Pass2 を適用する。巡回チェックと月次スイープの共通処理。

    pass2_cap: Pass2(完全再取得)を実施するスレッド数の上限。None なら無制限
    (月次スイープはこちら)。バースト時に食い違いスレッドが大量発生しても
    1回の呼び出しの実行時間を有界にするための安全弁(HOT_BATCH_CAP/PASS2_RUN_CAP
    の注記参照)。上限を超えた食い違いは今回書き込まれないだけで、次回の
    Pass1 でも同じ食い違いとして再検知される(恒久的な見逃しにはならない)。

    戻り値: (書き込んだ返信件数, 消滅スレッド数, 食い違いスレッド数, 中断したか, Pass2で先送りした件数)
    「中断したか」が True のときは Pass1 が完走していないので、
    呼び出し側はカーソルを進めてはいけない。
    """
    if not threads:
        return 0, 0, 0, False, 0

    thread_pub_map = {t["comment_id"]: t["published_at"] for t in threads}
    checked_ids = [t["comment_id"] for t in threads]

    # --- Pass 1: 安い探索 ---
    youtube, alive_ids, reply_counts, quota_exhausted, truncated = _pass1_reply_counts(youtube, checked_ids)
    if quota_exhausted:
        print(f"  {label}をスキップ（クォータ枯渇）", flush=True)
        return 0, 0, 0, True, 0
    if truncated:
        return 0, 0, 0, True, 0

    known_counts = _known_reply_counts(client, checked_ids)
    now_epoch = int(datetime.now(timezone.utc).timestamp())

    # --- スレッド自体が消えた分は返信ごと削除扱いに(APIを叩かず既知IDを流用) ---
    dead_ids = set(checked_ids) - alive_ids
    dead_ratio = len(dead_ids) / len(checked_ids)
    if (len(checked_ids) >= _MIN_BATCH_FOR_DEAD_RATIO_GUARD
            and dead_ratio > _MAX_DEAD_RATIO):
        # 実データ破損より取りこぼしを選ぶ。次の巡回で拾い直せる。
        print(
            f"  WARNING: {label}: {len(checked_ids)}件中{len(dead_ids)}件"
            f"（{dead_ratio:.0%}）が消滅と判定された。異常値のため削除マーキングを"
            f"スキップする（Pass2 は続行）",
            flush=True,
        )
        dead_ids = set()
    else:
        _mark_deleted(client, list(dead_ids), now_epoch)
        for tid in dead_ids:
            known = client.query(
                "SELECT comment_id FROM comments WHERE parent_id = ? AND is_deleted = 0", [tid],
            )
            _mark_deleted(client, [r["comment_id"] for r in known], now_epoch)

    # --- 返信数が食い違うスレッドだけ完全再取得(Pass 2) ---
    mismatched = [
        tid for tid in alive_ids
        if reply_counts.get(tid, 0) != known_counts.get(tid, 0)
    ]

    deferred_count = 0
    to_resync = mismatched
    if pass2_cap is not None and len(mismatched) > pass2_cap:
        # 新しい順に優先して実行時間を有界にする。溢れた分は次回の Pass1 で
        # 再検知される(書き込まれていないので食い違いが残ったまま)。
        mismatched.sort(key=lambda tid: thread_pub_map[tid], reverse=True)
        to_resync = mismatched[:pass2_cap]
        deferred_count = len(mismatched) - pass2_cap
        print(
            f"  WARNING: {label}: 食い違い{len(mismatched)}件が上限{pass2_cap}件を超過。"
            f"新しい{pass2_cap}件だけ Pass2 を実行し、残り{deferred_count}件は次回に先送りする",
            flush=True,
        )

    sightings: dict = {}
    written = 0
    pass2_exhausted = False
    for tid in to_resync:
        # 更新後の youtube を必ず受け取り、次のスレッドへ引き継ぐ(2026-07-29修正。
        # 関数docstring参照)。
        youtube, n, _deleted, exhausted = _resync_thread_replies(
            youtube, client, tid, thread_pub_map[tid], now_epoch, sightings,
        )
        written += n
        if exhausted:
            pass2_exhausted = True
            break
    upsert_author_sightings(client, sightings)

    return written, len(dead_ids), len(mismatched), pass2_exhausted, deferred_count


def run_reply_recheck_batch(client: TursoClient, include_hot: bool) -> int:
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    state = _read_recheck_state(client, now_epoch)
    cold_batch, wrapped = _next_recheck_batch(client, state["cursor_published_at"])

    # ホット層。コールド層のカーソルが末尾付近にいると対象が重なるので、
    # 同じスレッドに2回 Pass1 を使わないよう重複を落とす。
    hot_batch: list[dict] = []
    hot_capped = False
    if include_hot:
        raw_hot = _hot_window_threads(client, now_epoch)
        hot_capped = len(raw_hot) >= HOT_BATCH_CAP  # ちょうど一致=ほぼ確実に切り詰められた合図
        cold_ids = {b["comment_id"] for b in cold_batch}
        hot_batch = [t for t in raw_hot if t["comment_id"] not in cold_ids]
        if hot_capped:
            print(
                f"  WARNING: ホット層が上限{HOT_BATCH_CAP}件に達した（バースト検知）。"
                f"直近{HOT_WINDOW_HOURS}時間のうち新しい{HOT_BATCH_CAP}件のみ処理し、"
                f"残りは次回以降のホット層・コールド層・月次スイープに委ねる",
                flush=True,
            )

    batch = cold_batch + hot_batch
    if not batch:
        return 0

    youtube = get_youtube()
    written, dead_count, mismatch_count, aborted, deferred_pass2 = _recheck_threads(
        client, youtube, batch, "スレッド巡回チェック", pass2_cap=PASS2_RUN_CAP,
    )
    if aborted:
        # 状態は進めず、次回同じカーソル位置からやり直す
        return 0

    # --- カーソル更新 ---
    # ホット層は毎回同じ範囲を見直す層なのでカーソルに影響させない。
    # 進めるのはコールド層が実際に消化した分だけ。
    if cold_batch:
        if wrapped:
            state["cycle_count"] += 1
            state["cycle_started_at"] = now_epoch
            state["threads_checked_in_cycle"] = len(cold_batch)
            print(f"  スレッド巡回チェック: 1周完了 → 第{state['cycle_count']}周を開始", flush=True)
        else:
            state["threads_checked_in_cycle"] += len(cold_batch)
        state["cursor_published_at"] = cold_batch[-1]["published_at"]
        _write_recheck_state(client, state, now_epoch)

    print(
        f"  スレッド巡回チェック: {len(batch)}件確認"
        f"（巡回{len(cold_batch)}件 + 直近{HOT_WINDOW_HOURS}時間{len(hot_batch)}件）、"
        f"食い違い{mismatch_count}件（うち先送り{deferred_pass2}件）、"
        f"返信{written}件書込み、消滅{dead_count}件",
        flush=True,
    )
    return written


# ------------------------------------------------------------------ #
# 時間バケット更新（/comment-velocity の高コストな2日分スキャンを
# 廃止するため、1時間ごとの確定値を comments_hourly に貯めておく）
# ------------------------------------------------------------------ #

_UPSERT_HOURLY_SQL = """
    INSERT INTO comments_hourly (hour_start, comment_count, thread_count, reply_count, handles)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(hour_start) DO UPDATE SET
      comment_count = excluded.comment_count,
      thread_count  = excluded.thread_count,
      reply_count   = excluded.reply_count,
      handles       = excluded.handles
"""


def compute_hour_bucket(client: TursoClient, hour_start: int) -> dict:
    hour_end = hour_start + 3600
    rows = client.query(
        "SELECT parent_id, handle FROM comments "
        "WHERE is_deleted = 0 AND published_at >= ? AND published_at < ?",
        [hour_start, hour_end],
    )
    thread_count = sum(1 for r in rows if r["parent_id"] is None)
    handles: dict[str, int] = {}
    for r in rows:
        h = r["handle"]
        if h:
            handles[h] = handles.get(h, 0) + 1
    return {
        "comment_count": len(rows),
        "thread_count": thread_count,
        "reply_count": len(rows) - thread_count,
        "handles": handles,
    }


def update_hourly_buckets(client: TursoClient, hours_back: int = 1) -> int:
    """直近 hours_back 時間ぶんのバケットを再計算してUPSERTする。

    「確定した過去は二度とスキャンしない」ための土台。1時間だけの再計算は
    毎分実行しても数百行程度で軽い。10分おきに hours_back=6 で広めに
    再計算し、削除検知・返信backfillの遅延を吸収する（daily_statsの
    _REPROCESS_DAYS と同じ考え方）。
    """
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    current_hour_start = (now_epoch // 3600) * 3600
    for i in range(hours_back):
        hour_start = current_hour_start - i * 3600
        bucket = compute_hour_bucket(client, hour_start)
        client.execute(_UPSERT_HOURLY_SQL, [
            hour_start, bucket["comment_count"], bucket["thread_count"],
            bucket["reply_count"], json.dumps(bucket["handles"], ensure_ascii=False),
        ])
    return hours_back


def process_hourly_repair_queue(client: TursoClient, limit: int = 24) -> int:
    """Repair historical hour buckets recorded by the monthly sweep."""
    claims = repair_queue.load(client, "hourly_bucket", limit=limit)
    completed = {}
    for key, generation in claims.items():
        try:
            hour_start = int(key)
            if hour_start < 0 or hour_start % 3600:
                raise ValueError("invalid hour bucket")
            bucket = compute_hour_bucket(client, hour_start)
            client.execute(_UPSERT_HOURLY_SQL, [
                hour_start, bucket["comment_count"], bucket["thread_count"],
                bucket["reply_count"], json.dumps(bucket["handles"], ensure_ascii=False),
            ])
            completed[key] = generation
        except Exception as exc:
            repair_queue.fail(client, "hourly_bucket", [key], exc)
    repair_queue.complete(client, "hourly_bucket", completed)
    return len(completed)


# ------------------------------------------------------------------ #
# 時報コメントのリアルタイム反映（毎分同期に便乗、2026-07-29）
#
# 対象8時刻(time_comment_common.TARGET_TIMES)が実際に発生した後、
# TIME_COMMENT_CHECK_WINDOW_MIN 分間だけ、そのちょうど1分幅の published_at
# 範囲を毎分再チェックする。1回だけでなく複数回チェックするのは、YouTube側の
# コメント反映(commentThreads.list への反映)に数分の遅延があり得るため
# (hourly_archive.py の RETRY_BURST_MINUTES と同じ考え方)。
#
# 冪等性: 毎回「対象分の開始からの新着分だけ」ではなく、毎回同じ固定の1分幅を
# 丸ごと再クエリし、group_matches/resolve_winners を再実行してUPSERTする。
# これにより「1分目より2分目の方がより早い投稿を見つけた」場合も自動的に
# 上書き訂正される(ON CONFLICT DO UPDATE)。「最初に見つかった勝者で確定・
# 以降チェックしない」という早期終了は行わない — 毎分の再クエリは
# idx_comments_published経由のインデックス済み・狭いSEARCHなので安価。
#
# 日次バッチ(analytics_aggregates_updater/main.py)との役割分担: こちらは
# 「達成が確定した瞬間、ベストエフォートで即時反映する」担当。ウィンドウが
# 閉じた後にYouTube側の反映がさらに遅れた場合の最終的な正しさは、従来通り
# 翌日0:01JSTの日次バッチ(_REPROCESS_DAYS=2)が保証する。
#
# 「今日」にachieved=1を書くこと自体はtime_comment_dataの「今日はまだ行を
# 持たない」設計ルール違反ではない — そのルールが禁じているのは「まだ発生
# していない今日」に根拠のないachieved=0を書くこと。実際に達成が確定した
# 瞬間にachieved=1を書くのはこの機能そのものの目的であり、
# time_comment_common.upsert_achieved_only()はそもそもachieved=0を書く
# 経路を持たない。
# ------------------------------------------------------------------ #

TIME_COMMENT_CHECK_WINDOW_MIN = 5  # hourly_archive.pyのRETRY_BURST_MINUTES=5と同じ考え方


def _time_comment_targets_this_minute(now_jst: datetime) -> list[tuple[str, datetime]]:
    """今チェックすべき (time_key, その対象分の開始JST datetime) の一覧を返す。"""
    targets = []
    for time_key in time_comment_common.TARGET_TIMES:
        h, m = (int(x) for x in time_key.split(":"))
        target_start = now_jst.replace(hour=h, minute=m, second=0, microsecond=0)
        if target_start <= now_jst < target_start + timedelta(minutes=TIME_COMMENT_CHECK_WINDOW_MIN):
            targets.append((time_key, target_start))
    return targets


def check_time_comments(client: TursoClient, now_jst: datetime) -> set[str]:
    """対象時刻発生後の数分間だけ、その1分幅を再チェックして即時UPSERTする。

    achieved=0は絶対に書かない(該当コメントが見つからなければ何もしない)。
    戻り値: 今回実際に書き込んだ time_key の集合(呼び出し側のキャッシュクリア判定用)。
    """
    written: set[str] = set()
    for time_key, target_start in _time_comment_targets_this_minute(now_jst):
        start_epoch = int(target_start.astimezone(timezone.utc).timestamp())
        end_epoch = start_epoch + 60
        rows = client.query(
            "SELECT rowid, comment_id, author_channel_id, handle, published_at, text, parent_id "
            "FROM comments WHERE published_at >= ? AND published_at < ?",
            [start_epoch, end_epoch],
        )
        groups = time_comment_common.group_matches(rows)
        if not groups:
            continue
        winners, _unresolved = time_comment_common.resolve_winners(groups)
        time_comment_common.upsert_achieved_only(client, set(groups.keys()), winners)
        written.add(time_key)
        print(f"  時報コメント即時反映: {time_key} {target_start.date().isoformat()} 達成", flush=True)
    return written


def _clear_time_comment_cache(time_key: str) -> None:
    """flaskr の /time・/graph/time-comment の該当キャッシュを消す(ベストエフォート、
    minutely_record/hourly_archive.py の _clear_button_stats_cache() と同じパターン)。"""
    flask_base_url = os.getenv("FLASK_BASE_URL")
    api_token = os.getenv("API_TOKEN")
    if not (flask_base_url and api_token):
        return
    try:
        requests.post(
            f"{flask_base_url.rstrip('/')}/api/cache-clear-time-comment",
            params={"time": time_key},
            headers={"Authorization": f"Bearer {api_token}"},
            timeout=10,
        )
    except Exception as e:
        print(f"  time-commentキャッシュクリア失敗({time_key}): {e}")


# ------------------------------------------------------------------ #
# ksk(加速)スレッドのコマンド検出（T0、毎分同期に便乗）
#
# スレ主が `!ksk`(または `/ksk`)を含む親コメントを立てると、そのスレッドが /ksk ページの
# 追跡対象として登録される（オプトイン）。解除はスレ主本人が自分のスレッドへ
# `!ksk stop` と返信する。
#
# YouTube API は一切叩かない — 直近 DETECT_WINDOW_MIN 分の comments を
# idx_comments_published 経由で読むだけ。窓を毎分丸ごと再スキャンするのは、
# YouTube側のコメント反映に数分の遅延があり得るため（check_time_comments と同じ理由）。
# 登録は thread_id の PK 冪等なので、同じ窓を何度見ても副作用はない。
#
# 解除コマンドは返信として打たれるため sync_new_comments() では拾えない（あちらは
# 新規スレッドしか見ない）。この窓スキャンは親・返信を区別せず読むので、返信の
# 解除コマンドもここで 1分以内に検知できる（追加の読み取りコストはゼロ）。
# ------------------------------------------------------------------ #

def check_ksk_commands(client: TursoClient, now_epoch: int) -> int:
    """直近の窓から ksk コマンドを検出して登録・解除する。戻り値は状態が変わった件数。"""
    start_epoch, end_epoch = ksk_common.detect_window_bounds(now_epoch)
    rows = client.query(
        "SELECT comment_id, parent_id, author_channel_id, handle, published_at, text "
        "FROM comments WHERE published_at >= ? AND published_at < ? ORDER BY published_at",
        [start_epoch, end_epoch],
    )
    if not rows:
        return 0

    # コマンドを含む行が1件も無ければ、ここで打ち切って以降のクエリを一切投げない
    # （通常はこの分岐に入る。ksk は稀なイベントなので毎分の実コストはこの1クエリだけ）。
    candidates = []
    for row in rows:
        cmd = ksk_common.parse_command(row.get("text"))
        if cmd is not None:
            candidates.append((row, cmd))
    if not candidates:
        return 0

    # main() の先頭で既に無条件ensure済みだが、この関数を単体で呼んでも安全なように
    # ここでも呼ぶ(冪等・実質無料。test_sync.py の KskCommandRegistrationTests は
    # main() を経由せずこの関数を直接呼ぶ)。
    ksk_common.ensure_schema(client)
    active = ksk_common.get_threads_by_state(client, ksk_common.STATE_ACTIVE)
    active_by_id = {t["thread_id"]: t for t in active}
    active_owners = {t["owner_channel_id"] for t in active}
    active_count = len(active)

    # 登録候補はアカウントごとに1件へ絞る（同一スキャン内で複数打っていたら最新を採用）。
    # scan_counts は絞る前の件数 = 連投の度合いなので、自動BAN判定に使う。
    start_candidates, scan_counts = ksk_common.select_start_candidates(candidates)

    # 既に登録済み（active/ended どちらでも）の thread_id は上限判定をやり直さない。
    thread_ids = [r["comment_id"] for r, _c in start_candidates]
    known_ids: set[str] = set()
    if thread_ids:
        placeholders = ",".join("?" for _ in thread_ids)
        known_ids = {
            r["thread_id"] for r in client.query(
                f"SELECT thread_id FROM ksk_threads WHERE thread_id IN ({placeholders})",
                thread_ids,
            )
        }

    banned = ksk_common.get_banned_channels(client) if thread_ids else set()
    day_start = ksk_common.jst_day_start_epoch(now_epoch)
    changed = 0

    # 1スキャンで大量にコマンドを打ったアカウントは自動BANする。
    # 判定は登録処理より先に行う（BANしたアカウントのスレッドをその場で弾くため）。
    for cid in ksk_common.auto_ban_targets(scan_counts):
        if cid in banned:
            continue
        reason = (f"auto: {scan_counts[cid]} ksk commands in one "
                  f"{ksk_common.DETECT_WINDOW_MIN}min scan at {now_epoch}")
        ksk_common.add_ban(client, cid, reason, now_epoch)
        banned.add(cid)
        n_ended = ksk_common.end_threads_of_owner(
            client, cid, ksk_common.REASON_BANNED, now_epoch)
        changed += 1 + n_ended
        print(f"  ksk 自動BAN: {cid} ({reason}) 稼働中スレッド{n_ended}件を終了", flush=True)

    # 5分スキャン単発の閾値だけでなく、もう少し長い10分の窓でも絶対件数を見て
    # 自動BANする（AUTO_BAN_THREADS_PER_SCANの注記どおり、スキャン間隔をわずかに
    # 超えて分散して打たれた連投を拾うため）。このスキャンで1件以上検知した
    # アカウントだけを対象に1クエリ確認すればよく、ksk自体が稀なので
    # 毎分の実コストへの影響はほぼ無い。
    for cid in sorted(scan_counts):
        if cid in banned:
            continue
        count = ksk_common.count_recent_trigger_threads(client, cid, now_epoch)
        if count < ksk_common.TRIGGER_BAN_THRESHOLD:
            continue
        reason = (f"auto: {count} ksk trigger threads in "
                  f"{ksk_common.TRIGGER_BAN_WINDOW_MIN}min window at {now_epoch}")
        ksk_common.add_ban(client, cid, reason, now_epoch)
        banned.add(cid)
        n_ended = ksk_common.end_threads_of_owner(
            client, cid, ksk_common.REASON_BANNED, now_epoch)
        changed += 1 + n_ended
        print(f"  ksk 自動BAN(10分閾値): {cid} ({reason}) 稼働中スレッド{n_ended}件を終了", flush=True)

    # 解除コマンドは絞り込み前の全候補から拾う（返信として打たれるため、
    # select_start_candidates() が落とした行にも含まれ得る）。
    for row, cmd in candidates:
        if cmd["action"] != ksk_common.ACTION_STOP:
            continue
        # 解除はスレ主本人が自分の active スレッドに返信した場合のみ
        if row.get("parent_id") is None:
            continue
        thread = active_by_id.get(row.get("parent_id"))
        if thread is None or thread["owner_channel_id"] != row.get("author_channel_id"):
            continue
        ksk_common.end_thread(client, thread["thread_id"], ksk_common.REASON_STOPPED, now_epoch)
        active_by_id.pop(thread["thread_id"], None)
        active_owners.discard(thread["owner_channel_id"])
        active_count -= 1
        changed += 1
        print(f"  ksk 解除: {thread['thread_id']}", flush=True)

    # 登録はアカウントごとに絞り込み済みの候補だけを見る。
    # 登録コマンドが親コメント本文限定なのは、返信として打たれた `!ksk` だと
    # 既存スレッドへの返信を拾う経路が30分おきの巡回チェックしか無く、最大30分
    # 検知できないため（select_start_candidates() が親コメントだけを残している）。
    for row, cmd in start_candidates:
        author = row["author_channel_id"]
        thread_id = row["comment_id"]
        if thread_id in known_ids:
            continue
        if author in banned:
            print(f"  ksk 登録拒否(BAN): {thread_id}", flush=True)
            continue
        if active_count >= ksk_common.MAX_ACTIVE_THREADS:
            print(f"  ksk 登録拒否(同時上限{ksk_common.MAX_ACTIVE_THREADS}本): {thread_id}", flush=True)
            continue
        if author in active_owners:
            print(f"  ksk 登録拒否(このアカウントは稼働中): {thread_id}", flush=True)
            continue
        if ksk_common.count_registrations_since(client, author, day_start) >= ksk_common.MAX_REGISTRATIONS_PER_DAY:
            print(f"  ksk 登録拒否(1日{ksk_common.MAX_REGISTRATIONS_PER_DAY}回まで): {thread_id}", flush=True)
            continue

        ksk_common.register_thread(
            client, thread_id, author, row.get("handle"), cmd.get("title"),
            int(row["published_at"]), now_epoch,
        )
        known_ids.add(thread_id)
        active_owners.add(author)
        active_count += 1
        changed += 1
        print(f"  ksk 登録: {thread_id} title={cmd.get('title')!r}", flush=True)

    if changed:
        _write_ksk_index(client, now_epoch)
    return changed


def _write_ksk_index(client: TursoClient, now_epoch: int) -> None:
    """一覧ページ用の1行インデックスを作り直す(状態が変わったときだけ呼ぶ)。"""
    threads = ksk_common.get_threads_by_state(client, ksk_common.STATE_ACTIVE)
    threads += ksk_common.get_recent_threads(client, limit=ksk_common.INDEX_PAST_LIMIT)
    ksk_common.write_index(client, ksk_common.build_index_payload(threads, now_epoch), now_epoch)


# ------------------------------------------------------------------ #
# ksk 追跡（T1: 速度 / T2: 内訳）
#
# T1: active スレッドがある分だけ Pass1(commentThreads.list(id=))を1回投げ、
#     totalReplyCount を取る。50件までなら1 unit なので、稼働中でも 1 unit/分。
# T2: 返信本体を取得して Turso へ UPSERT し、集計SQLで payload を作る。
#     実行間隔は返信数に比例(ksk_common.pass2_interval_min)。
#
# 専用APIキー: 本体同期と同じキープールを使うと、加速中(=コメント急増中で本体同期も
# 普段より多くリクエストを投げている)にコアのクォータを食い、新着同期そのものを
# 止めかねない。ksk は API_KEY_FOR_KSK だけを使う独立クライアントで動かす。
# monthly_sweep.py のように configure_api_keys() でグローバルのキープールを差し替える
# 手は使えない — あちらは専用エントリポイントで単独プロセスだが、ksk は本体同期と
# 同じプロセス内に同居するため、差し替えると本体同期のローテーションを壊す。
# ------------------------------------------------------------------ #

KSK_API_KEY = os.getenv("API_KEY_FOR_KSK", "").strip()
KSK_TIME_BUDGET_SEC = 25  # main() 開始からこの秒数を超えたら ksk の追跡を打ち切る

# KSK_TIME_BUDGET_SEC は main() 開始(run_started_at)からの経過で測るため、
# sync_new_comments() が長引くコメント急増時ほど ksk の取り分が削られる。
# ちょうど ksk が最も活発なタイミングと相関するため、毎分ゼロになりかねない
# ワーストケースを避けるための下限。全体の実行時間を守る上限
# (run_started_at基準)自体は残しつつ、ksk追跡の開始時点から最低これだけは確保する。
KSK_MIN_GUARANTEED_SEC = 10

_ksk_youtube = None


def _get_ksk_youtube():
    global _ksk_youtube
    if not KSK_API_KEY:
        return None
    if _ksk_youtube is None:
        _ksk_youtube = build("youtube", "v3", developerKey=KSK_API_KEY, cache_discovery=False)
    return _ksk_youtube


def _ksk_pass1(youtube, thread_ids: list[str]) -> tuple[set, dict, bool]:
    """ksk 用 Pass1。戻り値: (生存スレッドID, {ID: totalReplyCount}, 完走したか)。

    _pass1_reply_counts() を流用しないのは、あちらが _handle_api_error() 経由で
    **本体同期のキープール**をローテーションしてしまうため。ksk は単一の専用キーで動くので
    ローテーションは行わず、失敗したらこの分は諦めて次の分に任せる(状態を持たないので
    リトライは自動的に成立する)。
    """
    alive: set[str] = set()
    counts: dict[str, int] = {}
    for i in range(0, len(thread_ids), RECHECK_ID_CHUNK):
        chunk = thread_ids[i:i + RECHECK_ID_CHUNK]
        try:
            resp = youtube.commentThreads().list(
                part="snippet", id=",".join(chunk), textFormat="plainText", maxResults=50,
            ).execute()
        except HttpError as e:
            print(f"  ksk Pass1 失敗: {e}", flush=True)
            return alive, counts, False
        if resp.get("nextPageToken"):
            # 渡したIDが1回で返り切っていない = 「返ってこなかった=削除」判定が成立しない。
            # 生きているスレッドを誤って終了させないため中断する(_MAX_DEAD_RATIO と同じ思想)。
            print("  ksk Pass1 のレスポンスが分割されている。削除誤検知を避けるため中断", flush=True)
            return alive, counts, False
        for item in resp.get("items", []):
            alive.add(item["id"])
            counts[item["id"]] = item["snippet"].get("totalReplyCount", 0)
    return alive, counts, True


def _ksk_fetch_reply_pages(youtube, thread_id: str, start_token):
    """返信を取得する。戻り値: (items, 最後のページを取ったトークン, 消費units)。

    最後のページを**取得したときのトークン**を返すのがポイント。最終ページには
    nextPageToken が無いので「続きから」のトークンは存在しない。代わりに最終ページの
    トークンを保存しておき、次回はそこから取り直す — 新しい返信は最終ページに
    追記されるので、そのページを取り直せば差分が入っている。
    """
    token = start_token
    items = []
    last_page_token = None
    units = 0
    for _ in range(ksk_common.MAX_REPLY_PAGES):
        resp = youtube.comments().list(
            part="snippet", parentId=thread_id, maxResults=100,
            pageToken=token, textFormat="plainText",
        ).execute()
        units += 1
        last_page_token = token
        items.extend(resp.get("items", []))
        token = resp.get("nextPageToken")
        if not token:
            break
    return items, last_page_token, units


def _ksk_sync_replies(client: TursoClient, youtube, thread: dict, now_epoch: int):
    """ksk 用 Pass2。**削除検知は一切行わない**(upsert のみ)。

    _resync_thread_replies() を流用してはいけない — あちらは「今回返ってこなかった
    既知返信 = 削除」として is_deleted=1 を打つ。連投バースト中の YouTube API は
    結果整合で一時的に返信を落とすことがあり、これを高頻度で実行すると本番 comments
    (2.1M行)に大量の偽削除が入って自動では戻せない。削除検知は従来の30分 recheck に任せる。

    戻り値: (新規に書いた返信数, 次回用トークン, 消費units)
    """
    tid = thread["thread_id"]
    known = ksk_common.known_reply_ids(client, tid)
    token = thread.get("resume_token")

    items, last_token, units = _ksk_fetch_reply_pages(youtube, tid, token)
    if token and items and not any(it["id"] in known for it in items):
        # 保存トークンが指す位置がズレた(YouTube の pageToken はリストが変化すると
        # 位置を保証しない)。既知IDと1件も重ならないなら信用できないので捨てて取り直す。
        print(f"  ksk: resume_token がズレたためフル再取得 {tid}", flush=True)
        items, last_token, extra = _ksk_fetch_reply_pages(youtube, tid, None)
        units += extra

    thread_pub = int(thread["started_at"])
    order = len(known) + 1
    rows = []
    for it in items:
        if it["id"] in known:
            continue
        rs = it["snippet"]
        deleted = is_deleted_sentinel(rs)
        rows.append({
            "comment_id": it["id"],
            "parent_id": tid,
            "reply_order": order,
            "thread_published_at": thread_pub,
            "author_channel_id": rs.get("authorChannelId", {}).get("value"),
            "handle": rs.get("authorDisplayName") if not deleted else None,
            "text": rs.get("textDisplay") if not deleted else None,
            "original_text": None,
            "published_at": parse_epoch(rs["publishedAt"]),
            "like_count": None if deleted else int(rs.get("likeCount", 0)),
            "is_pinned": 0,
            "is_deleted": 1 if deleted else 0,
            "deleted_confirmed_at": now_epoch if deleted else None,
            "fetched_at": now_epoch,
        })
        known.add(it["id"])
        order += 1

    # 差分だけ書く。値が変わらない行まで UPSERT すると ON CONFLICT DO UPDATE が
    # 行を書き換えて rows_written に計上される(1000件×毎分で日次予算の36%)。
    upsert_rows(client, rows)
    return len(rows), last_token, units


def _ksk_refresh_stats(client: TursoClient, thread: dict, reply_count: int, now_epoch: int) -> int:
    """集計SQLを回して ksk_thread_stats を書き、参加アカウント数を返す。"""
    tid = thread["thread_id"]
    q1, q2, gaps, medians = ksk_common.aggregate_thread(client, tid)
    payload = ksk_common.build_payload(
        thread, q1, q2, gaps, medians,
        total_reply_count=reply_count, now_epoch=now_epoch,
    )
    ksk_common.write_thread_stats(client, tid, payload, now_epoch)
    return len(q1)


def run_ksk_tracking(client: TursoClient, now_epoch: int, deadline: float) -> int:
    """active な ksk スレッドの速度と内訳を更新する。戻り値は状態が変わった件数。"""
    active = ksk_common.get_threads_by_state(client, ksk_common.STATE_ACTIVE)
    if not active:
        return 0

    youtube = _get_ksk_youtube()
    if youtube is None:
        print("  ksk: API_KEY_FOR_KSK 未設定のため追跡をスキップ", flush=True)
        return 0

    quota = ksk_common.read_quota_state(client, now_epoch)
    if quota["units_used"] >= ksk_common.DAILY_UNIT_BUDGET:
        for t in active:
            ksk_common.end_thread(client, t["thread_id"], ksk_common.REASON_BUDGET, now_epoch)
        _write_ksk_index(client, now_epoch)
        print(f"  ksk: 日次unit予算({ksk_common.DAILY_UNIT_BUDGET})に到達、追跡終了", flush=True)
        return len(active)

    alive, counts, ok = _ksk_pass1(youtube, [t["thread_id"] for t in active])
    units = 1
    if not ok:
        ksk_common.write_quota_state(client, now_epoch, quota["units_used"] + units)
        return 0

    # 「応答に含まれなかった=削除された」という判定は、YouTube側の一時的な不整合
    # (グリッチ)でも成立してしまう。_ksk_pass1 は _MAX_DEAD_RATIO のような安全弁を
    # 持たない(commentThreads.list(id=)が正常応答・nextPageTokenも無しでも、
    # 依頼したIDの一部が理由不明で欠けることがある)。一度 ended(deleted) にすると
    # 同じ comment_id は二度と再登録できないため、ここで比率を見て怪しければ
    # 今回は削除判定そのものを見送る(次回の Pass1 で再検知できるので恒久的な
    # 見逃しにはならない — sync.py の _MAX_DEAD_RATIO と同じ思想)。
    dead_ids = {t["thread_id"] for t in active} - alive
    suspect = ksk_common.dead_ratio_is_suspect(len(active), len(dead_ids))
    if suspect:
        print(
            f"  WARNING: ksk Pass1: {len(active)}件中{len(dead_ids)}件が消滅と判定された。"
            f"異常値のため削除判定をスキップする(速度更新は続行)", flush=True,
        )

    changed = 0
    for thread in active:
        tid = thread["thread_id"]
        if tid not in alive:
            if suspect:
                continue
            ksk_common.end_thread(client, tid, ksk_common.REASON_DELETED, now_epoch)
            changed += 1
            continue

        reply_count = int(counts.get(tid, 0))
        prev_count = int(thread.get("last_reply_count") or 0)
        grew = reply_count > prev_count
        # 終了判定は更新前の行(last_growth_at)を見る必要があるので、書き戻しより先に行う
        reason = ksk_common.evaluate_end_reason(thread, reply_count, now_epoch)
        ksk_common.update_pass1_progress(client, tid, reply_count, now_epoch, grew)
        if grew:
            changed += 1

        # 誰も乗らなかった `!ksk` に返信取得のコストを払わない。
        # 終了するスレッドは、最後の状態を残すため間隔を無視して1回だけ回す。
        should_pass2 = (
            reply_count >= ksk_common.MIN_REPLIES_FOR_TRACKING
            and (reason is not None or ksk_common.pass2_due(thread, reply_count, now_epoch))
            and time.monotonic() < deadline
        )
        if should_pass2:
            n_new, last_token, used = _ksk_sync_replies(client, youtube, thread, now_epoch)
            units += used
            snapshot = dict(thread)
            snapshot["last_reply_count"] = reply_count
            if reason is not None:
                snapshot["state"] = ksk_common.STATE_ENDED
                snapshot["ended_at"] = now_epoch
                snapshot["ended_reason"] = reason
            n_accounts = _ksk_refresh_stats(client, snapshot, reply_count, now_epoch)
            ksk_common.update_pass2_progress(client, tid, last_token, n_accounts, now_epoch)
            print(f"  ksk 更新: {tid} 返信{reply_count}件(+{n_new}) {n_accounts}アカウント", flush=True)

        if reason is not None:
            ksk_common.end_thread(client, tid, reason, now_epoch)
            changed += 1
            print(f"  ksk 終了: {tid} 理由={reason}", flush=True)

    ksk_common.write_quota_state(client, now_epoch, quota["units_used"] + units)
    if changed:
        _write_ksk_index(client, now_epoch)
    return changed


def check_dormant_ksk_threads(client: TursoClient, now_epoch: int) -> int:
    """ended な ksk スレッドが後から復活していないか低頻度でチェックする。

    Pass2 は active にしか走らないため、一度 ended になった payload は
    二度と自動更新されない。アイドルタイムアウト後に遅れて数件だけ返信が
    付くケースを拾えるようにする(YouTube APIは一切使わない — comments の
    件数比較だけで済む)。戻り値は状態が変わった(再集計 or 復活)件数。
    """
    threads = ksk_common.get_recent_threads(client, limit=ksk_common.DORMANT_CHECK_LIMIT)
    if not threads:
        return 0

    active_count = len(ksk_common.get_threads_by_state(client, ksk_common.STATE_ACTIVE))
    changed = 0

    for thread in threads:
        tid = thread["thread_id"]
        current = ksk_common.count_current_replies(client, tid)
        increase = current - int(thread.get("last_reply_count") or 0)
        action = ksk_common.classify_dormant_growth(increase)
        if action == "none":
            continue

        if action == "reactivate" and active_count < ksk_common.MAX_ACTIVE_THREADS:
            ksk_common.reactivate_thread(client, tid, now_epoch)
            active_count += 1
            changed += 1
            print(f"  ksk 復活: {tid} 返信+{increase}件 → active に復帰", flush=True)
            continue

        # 復活の枠が無い、または閾値未満 → 内訳だけ再集計して ended のまま
        snapshot = dict(thread)
        _ksk_refresh_stats(client, snapshot, current, now_epoch)
        ksk_common.update_last_reply_count(client, tid, current, now_epoch)
        changed += 1
        print(f"  ksk 休眠中に返信+{increase}件検知、再集計: {tid}", flush=True)

    if changed:
        _write_ksk_index(client, now_epoch)
    return changed


# ------------------------------------------------------------------ #
# エントリポイント
# ------------------------------------------------------------------ #

def main():
    if not _API_KEYS:
        print("ERROR: API_KEY_FOR_ALL_COMMENT_GET を設定してください")
        sys.exit(1)

    url = os.getenv("TURSO_URL")
    token = os.getenv("TURSO_AUTH_TOKEN")
    if not url or not token:
        print("ERROR: TURSO_URL と TURSO_AUTH_TOKEN を設定してください")
        sys.exit(1)

    client = TursoClient(url, token)

    # Account-page basic counts are a monotonic record of comments once seen,
    # including rows later marked deleted.  Trigger setup is best-effort so a
    # profile-schema problem never prevents the primary comment sync.
    try:
        if account_live_core.ensure_live_core_trigger(client):
            print("  account profile live-core trigger ready", flush=True)
    except Exception as e:
        print(f"  account profile live-core trigger setup failed: {e}", flush=True)

    # ksk(加速)テーブルを毎回無条件でensureする(account_live_coreと同じパターン)。
    # check_ksk_commands()自身は「!ksk を含む行が実際に見つかったときだけ」
    # ensure_schema()を呼ぶ設計(コスト削減)だが、それだと誰も !ksk を打っていない
    # 間はTurso上にksk_indexテーブルすら存在せず、flaskrの/api/ksk-threadsが
    # 「テーブルが無い」例外を拾って503を返し続けてしまう(CREATE TABLE IF NOT EXISTS
    # は冪等かつ実質無料なので、無条件で毎分呼んでも構わない)。
    try:
        ksk_common.ensure_schema(client)
    except Exception as e:
        print(f"  kskスキーマ準備エラー: {e}", flush=True)

    wait_until_next_minute()

    run_started_at = time.monotonic()
    now = datetime.now(timezone.utc)
    print(f"[{now.strftime('%H:%M:%S')} UTC] 同期開始")

    n_new = sync_new_comments(client)
    print(f"  新着: {n_new} 件")

    now_jst = now.astimezone(JST)
    try:
        written_time_keys = check_time_comments(client, now_jst)
    except Exception as e:
        print(f"  時報コメント即時反映エラー: {e}", flush=True)
        written_time_keys = set()
    for time_key in written_time_keys:
        _clear_time_comment_cache(time_key)

    # ksk(加速)の検出と追跡。try/exceptで包むのはcheck_time_comments()と同じ理由 —
    # この処理の失敗が、既に成功している新着同期や、この後の巡回チェックを
    # 巻き添えにしてはいけない。
    #
    # 実行時間バジェット: 1000返信のPass2は逐次HTTP10回になり得る。毎分ジョブが60秒を
    # 超えると concurrency group により次の分の実行が待たされ、新着同期そのものが遅れる。
    # main()開始からの経過を見て、超過していたら追跡を打ち切る(次の分に持ち越すだけ)。
    try:
        n_ksk = check_ksk_commands(client, int(now.timestamp()))
        if n_ksk:
            print(f"  ksk 状態変化: {n_ksk} 件")
    except Exception as e:
        print(f"  kskコマンド検出エラー: {e}", flush=True)
    # sync_new_comments()/check_ksk_commands() がここまでに使った時間ぶん、
    # run_started_at基準の上限は既に食われている。それでも最低
    # KSK_MIN_GUARANTEED_SEC秒は確保する(下限、上のKSK_MIN_GUARANTEED_SEC注記参照)。
    ksk_deadline = max(
        run_started_at + KSK_TIME_BUDGET_SEC,
        time.monotonic() + KSK_MIN_GUARANTEED_SEC,
    )
    try:
        n_track = run_ksk_tracking(client, int(now.timestamp()), ksk_deadline)
        if n_track:
            print(f"  ksk 追跡更新: {n_track} 件")
    except Exception as e:
        print(f"  ksk追跡エラー: {e}", flush=True)

    # 1時間おき: ended な ksk スレッドが後から復活していないかチェック。
    # YouTube APIを使わずTursoの件数比較だけで済むので、他の処理と違って
    # 頻繁に回す理由が無い(アイドルタイムアウト自体が30分単位のため)。
    if now.minute == 0:
        try:
            n_dormant = check_dormant_ksk_threads(client, int(now.timestamp()))
            if n_dormant:
                print(f"  ksk 休眠チェック: {n_dormant} 件")
        except Exception as e:
            print(f"  ksk休眠チェックエラー: {e}", flush=True)

    # 10分おき: コールド層(全履歴のカーソル巡回)だけ
    # 30分おき: それに加えてホット層(直近24時間のスレッド全件)も見る
    if now.minute % RECHECK_INTERVAL_MIN == 0:
        n_recheck = run_reply_recheck_batch(
            client, include_hot=(now.minute % HOT_INTERVAL_MIN == 0),
        )
        print(f"  スレッド巡回チェック書込み: {n_recheck} 件")

    # 毎分: 現在時間のバケットだけ再計算（軽い）
    # 10分おき: 直近6時間ぶんを広めに再計算し、上記の返信backfill・削除検知の
    # 遅延を吸収する（daily_statsの_REPROCESS_DAYSと同じ考え方）
    hours_back = 6 if now.minute % 10 == 0 else 1
    n_hourly = update_hourly_buckets(client, hours_back=hours_back)
    repaired_hourly = process_hourly_repair_queue(client)
    if repaired_hourly:
        print(f"  historical hourly buckets repaired: {repaired_hourly}", flush=True)
    print(f"  時間バケット更新: 直近{n_hourly}時間分")


if __name__ == "__main__":
    main()
