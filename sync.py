"""
毎分差分同期 + 毎10分削除検知。GitHub Actions から呼び出す。

環境変数:
  API_KEY_FOR_ALL_COMMENT_GET, API_KEY_FOR_ALL_COMMENT_GET2
  VIDEO_ID
  TURSO_URL
  TURSO_AUTH_TOKEN
  CRONJOB_SECRET   (GitHub Actions の secret 認証用)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

dotenv.load_dotenv(Path(__file__).parent.parent / "flaskr" / ".env")
dotenv.load_dotenv()

from turso_client import TursoClient

VIDEO_ID = os.getenv("VIDEO_ID", "niKAylKNIEI")
BATCH_SIZE = 300
REPLY_SYNC_WINDOW_HOURS = 3
REPLY_SYNC_INTERVAL_MIN = 30

# ------------------------------------------------------------------ #
# API キーローテーション（ダッシュボード用 YOUTUBE_API_KEY は使わない）
# ------------------------------------------------------------------ #

_API_KEYS = [k for k in [
    os.getenv("API_KEY_FOR_ALL_COMMENT_GET"),
    os.getenv("API_KEY_FOR_ALL_COMMENT_GET2"),
] if k]
_key_idx = 0
_exhausted_count = 0


def get_youtube():
    return build("youtube", "v3", developerKey=_API_KEYS[_key_idx], cache_discovery=False)


def rotate_key(e: Exception) -> bool:
    global _key_idx, _exhausted_count
    _exhausted_count += 1
    if _exhausted_count >= len(_API_KEYS):
        print(f"ERROR: 全キーのクォータが枯渇: {e}")
        return False
    _key_idx = (_key_idx + 1) % len(_API_KEYS)
    print(f"APIキーをローテーション → キー {_key_idx + 1}")
    return True


def is_quota_error(e: HttpError) -> bool:
    err = str(e).lower()
    return e.resp.status in (403, 429) and any(s in err for s in [
        "quotaexceeded", "dailylimitexceeded",
        "userdailylimitexceeded", "ratelimitexceeded",
    ])


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
    # idx_parent_published により1行読み取りで済む。
    # スレッドIDではなく published_at を停止条件にする:
    # 最新スレッドが YouTube 上で削除されると ID は二度と API 応答に
    # 現れず、全履歴をページングし続けてクォータを焼き尽くすため。
    rows = client.query(
        "SELECT MAX(published_at) AS p FROM comments WHERE parent_id IS NULL"
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
    stmts = [
        {"sql": _UPSERT_AUTHOR_SQL,
         "args": [cid, s["handle"], s["avatar_url"], s["updated_at"], s.get("first_seen_at")]}
        for cid, s in sightings.items()
    ]
    for i in range(0, len(stmts), BATCH_SIZE):
        client.batch(stmts[i : i + BATCH_SIZE])


def fetch_all_replies(
    youtube,
    thread_id: str,
    thread_pub: int,
    included_replies: list,
    total_reply_count: int,
    now_epoch: int,
    sightings: dict | None = None,
) -> list[dict]:
    if total_reply_count <= len(included_replies):
        return []

    rows = []
    seen_ids = {r["id"] for r in included_replies}
    order = len(included_replies) + 1
    next_page = None

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
            if is_quota_error(e):
                if not rotate_key(e):
                    return rows
                youtube = get_youtube()
                continue
            raise

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

    return rows


# ------------------------------------------------------------------ #
# 新着同期（毎分）
# ------------------------------------------------------------------ #

MAX_PAGES = 30  # 安全弁: 毎分実行で30ページ(3000スレッド)を超える新着はあり得ない


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
            if is_quota_error(e):
                if not rotate_key(e):
                    break
                youtube = get_youtube()
                continue
            raise

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
                    print(f"  固定コメントとみなしてスキップ: {tid} (pub={top_snip['publishedAt']})", flush=True)
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

            # ここに到達するのは新着スレッドのみ（既知に達したら上で break 済み）
            extra = fetch_all_replies(
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
# 既存スレッドの返信再同期 + 削除検知（毎30分）
#
# 削除は「前回取得できていたスレッド/返信が、再取得したら消えている」
# ことでしか判定できない（生のYouTube APIレスポンスに削除済みを示す
# センチネル値は存在しない）。よってスレッド・返信それぞれについて
# 既知IDと再取得結果のIDを突き合わせ、消えたものだけ削除扱いにする。
# ------------------------------------------------------------------ #

def sync_recent_replies(client: TursoClient) -> int:
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    cutoff = now_epoch - REPLY_SYNC_WINDOW_HOURS * 3600

    threads = client.query(
        "SELECT comment_id, published_at FROM comments "
        "WHERE parent_id IS NULL AND published_at >= ? "
        "ORDER BY published_at DESC",
        [cutoff],
    )
    if not threads:
        return 0

    youtube = get_youtube()
    total = 0
    deleted_total = 0
    sightings: dict = {}

    # --- スレッド自身の生存確認 ---
    thread_ids = [t["comment_id"] for t in threads]
    alive_thread_ids: set[str] = set()
    quota_exhausted = False

    for i in range(0, len(thread_ids), 50):
        chunk = thread_ids[i : i + 50]
        while True:
            try:
                resp = youtube.comments().list(
                    part="snippet",
                    id=",".join(chunk),
                    textFormat="plainText",
                ).execute()
                break
            except HttpError as e:
                if is_quota_error(e):
                    if not rotate_key(e):
                        quota_exhausted = True
                        break
                    youtube = get_youtube()
                    continue
                raise
        if quota_exhausted:
            break
        for item in resp.get("items", []):
            alive_thread_ids.add(item["id"])
            _note_author(sightings, item["snippet"], now_epoch)

    if quota_exhausted:
        upsert_author_sightings(client, sightings)
        print("  返信再同期をスキップ（クォータ枯渇）")
        return 0

    dead_thread_ids = [tid for tid in thread_ids if tid not in alive_thread_ids]
    deleted_total += _mark_deleted(client, dead_thread_ids, now_epoch)
    dead_thread_set = set(dead_thread_ids)

    # スレッドが消えれば、そこにぶら下がる返信も一緒に消える
    # → APIを叩かず、既知の返信IDをそのまま削除扱いにする
    for tid in dead_thread_ids:
        known = client.query(
            "SELECT comment_id FROM comments WHERE parent_id = ? AND is_deleted = 0",
            [tid],
        )
        deleted_total += _mark_deleted(client, [r["comment_id"] for r in known], now_epoch)

    # --- 各スレッドの返信を再取得し、消えた返信を検知 ---
    for t in threads:
        tid = t["comment_id"]
        if tid in dead_thread_set:
            continue
        thread_pub = t["published_at"]
        next_page = None
        order = 1
        pending: list[dict] = []
        fetched_ids: set[str] = set()

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
                if is_quota_error(e):
                    if not rotate_key(e):
                        upsert_rows(client, pending)
                        upsert_author_sightings(client, sightings)
                        return total + len(pending)
                    youtube = get_youtube()
                    continue
                raise

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
        total += len(pending)

        known = client.query(
            "SELECT comment_id FROM comments WHERE parent_id = ? AND is_deleted = 0",
            [tid],
        )
        known_ids = {r["comment_id"] for r in known}
        deleted_total += _mark_deleted(client, list(known_ids - fetched_ids), now_epoch)

    upsert_author_sightings(client, sightings)
    print(f"  削除検知: {deleted_total} 件")
    return total


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

    wait_until_next_minute()

    now = datetime.now(timezone.utc)
    print(f"[{now.strftime('%H:%M:%S')} UTC] 同期開始")

    n_new = sync_new_comments(client)
    print(f"  新着: {n_new} 件")

    if now.minute % REPLY_SYNC_INTERVAL_MIN == 0:
        n_reply = sync_recent_replies(client)
        print(f"  返信再同期: {n_reply} 件")

    # 毎分: 現在時間のバケットだけ再計算（軽い）
    # 10分おき: 直近6時間ぶんを広めに再計算し、上記の返信backfill・削除検知の
    # 遅延を吸収する（daily_statsの_REPROCESS_DAYSと同じ考え方）
    hours_back = 6 if now.minute % 10 == 0 else 1
    n_hourly = update_hourly_buckets(client, hours_back=hours_back)
    print(f"  時間バケット更新: 直近{n_hourly}時間分")


if __name__ == "__main__":
    main()
