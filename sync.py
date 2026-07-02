"""
毎分差分同期 + 毎10分削除検知。GitHub Actions から呼び出す。

環境変数:
  API_KEY_FOR_ALL_COMMENT_GET, API_KEY_FOR_ALL_COMMENT_GET2
  VIDEO_ID
  TURSO_URL
  TURSO_AUTH_TOKEN
  CRONJOB_SECRET   (GitHub Actions の secret 認証用)
"""

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
DELETION_WINDOW_HOURS = 3
DELETION_INTERVAL_MIN = 10
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


def fetch_all_replies(
    youtube,
    thread_id: str,
    thread_pub: int,
    included_replies: list,
    total_reply_count: int,
    now_epoch: int,
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

            if stop_pub is not None and pub < stop_pub:
                if found_in_window or pages > 1:
                    found_stop = True
                else:
                    # 1ページ目でまだ新着を1件も見ていない → 固定コメント
                    print(f"  固定コメントとみなしてスキップ: {tid} (pub={top_snip['publishedAt']})", flush=True)
                    continue
            else:
                found_in_window = True

            deleted = is_deleted_sentinel(top_snip)
            thread_pub = pub

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

            if not found_stop:
                # 新着スレッドのみ: inline に収まらない返信を追加取得
                extra = fetch_all_replies(
                    youtube, tid, thread_pub, inline_replies,
                    item["snippet"]["totalReplyCount"], now_epoch,
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
    return inserted + len(pending)


# ------------------------------------------------------------------ #
# 既存スレッドへの新規返信再同期（毎10分）
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

    for t in threads:
        tid = t["comment_id"]
        thread_pub = t["published_at"]
        next_page = None
        order = 1
        pending: list[dict] = []

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
                        return total + len(pending)
                    youtube = get_youtube()
                    continue
                raise

            for r in resp.get("items", []):
                rs = r["snippet"]
                deleted = is_deleted_sentinel(rs)
                pending.append({
                    "comment_id": r["id"],
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
                order += 1

            next_page = resp.get("nextPageToken")
            if not next_page:
                break

        upsert_rows(client, pending)
        total += len(pending)

    return total


# ------------------------------------------------------------------ #
# 削除検知（毎10分）
# ------------------------------------------------------------------ #

def detect_deletions(client: TursoClient) -> int:
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    cutoff = now_epoch - DELETION_WINDOW_HOURS * 3600

    rows = client.query(
        "SELECT comment_id FROM comments "
        "WHERE published_at >= ? AND is_deleted = 0",
        [cutoff],
    )
    comment_ids = [r["comment_id"] for r in rows]
    if not comment_ids:
        return 0

    youtube = get_youtube()
    alive_ids: set[str] = set()

    for i in range(0, len(comment_ids), 50):
        chunk = comment_ids[i : i + 50]
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
                        print("削除検知をスキップ（クォータ枯渇）")
                        return 0
                    youtube = get_youtube()
                    continue
                raise

        for item in resp.get("items", []):
            alive_ids.add(item["id"])

    deleted_ids = [cid for cid in comment_ids if cid not in alive_ids]
    if not deleted_ids:
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
        for cid in deleted_ids
    ]
    for i in range(0, len(stmts), BATCH_SIZE):
        client.batch(stmts[i : i + BATCH_SIZE])

    return len(deleted_ids)


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

    if now.minute % DELETION_INTERVAL_MIN == 0:
        n_del = detect_deletions(client)
        print(f"  削除検知: {n_del} 件")


if __name__ == "__main__":
    main()
