"""
数日分キャッチアップ。一回だけ手動実行する。

  python catchup.py [--days N]

Phase 1: 直近N日の新着スレッド＋返信を YouTube API から取得して Turso に UPSERT
         （DB上の最新スレッドID到達 または N日より古い投稿で停止）
Phase 2: Turso DB上にある直近N日のスレッドの返信を全件再スキャンして UPSERT
         （既存スレッドへの新着返信を拾う）
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

dotenv.load_dotenv(Path(__file__).parent.parent / "flaskr" / ".env")
dotenv.load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
from turso_client import TursoClient

VIDEO_ID = os.getenv("VIDEO_ID", "niKAylKNIEI")
BATCH_SIZE = 300

_API_KEYS = [k for k in [
    os.getenv("API_KEY_FOR_ALL_COMMENT_GET"),
    os.getenv("API_KEY_FOR_ALL_COMMENT_GET2"),
] if k]
_key_idx = 0
_exhausted_count = 0


def get_youtube():
    return build("youtube", "v3", developerKey=_API_KEYS[_key_idx], cache_discovery=False)


def rotate_key(e) -> bool:
    global _key_idx, _exhausted_count
    _exhausted_count += 1
    if _exhausted_count >= len(_API_KEYS):
        print(f"ERROR: 全キーのクォータが枯渇: {e}")
        return False
    _key_idx = (_key_idx + 1) % len(_API_KEYS)
    print(f"APIキーをローテーション → キー {_key_idx + 1}/{len(_API_KEYS)}")
    return True


def is_quota_error(e: HttpError) -> bool:
    err = str(e).lower()
    return e.resp.status in (403, 429) and any(s in err for s in [
        "quotaexceeded", "dailylimitexceeded",
        "userdailylimitexceeded", "ratelimitexceeded",
    ])


def parse_epoch(dt_str: str) -> int:
    return int(datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
               .replace(tzinfo=timezone.utc).timestamp())


def is_deleted_sentinel(snippet: dict) -> bool:
    return snippet.get("authorDisplayName", "") == ""


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
    stmts = [{"sql": _UPSERT_SQL, "args": _row_args(r)} for r in rows]
    for i in range(0, len(stmts), BATCH_SIZE):
        client.batch(stmts[i : i + BATCH_SIZE])


def fetch_all_replies(
    youtube, thread_id: str, thread_pub: int,
    included_replies: list, total_reply_count: int, now_epoch: int,
) -> list[dict]:
    """inline に収まらなかった返信を comments.list で補完取得。"""
    if total_reply_count <= len(included_replies):
        return []
    rows = []
    seen_ids = {r["id"] for r in included_replies}
    order = len(included_replies) + 1
    next_page = None
    while True:
        try:
            resp = youtube.comments().list(
                part="snippet", parentId=thread_id,
                maxResults=100, pageToken=next_page,
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
                "comment_id": r["id"], "parent_id": thread_id, "reply_order": order,
                "thread_published_at": thread_pub,
                "author_channel_id": rs.get("authorChannelId", {}).get("value"),
                "handle": rs.get("authorDisplayName") if not deleted else None,
                "text": rs.get("textDisplay") if not deleted else None,
                "original_text": None,
                "published_at": parse_epoch(rs["publishedAt"]),
                "like_count": None if deleted else int(rs.get("likeCount", 0)),
                "is_pinned": 0, "is_deleted": 1 if deleted else 0,
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
# Phase 1: 新着スレッド + 返信をAPIから取得
# ------------------------------------------------------------------ #

def catchup_new_comments(client: TursoClient, cutoff_epoch: int) -> int:
    """
    cutoff より古い投稿に到達するまで commentThreads.list を順にたどって UPSERT する。
    stop_id は使わない（固定コメントがAPIの先頭に来て誤検知するため）。
    """
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    youtube = get_youtube()
    next_page_token = None
    pending: list[dict] = []
    inserted = 0
    pages = 0

    cutoff_str = datetime.fromtimestamp(cutoff_epoch, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    print(f"Phase 1: 新着スレッドを取得（{cutoff_str} 以降）", flush=True)

    found_in_window = False  # ウィンドウ内スレッドを一件でも処理したか

    while True:
        try:
            resp = youtube.commentThreads().list(
                part="snippet,replies", videoId=VIDEO_ID,
                maxResults=100, pageToken=next_page_token,
                order="time", textFormat="plainText",
            ).execute()
        except HttpError as e:
            if is_quota_error(e):
                if not rotate_key(e):
                    break
                youtube = get_youtube()
                continue
            raise

        pages += 1
        stop = False

        for item in resp.get("items", []):
            top_snip = item["snippet"]["topLevelComment"]["snippet"]
            tid = item["snippet"]["topLevelComment"]["id"]
            pub = parse_epoch(top_snip["publishedAt"])

            if pub < cutoff_epoch:
                if found_in_window or pages > 1:
                    # 正常な終端（ウィンドウを過ぎた）
                    stop = True
                    break
                else:
                    # まだウィンドウ内のスレッドを一件も見ていない状態で古いアイテムが来た
                    # → 固定コメントが先頭に出てきた可能性が高いのでスキップ
                    print(f"  固定コメントとみなしてスキップ: {tid} (pub={top_snip['publishedAt']})", flush=True)
                    continue

            found_in_window = True
            deleted = is_deleted_sentinel(top_snip)
            thread_pub = pub

            pending.append({
                "comment_id": tid, "parent_id": None, "reply_order": None,
                "thread_published_at": thread_pub,
                "author_channel_id": top_snip.get("authorChannelId", {}).get("value"),
                "handle": top_snip.get("authorDisplayName") if not deleted else None,
                "text": top_snip.get("textDisplay") if not deleted else None,
                "original_text": None, "published_at": pub,
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
                    "comment_id": r["id"], "parent_id": tid, "reply_order": order,
                    "thread_published_at": thread_pub,
                    "author_channel_id": rs.get("authorChannelId", {}).get("value"),
                    "handle": rs.get("authorDisplayName") if not r_del else None,
                    "text": rs.get("textDisplay") if not r_del else None,
                    "original_text": None,
                    "published_at": parse_epoch(rs["publishedAt"]),
                    "like_count": None if r_del else int(rs.get("likeCount", 0)),
                    "is_pinned": 0, "is_deleted": 1 if r_del else 0,
                    "deleted_confirmed_at": now_epoch if r_del else None,
                    "fetched_at": now_epoch,
                })

            extra = fetch_all_replies(
                youtube, tid, thread_pub, inline_replies,
                item["snippet"]["totalReplyCount"], now_epoch,
            )
            pending.extend(extra)

        if len(pending) >= BATCH_SIZE:
            upsert_rows(client, pending)
            inserted += len(pending)
            pending = []
            print(f"  {inserted:,} 件書込み済み（{pages} ページ）", flush=True)

        if stop:
            break

        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break

    upsert_rows(client, pending)
    inserted += len(pending)
    print(f"Phase 1 完了: {inserted:,} 件 UPSERT（{pages} ページ）", flush=True)
    return inserted


# ------------------------------------------------------------------ #
# Phase 2: 既存スレッドの返信を再スキャン
# ------------------------------------------------------------------ #

def catchup_replies(client: TursoClient, cutoff_epoch: int) -> int:
    """
    Turso DB上の直近N日のスレッド全件の返信を comments.list で全取得して UPSERT。
    既存スレッドへの新着返信を補完する。
    """
    now_epoch = int(datetime.now(timezone.utc).timestamp())

    threads = client.query(
        "SELECT comment_id, published_at FROM comments "
        "WHERE parent_id IS NULL AND published_at >= ? "
        "ORDER BY published_at DESC",
        [cutoff_epoch],
    )
    if not threads:
        print("Phase 2: 対象スレッドなし", flush=True)
        return 0

    print(f"Phase 2: {len(threads):,} スレッドの返信を再スキャン", flush=True)
    youtube = get_youtube()
    total = 0

    for i, t in enumerate(threads, 1):
        tid = t["comment_id"]
        thread_pub = t["published_at"]
        next_page = None
        order = 1
        pending: list[dict] = []

        while True:
            try:
                resp = youtube.comments().list(
                    part="snippet", parentId=tid,
                    maxResults=100, pageToken=next_page,
                    textFormat="plainText",
                ).execute()
            except HttpError as e:
                if is_quota_error(e):
                    if not rotate_key(e):
                        upsert_rows(client, pending)
                        total += len(pending)
                        print(f"クォータ枯渇で中断。{i:,}/{len(threads):,} スレッド処理済み、{total:,} 件書込み", flush=True)
                        return total
                    youtube = get_youtube()
                    continue
                raise

            for r in resp.get("items", []):
                rs = r["snippet"]
                deleted = is_deleted_sentinel(rs)
                pending.append({
                    "comment_id": r["id"], "parent_id": tid, "reply_order": order,
                    "thread_published_at": thread_pub,
                    "author_channel_id": rs.get("authorChannelId", {}).get("value"),
                    "handle": rs.get("authorDisplayName") if not deleted else None,
                    "text": rs.get("textDisplay") if not deleted else None,
                    "original_text": None,
                    "published_at": parse_epoch(rs["publishedAt"]),
                    "like_count": None if deleted else int(rs.get("likeCount", 0)),
                    "is_pinned": 0, "is_deleted": 1 if deleted else 0,
                    "deleted_confirmed_at": now_epoch if deleted else None,
                    "fetched_at": now_epoch,
                })
                order += 1

            next_page = resp.get("nextPageToken")
            if not next_page:
                break

        upsert_rows(client, pending)
        total += len(pending)

        if i % 100 == 0 or i == len(threads):
            print(f"  [{i:,}/{len(threads):,}] {total:,} 件返信書込み済み", flush=True)

    print(f"Phase 2 完了: {total:,} 件 UPSERT", flush=True)
    return total


# ------------------------------------------------------------------ #
# エントリポイント
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(description="数日分コメントキャッチアップ")
    parser.add_argument("--days", type=int, default=5, help="何日分遡るか（デフォルト: 5）")
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
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=args.days)).timestamp())
    cutoff_str = datetime.fromtimestamp(cutoff, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"キャッチアップ: 直近 {args.days} 日（{cutoff_str} 以降）\n")

    catchup_new_comments(client, cutoff)
    print()
    catchup_replies(client, cutoff)
    print("\n完了")


if __name__ == "__main__":
    main()
