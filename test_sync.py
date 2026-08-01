"""sync.py の回帰テスト。

2026-07-29に確認・修正した2件のバグを固定する:
  1. DB上「最新」とされるスレッドがYouTube側で削除されると、get_latest_thread_pub()の
     停止条件(stop_pub)が永久にそのスレッドを指したまま固定され、1ページ目の全itemが
     「固定コメント候補」の分岐に落ちて is_pinned の連鎖的な誤書き換えを繰り返す。
     → get_latest_thread_pub()にis_deleted=0を足す(巡回チェックが削除を確定させた後の
       自己修復)と、1回の実行あたりの固定コメント再判定を1回にキャップする安全弁の2つで対応。
  2. fetch_all_replies()/_resync_thread_replies()が内部でキーローテーションしても、
     更新後のyoutubeクライアントを呼び出し元へ返さず、次のスレッドで枯渇済みキーを
     再び使ってしまう。→ 両関数の戻り値に youtube を追加。
"""
import json
import sqlite3
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from googleapiclient.errors import HttpError

sys.path.insert(0, str(Path(__file__).resolve().parent))

import sync
import account_live_core


def epoch_to_iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_item(tid: str, published_epoch: int, reply_count: int = 0, replies=None) -> dict:
    return {
        "snippet": {
            "topLevelComment": {
                "id": tid,
                "snippet": {
                    "publishedAt": epoch_to_iso(published_epoch),
                    "authorDisplayName": "@someone",
                    "likeCount": 0,
                    "authorChannelId": {"value": "UC0000000000000000000000"},
                    "textDisplay": "text",
                    "isPinned": False,
                },
            },
            "totalReplyCount": reply_count,
        },
        "replies": {"comments": replies or []},
    }


class FakeTurso:
    """get_latest_thread_pub()・upsert_rows()・upsert_author_sightings() が
    使う分だけを実装する最小限の偽Turso。"""

    def __init__(self, latest_thread_pub=None):
        self._latest_thread_pub = latest_thread_pub
        self.executed = []
        self.batched = []

    def query(self, sql, args=None, timeout=30):
        if "MAX(published_at)" in sql:
            return [{"p": self._latest_thread_pub}]
        return []

    def execute(self, sql, args=None, timeout=30):
        self.executed.append((" ".join(sql.split()), args))
        return {}

    def batch(self, statements, timeout=30):
        self.batched.append(statements)
        return {}


class SqliteTurso:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row

    def query(self, sql, args=None, timeout=30):
        return [dict(row) for row in self.db.execute(sql, args or []).fetchall()]

    def execute(self, sql, args=None, timeout=30):
        self.db.execute(sql, args or [])
        self.db.commit()
        return {}


class FakeResponsePage:
    def __init__(self, response):
        self._response = response

    def execute(self):
        return self._response


class FakeCommentThreads:
    def __init__(self, pages):
        self._pages = list(pages)
        self.list_calls = 0

    def list(self, **kwargs):
        self.list_calls += 1
        return FakeResponsePage(self._pages[self.list_calls - 1])


class FakeYoutube:
    def __init__(self, pages):
        self._comment_threads = FakeCommentThreads(pages)

    def commentThreads(self):
        return self._comment_threads


class SyncNewCommentsPinnedCascadeTests(unittest.TestCase):
    def test_caps_pinned_reconciles_when_anchor_thread_was_deleted(self):
        """stop_pubアンカーが壊れている(削除されたスレッドを指したまま)状況を再現する。

        1ページに5件、全て stop_pub より古く、新着(found_in_window)も stop_pub と
        一致する行も無い ―― 修正前はこの5件が全て _reconcile_pinned_comment に
        落ちて is_pinned の連鎖書き換えを起こしていた。修正後は1回で打ち切られる。
        """
        stop_pub = 2_000_000_000
        items = [make_item(f"old{i}", stop_pub - 100 - i) for i in range(5)]
        client = FakeTurso(latest_thread_pub=stop_pub)
        reconcile_calls = []

        with patch.object(sync, "_reconcile_pinned_comment",
                           side_effect=lambda c, tid, snip, now: reconcile_calls.append(tid)), \
             patch.object(sync, "get_youtube", return_value=FakeYoutube([{"items": items}])):
            inserted = sync.sync_new_comments(client)

        self.assertEqual(len(reconcile_calls), 1, "5件全部ではなく1件だけで打ち切られるべき")
        self.assertEqual(inserted, 0, "固定コメント候補はどれも新着として書き込まれない")

    def test_still_reconciles_a_genuine_single_pinned_change(self):
        """正常系: 固定コメントが実際に1件だけ変わり、その後に新着が続くケースは
        従来どおり正しく処理できること(安全弁が正常系を壊していないことの固定)。"""
        stop_pub = 2_000_000_000
        pinned_change = make_item("newly_pinned", stop_pub - 500)  # 固定コメント候補(1件だけ)
        fresh_thread = make_item("fresh1", stop_pub + 10)          # 本当の新着
        known_thread = make_item("known", stop_pub)                # stop_pubに一致 → 打ち切り
        items = [pinned_change, fresh_thread, known_thread]
        client = FakeTurso(latest_thread_pub=stop_pub)
        reconcile_calls = []

        with patch.object(sync, "_reconcile_pinned_comment",
                           side_effect=lambda c, tid, snip, now: reconcile_calls.append(tid)), \
             patch.object(sync, "get_youtube", return_value=FakeYoutube([{"items": items}])), \
             patch.object(sync, "fetch_all_replies", return_value=(object(), [])):
            inserted = sync.sync_new_comments(client)

        self.assertEqual(reconcile_calls, ["newly_pinned"])
        self.assertEqual(inserted, 1, "fresh1だけが新着スレッドとして書き込まれる")

    def test_get_latest_thread_pub_ignores_deleted_threads(self):
        """MAX(published_at)クエリにis_deleted=0が乗っていること(SQL文字列レベルで固定)。"""
        client = FakeTurso(latest_thread_pub=123)
        recorded = {}

        original_query = client.query

        def spy_query(sql, args=None, timeout=30):
            recorded["sql"] = sql
            return original_query(sql, args, timeout)

        client.query = spy_query
        result = sync.get_latest_thread_pub(client)

        self.assertEqual(result, 123)
        self.assertIn("is_deleted = 0", recorded["sql"])
        self.assertIn("parent_id IS NULL", recorded["sql"])


class AccountLiveCoreTests(unittest.TestCase):
    def setUp(self):
        self.client = SqliteTurso()
        self.addCleanup(self.client.db.close)
        self.client.execute(
            "CREATE TABLE comments (comment_id TEXT PRIMARY KEY, parent_id TEXT, "
            "author_channel_id TEXT, handle TEXT, published_at INTEGER, is_deleted INTEGER, "
            "fetched_at INTEGER)"
        )
        self.client.execute(
            "CREATE TABLE account_profile_data (channel_id TEXT PRIMARY KEY, "
            "core_payload TEXT NOT NULL, core_hash TEXT NOT NULL, core_updated_at INTEGER NOT NULL)"
        )
        core = {
            "handle_snapshot": "@old", "first_comment_date": "2025-01-01",
            "last_comment_date": "2025-01-01", "total_comments": 5,
            "peak_total_comments": 5, "thread_count": 2, "reply_count": 3,
            "thread_ratio": 0.4,
        }
        self.client.execute(
            "INSERT INTO account_profile_data VALUES(?,?,?,?)",
            ["UC0000000000000000000000", json.dumps(core), "old-hash", 1],
        )

    def _write_comment(self, comment_id, parent_id, published_at, deleted=0):
        self.client.execute(
            "INSERT INTO comments(comment_id,parent_id,author_channel_id,handle,published_at,is_deleted,fetched_at) "
            "VALUES(?,?,?,?,?,?,?) ON CONFLICT(comment_id) DO UPDATE SET fetched_at=excluded.fetched_at",
            [comment_id, parent_id, "UC0000000000000000000000", "@new", published_at, deleted, published_at],
        )

    def test_real_insert_updates_basic_counts_and_deleted_rows_still_count(self):
        self.assertTrue(account_live_core.ensure_live_core_trigger(self.client))
        published = int(datetime(2025, 1, 2, 0, 0, tzinfo=timezone.utc).timestamp())
        self._write_comment("new-deleted", None, published, deleted=1)

        row = self.client.query("SELECT * FROM account_profile_data")[0]
        core = json.loads(row["core_payload"])
        self.assertEqual(6, core["total_comments"])
        self.assertEqual(3, core["thread_count"])
        self.assertEqual(3, core["reply_count"])
        self.assertEqual(0.5, core["thread_ratio"])
        self.assertEqual("2025-01-02", core["last_comment_date"])
        self.assertEqual("@new", core["handle_snapshot"])
        self.assertEqual("", row["core_hash"])

        # The regular comment_sync conflict path must not fire the INSERT trigger.
        self._write_comment("new-deleted", None, published, deleted=1)
        core = json.loads(self.client.query("SELECT core_payload FROM account_profile_data")[0]["core_payload"])
        self.assertEqual(6, core["total_comments"])

    def test_setup_skips_a_comments_only_database(self):
        client = SqliteTurso()
        try:
            client.execute("CREATE TABLE comments(comment_id TEXT PRIMARY KEY)")
            self.assertFalse(account_live_core.ensure_live_core_trigger(client))
        finally:
            client.db.close()


def make_http_error(status: int = 403, message: str = "quotaExceeded") -> HttpError:
    class FakeResp:
        def __init__(self, status):
            self.status = status
            self.reason = "error"

    content = json.dumps({"error": {"message": message}}).encode("utf-8")
    return HttpError(FakeResp(status), content, uri="https://example.test")


class RaisingYoutube:
    """comments().list().execute() が常に指定のHttpErrorを送出する偽youtube。
    (=枯渇済みキーを表す)"""

    def __init__(self, error: HttpError):
        self._error = error

    def comments(self):
        return self

    def list(self, **kwargs):
        return self

    def execute(self):
        raise self._error


class SucceedingYoutube:
    """comments().list().execute() が固定のitemsを返す偽youtube。(=生きているキーを表す)"""

    def __init__(self, items):
        self._items = items

    def comments(self):
        return self

    def list(self, **kwargs):
        return self

    def execute(self):
        return {"items": self._items}


class KeyRotationPropagationTests(unittest.TestCase):
    """fetch_all_replies()/_resync_thread_replies()が内部でキーローテーションした際、
    更新後のyoutubeを呼び出し元へ返すこと(2026-07-29修正)を固定する。"""

    def test_fetch_all_replies_returns_the_rotated_youtube(self):
        old_youtube = RaisingYoutube(make_http_error())
        new_youtube = SucceedingYoutube(items=[])

        def fake_handle(e, youtube, retries):
            # ローテーション成功: 新しいyoutube、リトライ回数リセット、raiseしない、枯渇していない
            return new_youtube, 0, False, False

        with patch.object(sync, "_handle_api_error", side_effect=fake_handle):
            returned_youtube, rows = sync.fetch_all_replies(
                old_youtube, "thread1", 1000, [], total_reply_count=5,
                now_epoch=1000, sightings={},
            )

        self.assertIs(returned_youtube, new_youtube,
                       "ローテーション後のyoutubeを呼び出し元へ返していない(旧バグ)")
        self.assertEqual(rows, [])

    def test_resync_thread_replies_returns_the_rotated_youtube(self):
        old_youtube = RaisingYoutube(make_http_error())
        new_youtube = SucceedingYoutube(items=[])
        client = FakeTurso()

        def fake_handle(e, youtube, retries):
            return new_youtube, 0, False, False

        with patch.object(sync, "_handle_api_error", side_effect=fake_handle):
            returned_youtube, n, deleted, exhausted = sync._resync_thread_replies(
                old_youtube, client, "thread1", 1000, 1000, {},
            )

        self.assertIs(returned_youtube, new_youtube,
                       "ローテーション後のyoutubeを呼び出し元へ返していない(旧バグ)")
        self.assertFalse(exhausted)

    def test_recheck_threads_carries_the_rotated_youtube_across_threads(self):
        """_recheck_threads() の for ループが、あるスレッドで起きたローテーションを
        次のスレッドの _resync_thread_replies 呼び出しに引き継ぐこと。

        以前は関数ローカルの再代入が反映されず、2件目以降も1件目に渡したのと同じ
        (場合によっては既に枯渇済みの)youtubeを渡し続けていた。
        """
        threads = [
            {"comment_id": "t1", "published_at": 100},
            {"comment_id": "t2", "published_at": 200},
        ]
        after_pass1_youtube = object()
        received_youtube_per_call = []
        rotated_per_call = []

        def fake_resync(youtube, client, tid, thread_pub, now_epoch, sightings):
            received_youtube_per_call.append(youtube)
            new_youtube = object()
            rotated_per_call.append(new_youtube)
            return new_youtube, 1, 0, False

        client = FakeTurso()
        with patch.object(
            sync, "_pass1_reply_counts",
            return_value=(after_pass1_youtube, {"t1", "t2"}, {"t1": 5, "t2": 5}, False, False),
        ), patch.object(
            sync, "_known_reply_counts", return_value={"t1": 3, "t2": 3},
        ), patch.object(
            sync, "_resync_thread_replies", side_effect=fake_resync,
        ):
            sync._recheck_threads(client, object(), threads, "test", pass2_cap=None)

        self.assertEqual(len(received_youtube_per_call), 2)
        self.assertIs(received_youtube_per_call[0], after_pass1_youtube,
                       "1件目はPass1後のyoutubeを受け取るべき")
        self.assertIs(received_youtube_per_call[1], rotated_per_call[0],
                       "2件目は1件目が返したyoutubeを受け取るべき(旧バグではPass1直後のものを再利用していた)")

    def test_recheck_threads_reports_pass2_quota_exhaustion_as_aborted(self):
        threads = [{"comment_id": "t1", "published_at": 100}]
        client = FakeTurso()
        with patch.object(
            sync, "_pass1_reply_counts",
            return_value=(object(), {"t1"}, {"t1": 5}, False, False),
        ), patch.object(
            sync, "_known_reply_counts", return_value={"t1": 0},
        ), patch.object(
            sync, "_resync_thread_replies",
            return_value=(object(), 0, 0, True),
        ):
            _written, _dead, _mismatch, aborted, _deferred = sync._recheck_threads(
                client, object(), threads, "test", pass2_cap=None,
            )

        self.assertTrue(aborted)


if __name__ == "__main__":
    unittest.main()
