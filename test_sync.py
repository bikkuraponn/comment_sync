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
import ksk_common
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


class KskCommandRegistrationTests(unittest.TestCase):
    """check_ksk_commands() を実DB(sqlite)に対して回す回帰テスト。

    ksk_common のスキーマ(ON CONFLICT/CHECK制約等)は libSQL 前提だが、
    sqlite3 でも同じ文法が通るため、AccountLiveCoreTests と同じ
    SqliteTurso を使う(モックだと「本当にINSERT/UPDATEされたか」まで
    確認できないため)。
    """

    def setUp(self):
        self.client = SqliteTurso()
        self.addCleanup(self.client.db.close)
        self.client.execute(
            "CREATE TABLE comments (comment_id TEXT PRIMARY KEY, parent_id TEXT, "
            "author_channel_id TEXT, handle TEXT, published_at INTEGER, text TEXT, "
            "is_deleted INTEGER DEFAULT 0)"
        )

    def _post(self, comment_id, author, text, parent_id=None, published_at=1_000_000):
        self.client.execute(
            "INSERT INTO comments(comment_id,parent_id,author_channel_id,handle,published_at,text) "
            "VALUES(?,?,?,?,?,?)",
            [comment_id, parent_id, author, "@" + author, published_at, text],
        )

    def _active_owners(self):
        rows = self.client.query(
            "SELECT owner_channel_id FROM ksk_threads WHERE state = 'active'"
        )
        return [r["owner_channel_id"] for r in rows]

    def test_same_account_cannot_run_two_threads_at_once(self):
        self._post("t1", "UC_a", "!ksk 1本目", published_at=1_000_000)
        sync.check_ksk_commands(self.client, 1_000_060)
        self.assertEqual(self._active_owners(), ["UC_a"])

        # 同じアカウントが別スレッドで !ksk を打っても登録されない
        self._post("t2", "UC_a", "!ksk 2本目", published_at=1_000_010)
        sync.check_ksk_commands(self.client, 1_000_060)
        rows = self.client.query("SELECT thread_id FROM ksk_threads WHERE state = 'active'")
        self.assertEqual([r["thread_id"] for r in rows], ["t1"])
        self.assertEqual(
            self.client.query("SELECT thread_id FROM ksk_threads WHERE thread_id = 't2'"), []
        )

    def test_account_can_start_a_new_thread_after_stopping(self):
        self._post("t1", "UC_a", "!ksk 1本目", published_at=1_000_000)
        sync.check_ksk_commands(self.client, 1_000_060)

        self._post("r1", "UC_a", "!ksk stop", parent_id="t1", published_at=1_000_020)
        sync.check_ksk_commands(self.client, 1_000_060)
        self.assertEqual(
            self.client.query("SELECT state FROM ksk_threads WHERE thread_id = 't1'")[0]["state"],
            "ended",
        )

        self._post("t2", "UC_a", "!ksk 2本目", published_at=1_000_030)
        sync.check_ksk_commands(self.client, 1_000_060)
        self.assertEqual(self._active_owners(), ["UC_a"])

    def test_global_cap_rejects_beyond_max_active_threads(self):
        for i in range(ksk_common.MAX_ACTIVE_THREADS + 3):
            self._post(f"t{i}", f"UC_{i}", "!ksk", published_at=1_000_000 + i)
        sync.check_ksk_commands(self.client, 1_000_060)

        active = self.client.query("SELECT thread_id FROM ksk_threads WHERE state = 'active'")
        self.assertEqual(len(active), ksk_common.MAX_ACTIVE_THREADS)

    def test_owner_can_stop_with_bare_owari(self):
        self._post("t1", "UC_a", "!ksk", published_at=1_000_000)
        sync.check_ksk_commands(self.client, 1_000_060)

        self._post("r1", "UC_a", "終了", parent_id="t1", published_at=1_000_020)
        sync.check_ksk_commands(self.client, 1_000_060)

        row = self.client.query(
            "SELECT state, ended_reason FROM ksk_threads WHERE thread_id='t1'")[0]
        self.assertEqual(row["state"], "ended")
        self.assertEqual(row["ended_reason"], "stopped")

    def test_owari_from_someone_else_does_not_stop(self):
        # 止められるのはスレ主本人だけ(他人が「終了」と書いても効かない)
        self._post("t1", "UC_a", "!ksk", published_at=1_000_000)
        sync.check_ksk_commands(self.client, 1_000_060)

        self._post("r1", "UC_other", "終了", parent_id="t1", published_at=1_000_020)
        sync.check_ksk_commands(self.client, 1_000_060)

        self.assertEqual(
            self.client.query("SELECT state FROM ksk_threads WHERE thread_id='t1'")[0]["state"],
            "active",
        )

    def test_newest_thread_wins_within_one_scan(self):
        # 同じスキャンで複数打った場合は最後のものが採用される
        self._post("t1", "UC_a", "!ksk 1本目", published_at=1_000_000)
        self._post("t2", "UC_a", "!ksk 2本目", published_at=1_000_020)
        sync.check_ksk_commands(self.client, 1_000_060)

        rows = self.client.query(
            "SELECT thread_id, title FROM ksk_threads WHERE state = 'active'")
        self.assertEqual([r["thread_id"] for r in rows], ["t2"])
        self.assertEqual(rows[0]["title"], "2本目")

    def test_already_running_thread_is_not_replaced_by_a_later_one(self):
        # 別スキャンで後から立てたスレッドが、走っているスレッドを置き換えないこと
        self._post("t1", "UC_a", "!ksk 走行中", published_at=1_000_000)
        sync.check_ksk_commands(self.client, 1_000_060)

        self._post("t2", "UC_a", "!ksk 後発", published_at=1_000_100)
        sync.check_ksk_commands(self.client, 1_000_160)

        rows = self.client.query("SELECT thread_id FROM ksk_threads WHERE state = 'active'")
        self.assertEqual([r["thread_id"] for r in rows], ["t1"])


class KskAutoBanTest(unittest.TestCase):
    """1スキャンで大量にコマンドを打ったアカウントの自動BAN(2026-08-03追加)。"""

    def setUp(self):
        self.client = SqliteTurso()
        self.addCleanup(self.client.db.close)
        self.client.execute(
            "CREATE TABLE comments (comment_id TEXT PRIMARY KEY, parent_id TEXT, "
            "author_channel_id TEXT, handle TEXT, published_at INTEGER, text TEXT, "
            "is_deleted INTEGER DEFAULT 0)"
        )

    def _post(self, comment_id, author, text, parent_id=None, published_at=1_000_000):
        self.client.execute(
            "INSERT INTO comments(comment_id,parent_id,author_channel_id,handle,published_at,text) "
            "VALUES(?,?,?,?,?,?)",
            [comment_id, parent_id, author, "@" + author, published_at, text],
        )

    def _bans(self):
        return {r["channel_id"]: r["reason"]
                for r in self.client.query("SELECT channel_id, reason FROM ksk_bans")}

    def test_spamming_account_is_auto_banned_and_not_registered(self):
        n = ksk_common.AUTO_BAN_THREADS_PER_SCAN
        for i in range(n):
            self._post(f"t{i}", "UC_spam", "!ksk", published_at=1_000_000 + i)
        sync.check_ksk_commands(self.client, 1_000_060)

        self.assertIn("UC_spam", self._bans())
        active = self.client.query("SELECT thread_id FROM ksk_threads WHERE state = 'active'")
        self.assertEqual(active, [], "BANされたアカウントのスレッドは登録されない")

    def test_ban_reason_records_the_evidence(self):
        n = ksk_common.AUTO_BAN_THREADS_PER_SCAN + 2
        for i in range(n):
            self._post(f"t{i}", "UC_spam", "!ksk", published_at=1_000_000 + i)
        sync.check_ksk_commands(self.client, 1_000_060)

        # 誤検知し得るので、後から人手で判断できる根拠を残すこと
        self.assertIn(str(n), self._bans()["UC_spam"])

    def test_below_threshold_is_not_banned(self):
        for i in range(ksk_common.AUTO_BAN_THREADS_PER_SCAN - 1):
            self._post(f"t{i}", "UC_ok", "!ksk", published_at=1_000_000 + i)
        sync.check_ksk_commands(self.client, 1_000_060)

        self.assertEqual(self._bans(), {})
        active = self.client.query("SELECT thread_id FROM ksk_threads WHERE state = 'active'")
        self.assertEqual(len(active), 1, "最新の1件だけ登録される")

    def test_auto_ban_ends_already_running_thread(self):
        # 先にスレッドを走らせておき、後から連投してBANされたら追跡も止まること
        self._post("t_old", "UC_spam", "!ksk 先行", published_at=1_000_000)
        sync.check_ksk_commands(self.client, 1_000_060)
        self.assertEqual(
            self.client.query("SELECT state FROM ksk_threads WHERE thread_id='t_old'")[0]["state"],
            "active",
        )

        for i in range(ksk_common.AUTO_BAN_THREADS_PER_SCAN):
            self._post(f"t{i}", "UC_spam", "!ksk", published_at=1_000_100 + i)
        sync.check_ksk_commands(self.client, 1_000_160)

        row = self.client.query(
            "SELECT state, ended_reason FROM ksk_threads WHERE thread_id='t_old'")[0]
        self.assertEqual(row["state"], "ended")
        self.assertEqual(row["ended_reason"], "banned")


class KskTriggerWindowBanTest(unittest.TestCase):
    """DETECT_WINDOW_MIN(5分)の単発スキャンには収まらない、もう少し長い
    TRIGGER_BAN_WINDOW_MIN(10分)の絶対件数での自動BAN(2026-08-04追加)。

    5分ごとのスキャンでは閾値未満に見える(1分1件ペースの連投など)が、
    10分通算では TRIGGER_BAN_THRESHOLD 件に達するケースを狙う。
    """

    def setUp(self):
        self.client = SqliteTurso()
        self.addCleanup(self.client.db.close)
        self.client.execute(
            "CREATE TABLE comments (comment_id TEXT PRIMARY KEY, parent_id TEXT, "
            "author_channel_id TEXT, handle TEXT, published_at INTEGER, text TEXT, "
            "is_deleted INTEGER DEFAULT 0)"
        )

    def _post(self, comment_id, author, text, parent_id=None, published_at=1_000_000):
        self.client.execute(
            "INSERT INTO comments(comment_id,parent_id,author_channel_id,handle,published_at,text) "
            "VALUES(?,?,?,?,?,?)",
            [comment_id, parent_id, author, "@" + author, published_at, text],
        )

    def _bans(self):
        return {r["channel_id"]: r["reason"]
                for r in self.client.query("SELECT channel_id, reason FROM ksk_bans")}

    def test_spread_out_spam_is_banned_by_the_10min_absolute_count(self):
        # 直近5分スキャン(DETECT_WINDOW_MIN)には1件しか入らないが、10分通算では
        # ちょうど閾値(TRIGGER_BAN_THRESHOLD)件になるように仕込む。
        now = 1_000_600
        n_old = ksk_common.TRIGGER_BAN_THRESHOLD - 1
        for i in range(n_old):
            self._post(f"old{i}", "UC_spam", "!ksk", published_at=1_000_000 + i)
        # 直近5分スキャンに入る1件だけを追加(now-300 以降)
        self._post("recent", "UC_spam", "!ksk", published_at=1_000_310)

        sync.check_ksk_commands(self.client, now)

        self.assertIn("UC_spam", self._bans())
        self.assertIn(str(ksk_common.TRIGGER_BAN_THRESHOLD), self._bans()["UC_spam"])
        active = self.client.query("SELECT thread_id FROM ksk_threads WHERE state = 'active'")
        self.assertEqual(active, [], "BANされたアカウントのスレッドは登録されない")

    def test_below_the_10min_threshold_is_not_banned(self):
        now = 1_000_600
        n_old = ksk_common.TRIGGER_BAN_THRESHOLD - 2
        for i in range(n_old):
            self._post(f"old{i}", "UC_ok", "!ksk", published_at=1_000_000 + i)
        self._post("recent", "UC_ok", "!ksk", published_at=1_000_310)

        sync.check_ksk_commands(self.client, now)

        self.assertEqual(self._bans(), {})
        active = self.client.query("SELECT thread_id FROM ksk_threads WHERE state = 'active'")
        self.assertEqual([r["thread_id"] for r in active], ["recent"])


class KskDeadRatioTrackingTest(unittest.TestCase):
    """run_ksk_tracking() が Pass1 の消滅判定に安全弁を持つことを固定する(2026-08-03追加)。

    _ksk_pass1 は本体同期の _pass1_reply_counts と違い _MAX_DEAD_RATIO 相当の
    保険を持たない。commentThreads.list(id=) が正常応答(nextPageTokenも無し)でも、
    依頼したIDの一部が理由不明で欠けることがあり、それをそのまま信用すると
    生きているスレッドを ended(deleted) にしてしまう — 一度 ended にすると
    同じ comment_id は二度と再登録できないため、実害が大きい。
    """

    def setUp(self):
        self.client = SqliteTurso()
        self.addCleanup(self.client.db.close)
        self.client.execute(
            "CREATE TABLE comments (comment_id TEXT PRIMARY KEY, parent_id TEXT, "
            "author_channel_id TEXT, handle TEXT, published_at INTEGER, "
            "like_count INTEGER, is_pinned INTEGER DEFAULT 0, is_deleted INTEGER DEFAULT 0, "
            "deleted_confirmed_at INTEGER, fetched_at INTEGER, reply_order INTEGER, "
            "thread_published_at INTEGER, original_text TEXT, text TEXT)"
        )
        ksk_common.ensure_schema(self.client)

    def _register(self, thread_id, owner):
        ksk_common.register_thread(
            self.client, thread_id, owner, "@x", None, 1_000_000, 1_000_000,
        )

    def _states(self):
        rows = self.client.query("SELECT thread_id, state FROM ksk_threads")
        return {r["thread_id"]: r["state"] for r in rows}

    def _run(self, alive_ids, counts):
        # deadline=0 で should_pass2 を常に False にし、Pass2(HTTP呼び出しが必要)を
        # 経由せずに Pass1 の削除判定ロジックだけを検証する。
        with patch.object(sync, "_get_ksk_youtube", return_value=object()), \
             patch.object(sync, "_ksk_pass1", return_value=(set(alive_ids), counts, True)):
            return sync.run_ksk_tracking(self.client, 1_000_100, deadline=0)

    def test_high_dead_ratio_skips_deletion(self):
        n = ksk_common.KSK_MIN_BATCH_FOR_DEAD_RATIO_GUARD + 3
        ids = [f"Ugy_{i}" for i in range(n)]
        for tid in ids:
            self._register(tid, owner=tid)  # 1アカウント1スレッド上限を回避するため別アカウント

        # 応答に1件しか含まれない(ほぼ全滅) = 異常値として削除判定をスキップすべき
        self._run(alive_ids=[ids[0]], counts={ids[0]: 5})

        states = self._states()
        for tid in ids[1:]:
            self.assertEqual(states[tid], "active", f"{tid} は削除判定されないはず")

    def test_low_dead_ratio_still_marks_deleted(self):
        n = ksk_common.KSK_MIN_BATCH_FOR_DEAD_RATIO_GUARD + 3
        ids = [f"Ugy_{i}" for i in range(n)]
        for tid in ids:
            self._register(tid, owner=tid)

        # 1件だけ本当に消えた(正常な削除検知) = 従来どおり削除マーキングする
        self._run(alive_ids=ids[1:], counts={tid: 5 for tid in ids[1:]})

        self.assertEqual(self._states()[ids[0]], "ended")

    def test_small_batch_is_not_ratio_guarded(self):
        # KSK_MIN_BATCH_FOR_DEAD_RATIO_GUARD 未満では比率判定自体が無意味なので、
        # 全滅でも従来どおり削除マーキングする(直接のsignalを信用する)
        ids = ["Ugy_a", "Ugy_b"]
        for tid in ids:
            self._register(tid, owner=tid)

        self._run(alive_ids=[], counts={})

        states = self._states()
        self.assertEqual(states["Ugy_a"], "ended")
        self.assertEqual(states["Ugy_b"], "ended")


class CheckDormantKskThreadsTest(unittest.TestCase):
    """check_dormant_ksk_threads() が ended スレッドの復活を検知することを固定する
    (2026-08-04追加)。Pass2はactiveにしか走らないため、これが無いとアイドル
    タイムアウト後に遅れて付いた返信に永久に気づけない。
    """

    def setUp(self):
        self.client = SqliteTurso()
        self.addCleanup(self.client.db.close)
        self.client.execute(
            "CREATE TABLE comments (comment_id TEXT PRIMARY KEY, parent_id TEXT, "
            "author_channel_id TEXT, handle TEXT, published_at INTEGER, "
            "like_count INTEGER, is_pinned INTEGER DEFAULT 0, is_deleted INTEGER DEFAULT 0, "
            "deleted_confirmed_at INTEGER, fetched_at INTEGER, reply_order INTEGER, "
            "thread_published_at INTEGER, original_text TEXT, text TEXT)"
        )
        ksk_common.ensure_schema(self.client)

    def _end_thread(self, thread_id, owner, last_reply_count, started_at=1_000_000):
        ksk_common.register_thread(self.client, thread_id, owner, "@x", None, started_at, started_at)
        ksk_common.update_pass1_progress(self.client, thread_id, last_reply_count, started_at, True)
        ksk_common.end_thread(self.client, thread_id, ksk_common.REASON_IDLE, started_at + 10)

    def _add_replies(self, thread_id, n, start_ts=2_000_000):
        for i in range(n):
            self.client.execute(
                "INSERT INTO comments(comment_id,parent_id,author_channel_id,handle,published_at,is_deleted) "
                "VALUES(?,?,?,?,?,0)",
                [f"{thread_id}_r{i}", thread_id, "UC_replier", "@replier", start_ts + i],
            )

    def _row(self, thread_id):
        return self.client.query("SELECT * FROM ksk_threads WHERE thread_id=?", [thread_id])[0]

    def test_no_growth_does_nothing(self):
        self._end_thread("Ugy_a", "UC_owner", last_reply_count=0)
        n = sync.check_dormant_ksk_threads(self.client, 3_000_000)
        self.assertEqual(n, 0)
        self.assertEqual(self._row("Ugy_a")["state"], "ended")

    def test_small_growth_reaggregates_but_stays_ended(self):
        self._end_thread("Ugy_a", "UC_owner", last_reply_count=0)
        self._add_replies("Ugy_a", ksk_common.DORMANT_REACTIVATE_THRESHOLD - 1)

        n = sync.check_dormant_ksk_threads(self.client, 3_000_000)

        self.assertEqual(n, 1)
        row = self._row("Ugy_a")
        self.assertEqual(row["state"], "ended")
        self.assertEqual(row["last_reply_count"], ksk_common.DORMANT_REACTIVATE_THRESHOLD - 1)
        stats = self.client.query("SELECT payload FROM ksk_thread_stats WHERE thread_id=?", ["Ugy_a"])
        self.assertEqual(len(stats), 1, "再集計でpayloadが書かれているはず")

    def test_large_growth_reactivates_and_resets_growth_clock(self):
        self._end_thread("Ugy_a", "UC_owner", last_reply_count=0)
        self._add_replies("Ugy_a", ksk_common.DORMANT_REACTIVATE_THRESHOLD)

        n = sync.check_dormant_ksk_threads(self.client, 3_000_000)

        self.assertEqual(n, 1)
        row = self._row("Ugy_a")
        self.assertEqual(row["state"], "active")
        self.assertIsNone(row["ended_reason"])
        self.assertIsNone(row["ended_at"])
        # last_growth_at を復活時刻に更新していないと、次のPass1のアイドル判定が
        # 古い値のままで即座に ended(idle) へ逆戻りしてしまう
        self.assertEqual(row["last_growth_at"], 3_000_000)

    def test_reactivation_respects_max_active_threads(self):
        # 既に上限まで active が埋まっていたら、閾値を超えていても再集計だけに留める
        for i in range(ksk_common.MAX_ACTIVE_THREADS):
            ksk_common.register_thread(
                self.client, f"Ugy_active_{i}", f"UC_active_{i}", "@x", None, 1_000_000, 1_000_000,
            )
        self._end_thread("Ugy_a", "UC_owner", last_reply_count=0)
        self._add_replies("Ugy_a", ksk_common.DORMANT_REACTIVATE_THRESHOLD)

        sync.check_dormant_ksk_threads(self.client, 3_000_000)

        self.assertEqual(self._row("Ugy_a")["state"], "ended",
                          "枠が無いので active に戻さず再集計だけのはず")

    def test_only_checks_recent_ended_threads(self):
        # DORMANT_CHECK_LIMIT を超える古い ended は対象外(get_recent_threads と同じ範囲)
        for i in range(ksk_common.DORMANT_CHECK_LIMIT + 2):
            self._end_thread(f"Ugy_{i}", f"UC_{i}", last_reply_count=0, started_at=1_000_000 + i)
        n = sync.check_dormant_ksk_threads(self.client, 3_000_000)
        self.assertEqual(n, 0)  # 誰も返信が増えていないので0件だが、例外なく完走することを確認


if __name__ == "__main__":
    unittest.main()
