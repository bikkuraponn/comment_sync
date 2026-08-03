"""Unit tests for ksk_common's pure functions (no Turso, no YouTube API)."""
import unittest

import ksk_common as K


class ParseCommandTest(unittest.TestCase):
    def test_bare_bang_form(self):
        self.assertEqual(K.parse_command("!ksk"), {"action": "start", "title": None})

    def test_bare_slash_form(self):
        self.assertEqual(K.parse_command("/ksk"), {"action": "start", "title": None})

    def test_fullwidth_is_normalized_by_nfkc(self):
        self.assertEqual(K.parse_command("！ｋｓｋ"), {"action": "start", "title": None})
        self.assertEqual(K.parse_command("／ｋｓｋ"), {"action": "start", "title": None})

    def test_case_insensitive(self):
        self.assertEqual(K.parse_command("!KSK"), {"action": "start", "title": None})

    def test_title_is_captured(self):
        self.assertEqual(
            K.parse_command("!ksk 今日は加速するぞ"),
            {"action": "start", "title": "今日は加速するぞ"},
        )

    def test_title_is_truncated(self):
        long_title = "あ" * 100
        got = K.parse_command(f"!ksk {long_title}")
        self.assertEqual(len(got["title"]), K.TITLE_MAX_LEN)

    def test_stop_form(self):
        self.assertEqual(K.parse_command("!ksk stop"), {"action": "stop"})
        self.assertEqual(K.parse_command("/ksk STOP"), {"action": "stop"})
        self.assertEqual(K.parse_command("終わり /ksk stop!"), {"action": "stop"})

    def test_word_boundary_after_ksk(self):
        # `!kskstop` は ksk の直後が英数字なのでコマンド語ではない
        self.assertIsNone(K.parse_command("!kskstop"))
        self.assertIsNone(K.parse_command("/ksk2"))

    def test_command_anywhere_in_the_text(self):
        # 仕様は「本文に含まれていれば登録」。行頭限定ではない
        self.assertEqual(
            K.parse_command("加速するぞ /ksk"), {"action": "start", "title": None}
        )
        self.assertEqual(
            K.parse_command("こんばんは\n!ksk"), {"action": "start", "title": None}
        )

    def test_title_is_the_rest_of_the_same_line(self):
        self.assertEqual(
            K.parse_command("\n\n  !ksk 加速  \n2行目は無視"),
            {"action": "start", "title": "加速"},
        )
        self.assertEqual(
            K.parse_command("今から\n始めます /ksk 夜の部"),
            {"action": "start", "title": "夜の部"},
        )

    def test_mention_also_registers_by_design(self):
        # A案の受け入れたトレードオフ: 引用・言及でも登録される。
        # 誤登録は stop / アイドル自動終了 / BAN で回収する
        self.assertEqual(
            K.parse_command("さっき !ksk って言ってた人だれ"),
            {"action": "start", "title": "って言ってた人だれ"},
        )

    def test_url_does_not_trigger(self):
        self.assertIsNone(K.parse_command("https://example.com/ksk"))
        self.assertIsNone(K.parse_command("見て https://youtu.be/ksk"))

    def test_plain_word_without_prefix_is_ignored(self):
        self.assertIsNone(K.parse_command("ksk"))
        self.assertIsNone(K.parse_command("加速 ksk するぞ"))

    def test_none_and_empty(self):
        # 削除済み行の text は NULL
        self.assertIsNone(K.parse_command(None))
        self.assertIsNone(K.parse_command(""))
        self.assertIsNone(K.parse_command("   \n  \n"))
        self.assertIsNone(K.parse_command(123))


class WindowTest(unittest.TestCase):
    def test_detect_window_bounds(self):
        start, end = K.detect_window_bounds(1_000_000)
        self.assertEqual(end, 1_000_000)
        self.assertEqual(end - start, K.DETECT_WINDOW_MIN * 60)


class EndReasonTest(unittest.TestCase):
    def _thread(self, **kw):
        base = {
            "thread_id": "Ugy_test",
            "started_at": 1_000_000,
            "last_reply_count": 10,
            "last_growth_at": 1_000_000,
        }
        base.update(kw)
        return base

    def test_cap_wins(self):
        self.assertEqual(
            K.evaluate_end_reason(self._thread(), K.REPLY_CAP, 1_000_060),
            K.REASON_CAP,
        )

    def test_timeout(self):
        now = 1_000_000 + K.MAX_DURATION_SEC
        self.assertEqual(K.evaluate_end_reason(self._thread(), 11, now), K.REASON_TIMEOUT)

    def test_idle_when_count_did_not_grow(self):
        now = 1_000_000 + K.IDLE_TIMEOUT_SEC
        self.assertEqual(K.evaluate_end_reason(self._thread(), 10, now), K.REASON_IDLE)

    def test_growth_keeps_it_alive(self):
        now = 1_000_000 + K.IDLE_TIMEOUT_SEC
        self.assertIsNone(K.evaluate_end_reason(self._thread(), 11, now))

    def test_still_alive_before_idle_timeout(self):
        now = 1_000_000 + K.IDLE_TIMEOUT_SEC - 1
        self.assertIsNone(K.evaluate_end_reason(self._thread(), 10, now))

    def test_missing_last_growth_at_falls_back_to_started_at(self):
        t = self._thread()
        del t["last_growth_at"]
        now = 1_000_000 + K.IDLE_TIMEOUT_SEC
        self.assertEqual(K.evaluate_end_reason(t, 10, now), K.REASON_IDLE)


class Pass2ScheduleTest(unittest.TestCase):
    def test_interval_scales_with_reply_count(self):
        self.assertEqual(K.pass2_interval_min(0), 1)
        self.assertEqual(K.pass2_interval_min(50), 1)
        self.assertEqual(K.pass2_interval_min(100), 1)
        self.assertEqual(K.pass2_interval_min(150), 2)
        self.assertEqual(K.pass2_interval_min(450), 5)
        # 上限は5分で頭打ち
        self.assertEqual(K.pass2_interval_min(1000), 5)

    def test_due_when_never_run(self):
        self.assertTrue(K.pass2_due({"last_pass2_at": None}, 10, 1_000_000))

    def test_not_due_before_interval(self):
        self.assertFalse(K.pass2_due({"last_pass2_at": 1_000_000}, 900, 1_000_000 + 299))

    def test_due_after_interval(self):
        self.assertTrue(K.pass2_due({"last_pass2_at": 1_000_000}, 900, 1_000_000 + 300))


class BuildSpeedTest(unittest.TestCase):
    def test_empty(self):
        speed, peak = K.build_speed([], [])
        self.assertEqual(speed["total"], [])
        self.assertEqual(peak, {})

    def test_buckets_and_peak(self):
        rows = [
            {"m": 100, "author_channel_id": "A", "c": 3},
            {"m": 100, "author_channel_id": "B", "c": 1},
            {"m": 102, "author_channel_id": "A", "c": 5},
        ]
        speed, peak = K.build_speed(rows, ["A"])
        self.assertEqual(speed["bucket_sec"], 60)
        self.assertEqual(speed["t0"], 100 * 60)
        # 分101はデータが無いので0埋めされる
        self.assertEqual(speed["total"], [4, 0, 5])
        self.assertEqual(speed["by_account"], {"A": [3, 0, 5]})
        self.assertEqual(peak["A"], 5)
        self.assertEqual(peak["B"], 1)

    def test_downsamples_long_series(self):
        n = K.SPEED_MINUTE_BUCKET_LIMIT + 10
        rows = [{"m": 1000 + i, "author_channel_id": "A", "c": 1} for i in range(n)]
        speed, _ = K.build_speed(rows, ["A"])
        self.assertEqual(speed["bucket_sec"], 300)
        self.assertEqual(sum(speed["total"]), n)
        self.assertEqual(sum(speed["by_account"]["A"]), n)


class BuildPayloadTest(unittest.TestCase):
    def _thread(self):
        return {
            "thread_id": "Ugy_test",
            "title": "加速",
            "owner_channel_id": "UC_owner",
            "owner_handle": "@owner",
            "state": K.STATE_ACTIVE,
            "started_at": 1_000_000,
            "ended_at": None,
            "ended_reason": None,
        }

    def _q1(self, n_accounts, per=10):
        return [
            {"author_channel_id": f"UC_{i}", "handle": f"@u{i}", "c": per,
             "first_at": 1_000_000, "last_at": 1_000_600}
            for i in range(n_accounts)
        ]

    def test_shares_and_others(self):
        q1 = self._q1(12, per=10)  # 合計120件、上位8のみ individual
        payload = K.build_payload(self._thread(), q1, [], [], {}, now_epoch=1_000_600)
        self.assertEqual(len(payload["accounts"]), K.TOP_ACCOUNTS)
        self.assertEqual(payload["others"], {"count": 40, "accounts": 4})
        self.assertEqual(payload["unique_accounts"], 12)
        self.assertAlmostEqual(payload["accounts"][0]["share"], 10 / 120)

    def test_remaining_and_discrepancy(self):
        q1 = self._q1(1, per=100)
        payload = K.build_payload(
            self._thread(), q1, [], [], {}, total_reply_count=105, now_epoch=1_000_600
        )
        self.assertEqual(payload["reply_count"], 105)
        self.assertEqual(payload["remaining"], K.REPLY_CAP - 105)
        # YouTube側105件 vs 取得できた100件
        self.assertEqual(payload["count_discrepancy"], 5)

    def test_remaining_never_negative(self):
        q1 = self._q1(1, per=K.REPLY_CAP + 50)
        payload = K.build_payload(self._thread(), q1, [], [], {}, now_epoch=1)
        self.assertEqual(payload["remaining"], 0)

    def test_gap_stats_are_merged(self):
        q1 = self._q1(1, per=10)
        gap_rows = [{
            "author_channel_id": "UC_0", "avg_gap": 6.5,
            "min_gap": 3, "same_second_pairs": 4, "gap_n": 9,
        }]
        payload = K.build_payload(
            self._thread(), q1, [], gap_rows, {"UC_0": 7}, now_epoch=1
        )
        acc = payload["accounts"][0]
        self.assertEqual(acc["median_gap_sec"], 7)
        self.assertEqual(acc["min_gap_sec"], 3)
        self.assertEqual(acc["same_second_pairs"], 4)

    def test_empty_thread_does_not_divide_by_zero(self):
        payload = K.build_payload(self._thread(), [], [], [], {}, now_epoch=1)
        self.assertEqual(payload["reply_count"], 0)
        self.assertEqual(payload["accounts"], [])
        self.assertEqual(payload["others"], {"count": 0, "accounts": 0})


class BuildIndexPayloadTest(unittest.TestCase):
    def test_shape(self):
        payload = K.build_index_payload([{
            "thread_id": "Ugy_a", "title": "t", "owner_handle": "@o",
            "owner_channel_id": "UC_o", "state": K.STATE_ACTIVE,
            "started_at": 1_000_000, "ended_at": None, "ended_reason": None,
            "last_reply_count": 42, "unique_accounts": 3, "updated_at": 1_000_600,
        }], now_epoch=1_000_600)
        self.assertEqual(payload["generated_at"], 1_000_600)
        self.assertEqual(payload["threads"][0]["reply_count"], 42)


class SqlShapeTest(unittest.TestCase):
    """索引選択を左右する書き方が壊れていないことを固定する。

    実際の EXPLAIN QUERY PLAN は本番Tursoでしか取れないので、ここでは
    「なぜその書き方なのか」が失われないよう形だけを守る。
    """

    def test_gap_stats_takes_only_parent_id(self):
        # author_channel_id で絞ると idx_comments_author_published が選ばれ、
        # そのアカウントの全履歴を舐めてしまう(モジュール内コメント参照)
        self.assertEqual(K.Q3_GAP_STATS_SQL.count("?"), 1)
        self.assertNotIn("author_channel_id IN", K.Q3_GAP_STATS_SQL)

    def test_min_gap_excludes_same_second(self):
        self.assertIn("NULLIF(gap, 0)", K.Q3_GAP_STATS_SQL)

    def test_median_sql_suppresses_author_index(self):
        # `+` を落とすと idx_comments_author_published に切り替わる
        self.assertIn("+author_channel_id = ?", K.Q3_MEDIAN_GAP_SQL)
        self.assertIn("LIMIT 1 OFFSET ?", K.Q3_MEDIAN_GAP_SQL)


class _NoQueryBatchTurso:
    """query_batch() を持たない偽Turso。

    2026-08-03、本番で 'TursoClient' object has no attribute 'query_batch' が
    発生した(comment_sync/turso_client.py のコピーには query_batch() が無いのに
    aggregate_thread() がそれを呼んでいた — turso_client.py は複数コピー間で
    手動同期のため、flaskr側にあるからと言って comment_sync側にもあるとは限らない)。
    このクラスはあえて query_batch を実装しない = 呼んだ瞬間 AttributeError で
    落ちるので、aggregate_thread() が query_batch に依存しなくなったことを固定する。

    Q1/Q3本体はどちらも "SELECT author_channel_id," で始まり先頭一致では
    区別できないため、各クエリに固有の部分文字列でルーティングする。
    """

    def __init__(self, responses):
        self._responses = responses  # [(distinguishing substring, rows|callable), ...]
        self.calls = []

    def query(self, sql, args=None):
        self.calls.append((sql, args))
        for marker, rows in self._responses:
            if marker in sql:
                return rows(args) if callable(rows) else rows
        return []


class AggregateThreadTest(unittest.TestCase):
    def test_does_not_require_query_batch(self):
        gap_row = {"author_channel_id": "UC_a", "gap_n": 3}

        def median_rows(args):
            # args = [thread_id, channel_id, offset]
            self.assertEqual(args[1], "UC_a")
            return [{"gap": 7}]

        turso = _NoQueryBatchTurso([
            ("ORDER BY c DESC",
             [{"author_channel_id": "UC_a", "handle": "@a", "c": 5,
               "first_at": 1, "last_at": 2}]),
            ("published_at / 60", []),
            ("AVG(gap)", [gap_row]),
            ("LIMIT 1 OFFSET", median_rows),
        ])

        q1, q2, gaps, medians = K.aggregate_thread(turso, "Ugy_test")

        self.assertEqual(medians, {"UC_a": 7})
        self.assertFalse(hasattr(turso, "query_batch"))

    def test_skips_median_lookup_when_no_gap_data(self):
        # gap_n が無い(=まだ2投稿していない)アカウントは median クエリ自体を投げない
        turso = _NoQueryBatchTurso([
            ("ORDER BY c DESC",
             [{"author_channel_id": "UC_a", "handle": "@a", "c": 1,
               "first_at": 1, "last_at": 1}]),
            ("published_at / 60", []),
            ("AVG(gap)", []),
        ])

        _q1, _q2, _gaps, medians = K.aggregate_thread(turso, "Ugy_test")

        self.assertEqual(medians, {})
        median_calls = [c for c in turso.calls if "LIMIT 1 OFFSET" in c[0]]
        self.assertEqual(median_calls, [])


if __name__ == "__main__":
    unittest.main()
