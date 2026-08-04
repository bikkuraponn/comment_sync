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

    def test_bare_owari_word_stops(self):
        # スマホから記号を打つのが面倒なので「終了」だけでも止められる
        self.assertEqual(K.parse_command("終了"), {"action": "stop"})
        self.assertEqual(K.parse_command("  終了  "), {"action": "stop"})
        self.assertEqual(K.parse_command("終了！"), {"action": "stop"})
        self.assertEqual(K.parse_command("終了。"), {"action": "stop"})
        self.assertEqual(K.parse_command("終了w"), {"action": "stop"})
        # 複数行のうち1行が「終了」でもよい
        self.assertEqual(K.parse_command("おつかれ\n終了"), {"action": "stop"})

    def test_extra_trigger_phrases_register(self):
        for phrase in K.EXTRA_TRIGGER_PHRASES:
            with self.subTest(phrase=phrase):
                self.assertEqual(
                    K.parse_command(phrase), {"action": "start", "title": None})

    def test_extra_trigger_phrase_embedded_in_a_sentence(self):
        self.assertEqual(
            K.parse_command("今日は加速チャレンジやります！"),
            {"action": "start", "title": None},
        )
        self.assertEqual(
            K.parse_command("みんなで連投チャレンジしよう"),
            {"action": "start", "title": None},
        )

    def test_extra_trigger_phrase_title_is_always_none(self):
        # !ksk と違い、後ろの文字列をタイトルとして拾わない
        got = K.parse_command("kskチャレンジ 深夜の部やります")
        self.assertIsNone(got["title"])

    def test_owari_inside_a_sentence_does_not_stop(self):
        # 「終了」は普通の日本語なので、行全体が終了のときだけ発火させる。
        # 含むだけで止めると何気ない発言でスレッドが死に、復活もできない
        self.assertIsNone(K.parse_command("そろそろ終了かな"))
        self.assertIsNone(K.parse_command("終了までもう少し"))
        self.assertIsNone(K.parse_command("終了したくない"))

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
        extra = 4
        n = K.TOP_ACCOUNTS + extra
        q1 = self._q1(n, per=10)  # 上位 TOP_ACCOUNTS のみ accounts に individual で載る
        payload = K.build_payload(self._thread(), q1, [], [], {}, now_epoch=1_000_600)
        self.assertEqual(len(payload["accounts"]), K.TOP_ACCOUNTS)
        self.assertEqual(payload["others"], {"count": extra * 10, "accounts": extra})
        self.assertEqual(payload["unique_accounts"], n)
        self.assertAlmostEqual(payload["accounts"][0]["share"], 10 / (n * 10))

    def test_all_accounts_includes_everyone_not_just_top(self):
        extra = 4
        n = K.TOP_ACCOUNTS + extra
        q1 = self._q1(n, per=10)
        payload = K.build_payload(self._thread(), q1, [], [], {}, now_epoch=1)
        self.assertEqual(len(payload["all_accounts"]), n)
        self.assertEqual(
            {a["channel_id"] for a in payload["all_accounts"]},
            {r["author_channel_id"] for r in q1},
        )

    def test_all_accounts_have_peak_per_min_even_beyond_top(self):
        # peak_per_min は表(全員)にも使うので、上位に入らないアカウントでも
        # ちゃんと計算されていること(以前は上位ぶんしか計算していなかった)
        extra = 2
        n = K.TOP_ACCOUNTS + extra
        q1 = self._q1(n, per=1)
        last_cid = q1[-1]["author_channel_id"]
        q2 = [{"m": 100, "author_channel_id": last_cid, "c": 9}]
        payload = K.build_payload(self._thread(), q1, q2, [], {}, now_epoch=1)
        entry = next(a for a in payload["all_accounts"] if a["channel_id"] == last_cid)
        self.assertEqual(entry["peak_per_min"], 9)
        # 円グラフ用の accounts には(上位ではないので)含まれない
        self.assertNotIn(last_cid, [a["channel_id"] for a in payload["accounts"]])

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

    def test_last_reply_at_is_max_across_all_accounts_not_just_top(self):
        # 「全体の分速」の分母に使うため、上位TOP_ACCOUNTSに入らないアカウントの
        # 返信時刻も見落とさないこと
        q1 = self._q1(K.TOP_ACCOUNTS + 3, per=5)
        q1[-1]["last_at"] = 9_999_999  # 上位に入らない最後のアカウントが実は一番新しい
        payload = K.build_payload(self._thread(), q1, [], [], {}, now_epoch=1)
        self.assertEqual(payload["last_reply_at"], 9_999_999)

    def test_last_reply_at_is_none_when_no_replies(self):
        payload = K.build_payload(self._thread(), [], [], [], {}, now_epoch=1)
        self.assertIsNone(payload["last_reply_at"])
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

    def test_thread_replies_query_takes_only_parent_id(self):
        # author_channel_id を条件に足すと idx_comments_author_published が選ばれ、
        # 「このスレッドの返信」ではなく「そのアカウントの全履歴」を舐めてしまう
        # (ksk_common の集計セクションの注記参照)
        self.assertEqual(K.Q_THREAD_REPLIES_SQL.count("?"), 1)
        self.assertIn("parent_id = ?", K.Q_THREAD_REPLIES_SQL)
        self.assertNotIn("author_channel_id =", K.Q_THREAD_REPLIES_SQL)
        self.assertNotIn("author_channel_id IN", K.Q_THREAD_REPLIES_SQL)

    def test_thread_replies_query_orders_by_index_column(self):
        # idx_parent_published (parent_id, published_at) の索引順そのものなので
        # 追加ソートが発生しない。別の列で並べ替えると TEMP B-TREE が入る
        self.assertIn("ORDER BY published_at", K.Q_THREAD_REPLIES_SQL)


class _CountingTurso:
    """query() の呼び出し回数を数える偽Turso。

    aggregate_thread() が **スレッドあたり1クエリ** に収まっていることを固定する。
    以前は Q1/Q2/Q3 + 中央値×8 の計12本を投げており、どれも
    `WHERE parent_id = ?` でスレッド全体を舐め直すため、1000返信のスレッドで
    約12,000 rows_read/回になっていた(2026-08-03のレビューで発覚)。
    課金されるのは返る行数ではなくスキャン行数なので、クエリ本数がそのまま効く。
    """

    def __init__(self, rows):
        self._rows = rows
        self.calls = []

    def query(self, sql, args=None):
        self.calls.append((sql, args))
        return self._rows


def _reply(cid, ts, handle=None):
    return {"author_channel_id": cid, "handle": handle or ("@" + cid), "published_at": ts}


class AggregateThreadTest(unittest.TestCase):
    def test_issues_exactly_one_query(self):
        turso = _CountingTurso([_reply("UC_a", 100), _reply("UC_a", 110)])
        K.aggregate_thread(turso, "Ugy_test")
        self.assertEqual(len(turso.calls), 1, "スレッドあたり1クエリに収めること")
        self.assertEqual(turso.calls[0][1], ["Ugy_test"])


class AggregateRowsTest(unittest.TestCase):
    def test_counts_and_first_last(self):
        q1, _q2, _gaps, _medians = K.aggregate_rows([
            _reply("UC_a", 100), _reply("UC_b", 105), _reply("UC_a", 130),
        ])
        by_cid = {r["author_channel_id"]: r for r in q1}
        self.assertEqual(by_cid["UC_a"]["c"], 2)
        self.assertEqual(by_cid["UC_a"]["first_at"], 100)
        self.assertEqual(by_cid["UC_a"]["last_at"], 130)
        self.assertEqual(by_cid["UC_b"]["c"], 1)

    def test_sorted_by_count_desc(self):
        q1, _q2, _gaps, _medians = K.aggregate_rows([
            _reply("UC_a", 100), _reply("UC_b", 101), _reply("UC_b", 102),
        ])
        self.assertEqual([r["author_channel_id"] for r in q1], ["UC_b", "UC_a"])

    def test_gap_stats(self):
        # 間隔: 10, 0, 20 → 平均10, 最速(0除く)10, 同秒1組
        _q1, _q2, gaps, medians = K.aggregate_rows([
            _reply("UC_a", 100), _reply("UC_a", 110),
            _reply("UC_a", 110), _reply("UC_a", 130),
        ])
        g = gaps[0]
        self.assertEqual(g["gap_n"], 3)
        self.assertEqual(g["min_gap"], 10)
        self.assertEqual(g["same_second_pairs"], 1)
        self.assertAlmostEqual(g["avg_gap"], 10.0)
        # ソート済み [0,10,20] の下位中央値 = 10
        self.assertEqual(medians["UC_a"], 10)

    def test_median_is_computed_for_every_account_not_just_top(self):
        # 複数アカウントでの連投を見るのが目的なので、2番手以降も必ず出す
        rows = []
        for i in range(12):
            cid = f"UC_{i:02d}"
            # 上位ほど投稿数が多くなるようにする
            for k in range(12 - i + 1):
                rows.append(_reply(cid, 1000 + k * 5))
        _q1, _q2, _gaps, medians = K.aggregate_rows(rows)
        self.assertEqual(len(medians), 12, "全アカウント分の中央値が出ること")

    def test_all_same_second_leaves_min_gap_none(self):
        _q1, _q2, gaps, medians = K.aggregate_rows([
            _reply("UC_a", 100), _reply("UC_a", 100), _reply("UC_a", 100),
        ])
        self.assertIsNone(gaps[0]["min_gap"])
        self.assertEqual(gaps[0]["same_second_pairs"], 2)
        self.assertEqual(medians["UC_a"], 0)

    def test_single_post_account_has_no_gap_row(self):
        _q1, _q2, gaps, medians = K.aggregate_rows([_reply("UC_a", 100)])
        self.assertEqual(gaps, [])
        self.assertEqual(medians, {})

    def test_minute_buckets(self):
        _q1, q2, _gaps, _medians = K.aggregate_rows([
            _reply("UC_a", 600), _reply("UC_a", 630), _reply("UC_b", 660),
        ])
        self.assertIn({"m": 10, "author_channel_id": "UC_a", "c": 2}, q2)
        self.assertIn({"m": 11, "author_channel_id": "UC_b", "c": 1}, q2)

    def test_unsorted_input_does_not_produce_negative_gaps(self):
        _q1, _q2, gaps, _medians = K.aggregate_rows([
            _reply("UC_a", 130), _reply("UC_a", 100), _reply("UC_a", 110),
        ])
        self.assertEqual(gaps[0]["min_gap"], 10)
        self.assertGreater(gaps[0]["avg_gap"], 0)

    def test_empty(self):
        q1, q2, gaps, medians = K.aggregate_rows([])
        self.assertEqual((q1, q2, gaps, medians), ([], [], [], {}))


def _cand(comment_id, author, ts, title=None, parent_id=None, action=K.ACTION_START):
    row = {"comment_id": comment_id, "author_channel_id": author,
           "published_at": ts, "parent_id": parent_id}
    cmd = {"action": action} if action == K.ACTION_STOP else {"action": action, "title": title}
    return (row, cmd)


class SelectStartCandidatesTest(unittest.TestCase):
    def test_newest_wins_for_the_same_account(self):
        # 短時間に連続で打つのは打ち直しなので、最後のものを採用する
        selected, counts = K.select_start_candidates([
            _cand("t1", "UC_a", 100, title="1本目"),
            _cand("t2", "UC_a", 200, title="2本目"),
            _cand("t3", "UC_a", 150, title="間の1本"),
        ])
        self.assertEqual([r["comment_id"] for r, _c in selected], ["t2"])
        self.assertEqual(selected[0][1]["title"], "2本目")
        self.assertEqual(counts, {"UC_a": 3})

    def test_different_accounts_all_kept(self):
        selected, counts = K.select_start_candidates([
            _cand("t1", "UC_a", 100),
            _cand("t2", "UC_b", 110),
        ])
        self.assertEqual({r["comment_id"] for r, _c in selected}, {"t1", "t2"})
        self.assertEqual(counts, {"UC_a": 1, "UC_b": 1})

    def test_replies_and_stops_are_excluded(self):
        selected, counts = K.select_start_candidates([
            _cand("r1", "UC_a", 100, parent_id="t1"),          # 返信の登録コマンドは無効
            _cand("r2", "UC_a", 110, parent_id="t1", action=K.ACTION_STOP),
            _cand("t9", "UC_a", 120),                            # これだけ有効
        ])
        self.assertEqual([r["comment_id"] for r, _c in selected], ["t9"])
        self.assertEqual(counts, {"UC_a": 1})

    def test_rows_without_author_are_skipped(self):
        selected, counts = K.select_start_candidates([_cand("t1", None, 100)])
        self.assertEqual(selected, [])
        self.assertEqual(counts, {})

    def test_selected_is_sorted_by_published_at(self):
        selected, _counts = K.select_start_candidates([
            _cand("t1", "UC_b", 300),
            _cand("t2", "UC_a", 100),
        ])
        self.assertEqual([r["comment_id"] for r, _c in selected], ["t2", "t1"])


class AutoBanTargetsTest(unittest.TestCase):
    def test_at_threshold_is_banned(self):
        counts = {"UC_a": K.AUTO_BAN_THREADS_PER_SCAN}
        self.assertEqual(K.auto_ban_targets(counts), ["UC_a"])

    def test_below_threshold_is_not_banned(self):
        counts = {"UC_a": K.AUTO_BAN_THREADS_PER_SCAN - 1}
        self.assertEqual(K.auto_ban_targets(counts), [])

    def test_only_offending_accounts_returned(self):
        counts = {"UC_a": K.AUTO_BAN_THREADS_PER_SCAN + 5, "UC_b": 1}
        self.assertEqual(K.auto_ban_targets(counts), ["UC_a"])


class DeadRatioIsSuspectTest(unittest.TestCase):
    def test_small_batch_always_trusted(self):
        # KSK_MIN_BATCH_FOR_DEAD_RATIO_GUARD 未満は比率判定そのものが無意味なので、
        # 100%消滅でも「怪しい」とは判定しない(直接のsignalを信用する)
        self.assertFalse(K.dead_ratio_is_suspect(1, 1))
        self.assertFalse(K.dead_ratio_is_suspect(4, 4))

    def test_large_batch_high_ratio_is_suspect(self):
        self.assertTrue(K.dead_ratio_is_suspect(20, 15))

    def test_large_batch_low_ratio_is_trusted(self):
        self.assertFalse(K.dead_ratio_is_suspect(20, 2))

    def test_boundary_at_guard_floor(self):
        self.assertTrue(K.dead_ratio_is_suspect(K.KSK_MIN_BATCH_FOR_DEAD_RATIO_GUARD, 3))


class ClassifyDormantGrowthTest(unittest.TestCase):
    def test_no_growth_is_none(self):
        self.assertEqual(K.classify_dormant_growth(0), "none")
        self.assertEqual(K.classify_dormant_growth(-1), "none")

    def test_small_growth_is_reaggregate(self):
        self.assertEqual(K.classify_dormant_growth(1), "reaggregate")
        self.assertEqual(K.classify_dormant_growth(K.DORMANT_REACTIVATE_THRESHOLD - 1), "reaggregate")

    def test_large_growth_is_reactivate(self):
        self.assertEqual(K.classify_dormant_growth(K.DORMANT_REACTIVATE_THRESHOLD), "reactivate")
        self.assertEqual(K.classify_dormant_growth(K.DORMANT_REACTIVATE_THRESHOLD + 100), "reactivate")


if __name__ == "__main__":
    unittest.main()
