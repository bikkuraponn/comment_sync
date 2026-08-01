import unittest
import sqlite3
from datetime import datetime

import monthly_sweep


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


class MonthlySweepRepairTests(unittest.TestCase):
    def test_changed_rows_detects_insert_and_delete_transition(self):
        before = {
            "kept": {"comment_id": "kept", "is_deleted": 0},
            "gone": {"comment_id": "gone", "is_deleted": 0},
        }
        after = {
            "kept": {"comment_id": "kept", "is_deleted": 0},
            "gone": {"comment_id": "gone", "is_deleted": 1},
            "new": {"comment_id": "new", "is_deleted": 0},
        }
        changed = monthly_sweep._changed_rows(before, after)
        self.assertEqual({row["comment_id"] for row in changed}, {"gone", "new"})
        self.assertEqual(len(changed), 3)

    def test_repair_work_routes_exact_date_hour_and_author(self):
        stamp = int(datetime(2026, 7, 10, 12, 34, tzinfo=monthly_sweep.JST).timestamp())
        work = monthly_sweep._repair_work([{
            "comment_id": "r1", "published_at": stamp,
            "author_channel_id": "UC123", "is_deleted": 0,
        }])
        for consumer in (
            "ranking_date", "daily_stats_date", "analytics_date",
            "wordcloud_recent7d_date", "wordcloud_recent30d_date",
        ):
            self.assertEqual(work[consumer], {"2026-07-10"})
        self.assertEqual(work["hourly_bucket"], {str((stamp // 3600) * 3600)})
        self.assertEqual(work["account_profile"], {"UC123"})
        self.assertEqual(work["network_full"], {"required"})
        self.assertEqual(work["calendar_wordcloud_full"], {"required"})
        self.assertEqual(work["account_map"], {"required"})


class MonthlySweepResumeTests(unittest.TestCase):
    def setUp(self):
        self.client = SqliteTurso()
        self.client.execute(
            "CREATE TABLE comments (comment_id TEXT PRIMARY KEY, parent_id TEXT, published_at INTEGER)"
        )
        for comment_id, parent_id, published_at in (
            ("t2", None, 100),
            ("t1", None, 100),
            ("t3", None, 200),
            ("t4", None, 300),
            ("t5", None, 400),
            ("reply", "t1", 150),
            ("outside", None, -100000),
        ):
            self.client.execute(
                "INSERT INTO comments(comment_id,parent_id,published_at) VALUES(?,?,?)",
                [comment_id, parent_id, published_at],
            )

    def test_initial_offset_and_checkpoints_use_stable_composite_cursor(self):
        state, remaining = monthly_sweep.initialize_state(
            self.client, days=1, window_end=500, resume_offset=2,
        )
        self.assertEqual(state["processed_count"], 2)
        self.assertEqual(state["total_count"], 5)
        self.assertEqual(state["cursor_comment_id"], "t2")
        self.assertEqual([row["comment_id"] for row in remaining], ["t3", "t4", "t5"])

        monthly_sweep.checkpoint_state(self.client, state, remaining[:2])
        loaded = monthly_sweep.load_state(self.client)
        self.assertEqual(loaded["processed_count"], 4)
        self.assertEqual(loaded["cursor_comment_id"], "t4")

        resumed = monthly_sweep.fetch_threads(
            self.client,
            loaded["window_start"],
            loaded["window_end"],
            loaded["cursor_published_at"],
            loaded["cursor_comment_id"],
        )
        self.assertEqual([row["comment_id"] for row in resumed], ["t5"])

        monthly_sweep.checkpoint_state(self.client, state, resumed)
        monthly_sweep.complete_state(self.client, state)
        completed = monthly_sweep.load_state(self.client)
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["processed_count"], 5)


if __name__ == "__main__":
    unittest.main()
