import unittest
from datetime import datetime

import monthly_sweep


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


if __name__ == "__main__":
    unittest.main()
