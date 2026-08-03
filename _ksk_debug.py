import json
import os
import time
from datetime import datetime, timezone

import dotenv
from pathlib import Path

dotenv.load_dotenv(Path("../flaskr/.env"))
from turso_client import TursoClient

c = TursoClient(os.getenv("TURSO_URL"), os.getenv("TURSO_AUTH_TOKEN"))
now = int(time.time())

with open("_ksk_debug_out.txt", "w", encoding="utf-8") as f:
    def w(s=""):
        f.write(str(s) + "\n")

    w(f"now = {now} ({datetime.fromtimestamp(now, tz=timezone.utc).isoformat()})")
    w()

    threads = c.query(
        "SELECT thread_id, owner_handle, state, ended_reason, last_reply_count, "
        "last_pass1_at, last_pass2_at, last_growth_at, resume_token, unique_accounts, "
        "started_at, updated_at FROM ksk_threads WHERE state='active' ORDER BY started_at DESC"
    )
    for t in threads:
        w(f"=== {t['thread_id']}  {t['owner_handle']} ===")
        w(f"  last_reply_count = {t['last_reply_count']}")
        w(f"  last_pass1_at    = {t['last_pass1_at']}  ({now - (t['last_pass1_at'] or now)}秒前)")
        w(f"  last_pass2_at    = {t['last_pass2_at']}  ({now - (t['last_pass2_at'] or now)}秒前)")
        w(f"  last_growth_at   = {t['last_growth_at']}  ({now - (t['last_growth_at'] or now)}秒前)")
        w(f"  unique_accounts  = {t['unique_accounts']}")
        w(f"  resume_token     = {t['resume_token']!r}")
        w()

        # 実際に comments テーブルに何件書かれているか(=fetched_count相当)
        fetched = c.query(
            "SELECT COUNT(*) AS n FROM comments WHERE parent_id = ? AND is_deleted = 0",
            [t["thread_id"]],
        )
        w(f"  comments実件数(is_deleted=0) = {fetched[0]['n']}")

        by_author = c.query(
            "SELECT author_channel_id, handle, COUNT(*) AS c FROM comments "
            "WHERE parent_id = ? AND is_deleted = 0 GROUP BY author_channel_id",
            [t["thread_id"]],
        )
        for r in by_author:
            w(f"    {r['handle']}: {r['c']}件")
        w()

        stats = c.query("SELECT payload, updated_at FROM ksk_thread_stats WHERE thread_id = ?",
                         [t["thread_id"]])
        if stats:
            p = json.loads(stats[0]["payload"])
            w(f"  ksk_thread_stats.updated_at = {stats[0]['updated_at']} "
              f"({now - stats[0]['updated_at']}秒前)")
            w(f"  payload.reply_count = {p['reply_count']}, count_discrepancy = {p['count_discrepancy']}")
            w(f"  payload.accounts = {[(a['handle'], a['count'], a['share']) for a in p['accounts']]}")
        else:
            w("  ksk_thread_stats: まだ行なし")
        w()
        w("-" * 60)
        w()

    state = c.query("SELECT quota_date, units_used, updated_at FROM ksk_state WHERE id=1")
    w("=== ksk_state ===")
    w(state[0] if state else "行なし")

print("done")
