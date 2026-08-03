"""ksk(加速)スレッドの検出・追跡・集計の共通ロジック。

「ksk(加速)」= 誰かがスレッド(親コメント)を立て、参加者がそこに返信を連投しまくって
動画全体のコメント数・個人のコメント数を稼ぐ文化。flaskr の `/ksk` ページ用のデータを作る。

対象は**オプトイン**。スレ主が `!ksk`(または `/ksk`)を含む親コメントを立てたスレッド
だけを追跡する。理由は2つ:
  1. 「誰が何割連投したか」の無差別な常時公開は晒し・炎上装置になり得る
  2. 追跡対象が無制限に増えないので、YouTube API クォータ枯らし攻撃への防御になる

このモジュールは comment_sync/sync.py 専用。純関数(コマンド検出・payload生成)と
Turso アクセス関数を分けてあり、純関数側は YouTube/Turso 非依存で単体テストできる
(sync.py 自体は import しただけで Turso 接続と API 呼び出しが走るトップレベル
スクリプトなのでテストから import できない — minutely_record/key_rotation.py を
切り出したのと同じ理由)。

3層構成(詳細は計画書):
  T0 検出: 毎分、直近 DETECT_WINDOW_MIN 分の comments を再スキャンして `!ksk` を検出。
           YouTube API 消費ゼロ。この層だけで登録・解除が完結する。
  T1 速度: active スレッドがある分だけ Pass1(commentThreads.list(id=), 50件で1 unit)を
           投げ、totalReplyCount の毎分差分を連投速度にする。
  T2 内訳: 返信本体を取得して Turso へ UPSERT し、集計SQL(下記 Q1〜Q3)で payload を作る。

T2 で `sync.py` の `_resync_thread_replies()` を絶対に流用しないこと — 同関数は
「今回返ってこなかった既知返信 = 削除」として is_deleted=1 を打つ。連投バースト中の
YouTube API は結果整合で一時的に返信を落とすことがあり、これを高頻度で実行すると
本番 comments(2.1M行)に大量の偽削除が入って自動では戻せない。ksk 用は upsert 専用の
別関数を書き、削除検知は従来の30分 recheck に任せる(_MAX_DEAD_RATIO の注記と同じ思想)。
"""
import json
import math
import re
import time
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

from turso_client import TursoClient

JST = ZoneInfo("Asia/Tokyo")

# ------------------------------------------------------------------ #
# 定数
# ------------------------------------------------------------------ #

# T0 が毎分再スキャンする窓。1回きりのスキャンだと YouTube 側のコメント反映遅延で
# 取りこぼす(time_comment_common の TIME_COMMENT_CHECK_WINDOW_MIN=5 と同じ理由)。
# 登録は thread_id の PK 冪等なので、同じ窓を何度見ても副作用はない。
DETECT_WINDOW_MIN = 5

TITLE_MAX_LEN = 40

# 同時に追跡する active スレッドの全体上限。
#
# T1 の Pass1 は RECHECK_ID_CHUNK(=50件)ごとに1 unitなので、コストは
# ceil(active数/50)。つまり MAX_ACTIVE_THREADS を 50 以下のどの値にしても
# Pass1側の最悪コストは変わらず 1 unit/分(=1,440 units/日)のまま — この上限を
# 5→20に上げても T1 は実質無料枠のまま増える。真の歯止めは DAILY_UNIT_BUDGET
# (T2の返信取得コストがここに乗ってくる)であり、この値自体はクォータ枯らし
# 攻撃を「無制限」にしないための緩い上限という位置づけ。
#
# 1アカウントが同時に持てる active スレッドは常に最大1本
# (check_ksk_commands() の active_owners チェック、この定数とは別軸)。
MAX_ACTIVE_THREADS = 20
# 1チャンネルが1日に登録できる回数。
MAX_REGISTRATIONS_PER_DAY = 3
# 返信がこの件数に達するまで T2(返信の完全取得)を起動しない。
# 誰も乗らなかった `!ksk` が API units を食わないようにするため
# (T1 の Pass1 は全 active スレッド合計で1 unit なので、放置しても実質無料)。
MIN_REPLIES_FOR_TRACKING = 3

# 自動終了の条件
IDLE_TIMEOUT_SEC = 30 * 60      # 返信が増えないまま30分
MAX_DURATION_SEC = 6 * 3600     # 開始から6時間
REPLY_CAP = 1000                # YouTube 側の1スレッドあたり返信数の上限

# T2(返信の完全取得)の実行間隔。返信数が多いスレッドほどコストが高い
# (comments.list は100件で1 unit)ので、間隔を返信数に比例させる。
PASS2_MIN_INTERVAL_MIN = 1
PASS2_MAX_INTERVAL_MIN = 5

# 1日に ksk が使ってよい YouTube API units の上限。
# API_KEY_FOR_KSK は本体同期とは別プロジェクトなので、実際の日次上限は通常
# 10,000 units(GCPデフォルト)。ここでの自己申告の上限は「使い切って構わない」
# 前提のうえで、生の 403(quotaExceeded) を突然食らって当該分の処理が丸ごと
# 例外で落ちる(sync.py の _ksk_fetch_reply_pages 等は quota エラーの
# リトライ/ローテーションを持たない — 単一の専用キーしか無いため)より先に、
# 自分から ended(budget) で綺麗に終了させるためのバックストップとして持つ。
# 10,000 に対して十分な安全マージンを残した 8,000 とする。
DAILY_UNIT_BUDGET = 8000

# comments.list のページング上限。返信は最大1000件なので10ページで足りるが、
# 想定外の応答で無限ループしないための安全弁(sync.py の MAX_PAGES と同じ考え方)。
MAX_REPLY_PAGES = 15

# payload に個別に載せるアカウント数。残りは "others" に畳む
# (円グラフのスライス数と payload サイズの両方の都合)。
TOP_ACCOUNTS = 8

# 速度系列を1分バケットで持つ上限。超えたら5分バケットにダウンサンプルする。
SPEED_MINUTE_BUCKET_LIMIT = 120

# 一覧ページ(ksk_index)に載せる「過去の加速」の件数。active分に加えて直近
# ended分をこれだけ載せる。MAX_ACTIVE_THREADSを上げるほど終了済みスレッドも
# 速く積み上がるので、それに合わせて余裕を持たせておく。
# 全履歴ではなく直近分のみ — ksk_index は1行JSONなので、際限なく増やすと
# その1行の読み書きコストと payload サイズが両方膨らむ。
INDEX_PAST_LIMIT = 30

STATE_ACTIVE = "active"
STATE_ENDED = "ended"

# ended_reason
REASON_STOPPED = "stopped"
REASON_IDLE = "idle"
REASON_CAP = "cap"
REASON_TIMEOUT = "timeout"
REASON_DELETED = "deleted"
REASON_BANNED = "banned"
REASON_BUDGET = "budget"

PAYLOAD_VERSION = 1

# ------------------------------------------------------------------ #
# スキーマ
# ------------------------------------------------------------------ #

_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS ksk_threads (
      thread_id        TEXT PRIMARY KEY,
      owner_channel_id TEXT NOT NULL,
      owner_handle     TEXT,
      title            TEXT,
      started_at       INTEGER NOT NULL,
      registered_at    INTEGER NOT NULL,
      state            TEXT NOT NULL,
      ended_at         INTEGER,
      ended_reason     TEXT,
      last_pass1_at    INTEGER,
      last_pass2_at    INTEGER,
      last_reply_count INTEGER NOT NULL DEFAULT 0,
      -- 最後に返信数が「増えた」時刻。アイドル判定はこれで測る。
      -- last_pass1_at は毎分更新されるので「増えていない時間」の計測には使えない。
      last_growth_at   INTEGER,
      unique_accounts  INTEGER NOT NULL DEFAULT 0,
      resume_token     TEXT,
      updated_at       INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ksk_threads_state ON ksk_threads(state, started_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_ksk_threads_owner ON ksk_threads(owner_channel_id, registered_at)",
    """
    CREATE TABLE IF NOT EXISTS ksk_bans (
      channel_id TEXT PRIMARY KEY,
      reason     TEXT,
      created_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ksk_thread_stats (
      thread_id  TEXT PRIMARY KEY,
      payload    TEXT NOT NULL,
      updated_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ksk_index (
      id         INTEGER PRIMARY KEY CHECK (id = 1),
      payload    TEXT NOT NULL,
      updated_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ksk_state (
      id         INTEGER PRIMARY KEY CHECK (id = 1),
      quota_date TEXT NOT NULL,
      units_used INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
    """,
]


def ensure_schema(turso: TursoClient) -> None:
    """テーブル・インデックスを冪等に作る。T0 の先頭で1回呼ぶ。"""
    for sql in _TABLES_SQL:
        turso.execute(sql)


# ------------------------------------------------------------------ #
# コマンド検出（純関数）
# ------------------------------------------------------------------ #

# NFKC 正規化後の本文に当てる。`！ｋｓｋ` `／ｋｓｋ` は NFKC で `!ksk` `/ksk` になるので
# 文字クラスは半角2種だけで足りる。
#
# 仕様は「本文に含まれていれば登録」。行頭に限定しない — `加速するぞ /ksk` のような
# 自然な書き方を落とさないため。その代わり引用・言及（`さっき /ksk って言ってた人`）でも
# 登録されるが、誤登録の実害は小さい（スレ主が `/ksk stop` で解除でき、放置しても
# IDLE_TIMEOUT_SEC で自動終了し、悪用は ksk_bans で止められる）。
#
# 前後の否定先読み/後読みは URL 対策。`https://example.com/ksk` のような普通のリンクで
# 登録されるのを防ぐため、直前が ASCII の英数字・`.`・`_`・`-` の場合はコマンドとみなさない。
# 日本語文字は除外していないので `加速するぞ!ksk` は通る。
_COMMAND_RE = re.compile(
    r"(?<![0-9A-Za-z._-])[!/]\s*ksk(?![0-9A-Za-z])",
    re.IGNORECASE,
)

_STOP_ARG_RE = re.compile(r"^stop\b", re.IGNORECASE)

ACTION_START = "start"
ACTION_STOP = "stop"


def normalize_text(s: str) -> str:
    """NFKC 正規化のみ。

    time_comment_common.normalize_text() と違い「時」→":" 等の置換はしない
    (あちらは時刻表記の吸収が目的で、こちらはコマンド語の同定が目的)。
    """
    return unicodedata.normalize("NFKC", s)


def parse_command(text) -> dict | None:
    """コメント本文から ksk コマンドを取り出す。

    本文のどこにあってもよい（行頭限定ではない）。コマンドが複数行にまたがって
    現れる場合は、最初に見つかった行を採用する。

    戻り値:
      {"action": "start", "title": str|None}  … `!ksk` / `!ksk タイトル` / `加速するぞ /ksk`
      {"action": "stop"}                      … `!ksk stop`
      None                                    … コマンドではない

    タイトルはコマンド語より後ろの、同じ行の残りを使う（無ければ None）。
    text が None(削除済み行の text は NULL)や非文字列なら None。
    """
    if not isinstance(text, str) or not text:
        return None
    for line in normalize_text(text).splitlines():
        m = _COMMAND_RE.search(line)
        if m is None:
            continue
        rest = line[m.end():].strip()
        if _STOP_ARG_RE.match(rest):
            return {"action": ACTION_STOP}
        return {"action": ACTION_START, "title": rest[:TITLE_MAX_LEN] or None}
    return None


def jst_date_str(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=JST).date().isoformat()


def jst_day_start_epoch(now_epoch: int) -> int:
    """now_epoch を含む JST 暦日の 0:00 の epoch 秒。"""
    now_jst = datetime.fromtimestamp(now_epoch, tz=JST)
    day_start = now_jst.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(day_start.timestamp())


def detect_window_bounds(now_epoch: int) -> tuple[int, int]:
    """T0 が毎分スキャンする [start, end) の epoch 秒。"""
    return now_epoch - DETECT_WINDOW_MIN * 60, now_epoch


# ------------------------------------------------------------------ #
# 自動終了の判定（純関数）
# ------------------------------------------------------------------ #

def evaluate_end_reason(thread: dict, reply_count: int, now_epoch: int) -> str | None:
    """active スレッドを終了すべきかを判定する。終了不要なら None。

    thread: ksk_threads の1行(started_at / last_reply_count / updated_at を使う)。
    reply_count: 今回の Pass1 で得た totalReplyCount。
    """
    if reply_count >= REPLY_CAP:
        return REASON_CAP
    if now_epoch - int(thread["started_at"]) >= MAX_DURATION_SEC:
        return REASON_TIMEOUT
    # 返信が増えていない状態がどれだけ続いたかは「最後に件数が増えた時刻」で測る。
    # last_pass1_at は毎分更新されるので使えない(常に「今」になってしまう)。
    last_growth_at = thread.get("last_growth_at") or thread["started_at"]
    if reply_count > int(thread.get("last_reply_count") or 0):
        return None
    if now_epoch - int(last_growth_at) >= IDLE_TIMEOUT_SEC:
        return REASON_IDLE
    return None


# ------------------------------------------------------------------ #
# 集計SQL（T2 用。返る行数を十数行〜数百行に落とすため Turso 側で GROUP BY する）
#
# rows_read 自体は Python 側で全返信を舐めても同じ(どちらも idx_parent_published で
# そのスレッドの返信範囲を SEARCH する)。SQL にすると返る行数だけが減るので、転送量と
# ランナー実行時間が減る。さらに毎回 Turso の実データから引き直すため、resume_token に
# よる差分取得と組み合わせても内訳が構造的に自己修復する(Python 側の累積器だと削除・
# 取りこぼし・再起動でズレたまま直らない)。
#
# 本番投入前に4本とも EXPLAIN QUERY PLAN で idx_parent_published の SEARCH になって
# いることを確認すること。特に Q2 の `published_at / 60` と Q3 のウィンドウ関数で
# 索引が外れて SCAN に落ちていないか。
# ------------------------------------------------------------------ #

Q1_ACCOUNTS_SQL = """
SELECT author_channel_id,
       MAX(handle)       AS handle,
       COUNT(*)          AS c,
       MIN(published_at) AS first_at,
       MAX(published_at) AS last_at
FROM comments
WHERE parent_id = ? AND is_deleted = 0
GROUP BY author_channel_id
ORDER BY c DESC
"""

Q2_MINUTE_BUCKETS_SQL = """
SELECT published_at / 60 AS m, author_channel_id, COUNT(*) AS c
FROM comments
WHERE parent_id = ? AND is_deleted = 0
GROUP BY m, author_channel_id
ORDER BY m
"""

_Q3_GAP_SUBQUERY = """
SELECT author_channel_id,
       published_at - LAG(published_at) OVER (
         PARTITION BY author_channel_id ORDER BY published_at) AS gap
FROM comments
WHERE parent_id = ? AND is_deleted = 0
"""


# Q3: 投稿間隔サマリ。**アカウントで絞らず、スレッド全体を1回で GROUP BY する。**
#
# 当初は上位アカウントだけを `WHERE author_channel_id IN (...)` で絞る形だったが、
# EXPLAIN QUERY PLAN で確認したところ SQLite がその条件をサブクエリへ押し込み、
# idx_parent_published ではなく idx_comments_author_published(author_channel_id=?) を
# 選んでいた。つまり「このスレッドの返信」ではなく「そのアカウントの全履歴コメント」を
# 舐める形になる。連投の上位アカウントほど全履歴の投稿数も多いので最悪のケースになる
# (2026-07-06 の「結果が小さいから軽いと誤判断して2,000万行読んだ」インシデントと同型)。
#
# 絞りを外すと索引選択が idx_parent_published に戻り、しかも返る行数は
# 参加アカウント数(十数行)にしかならないので、1クエリで全アカウント分が取れる。
# 上位8件の抽出は Python 側で行う。
#
# min_gap は NULLIF(gap, 0) で0秒を除外する — published_at は秒精度なので同秒投稿が
# 普通に起きる。0を「最速0秒」として見せると無限速度に見えるため、0の個数は
# same_second_pairs という別指標にする。
Q3_GAP_STATS_SQL = f"""
SELECT author_channel_id,
       AVG(gap)                                  AS avg_gap,
       MIN(NULLIF(gap, 0))                       AS min_gap,
       SUM(CASE WHEN gap = 0 THEN 1 ELSE 0 END)  AS same_second_pairs,
       COUNT(gap)                                AS gap_n
FROM ({_Q3_GAP_SUBQUERY})
WHERE gap IS NOT NULL
GROUP BY author_channel_id
"""

# 中央値。SQLite/libSQL に中央値の集約関数が無いため OFFSET で真ん中の1行を取る。
# 上位 TOP_ACCOUNTS 件ぶんを query_batch() で1パイプラインにまとめて投げる想定。
#
# `+author_channel_id` の `+` は索引使用を抑制する単項演算子(random_comment.py の
# `+parent_id IS NULL` と同じ手)。これが無いと Q3 と同じく
# idx_comments_author_published が選ばれ、そのアカウントの全履歴を舐めてしまう。
Q3_MEDIAN_GAP_SQL = f"""
SELECT gap FROM ({_Q3_GAP_SUBQUERY})
WHERE gap IS NOT NULL AND +author_channel_id = ?
ORDER BY gap
LIMIT 1 OFFSET ?
"""


# ------------------------------------------------------------------ #
# payload 生成（純関数）
# ------------------------------------------------------------------ #

def _downsample(series: list[int], factor: int) -> list[int]:
    out = []
    for i in range(0, len(series), factor):
        out.append(sum(series[i:i + factor]))
    return out


def build_speed(q2_rows: list[dict], top_ids: list[str]) -> tuple[dict, dict[str, int]]:
    """Q2 の (分バケット, アカウント, 件数) から速度系列を組み立てる。

    戻り値: (speed dict, {channel_id: そのアカウントの最大 rpm})
    バケット数が SPEED_MINUTE_BUCKET_LIMIT を超えたら5分バケットにダウンサンプルする。
    """
    if not q2_rows:
        return {"bucket_sec": 60, "t0": 0, "total": [], "by_account": {}}, {}

    minutes = [int(r["m"]) for r in q2_rows]
    m0, m1 = min(minutes), max(minutes)
    n = m1 - m0 + 1

    total = [0] * n
    per_account: dict[str, list[int]] = {cid: [0] * n for cid in top_ids}
    peak_per_min: dict[str, int] = {}

    for r in q2_rows:
        idx = int(r["m"]) - m0
        c = int(r["c"])
        cid = r["author_channel_id"]
        total[idx] += c
        if cid in per_account:
            per_account[cid][idx] += c
        if c > peak_per_min.get(cid, 0):
            peak_per_min[cid] = c

    bucket_sec = 60
    if n > SPEED_MINUTE_BUCKET_LIMIT:
        factor = 5
        bucket_sec = 60 * factor
        total = _downsample(total, factor)
        per_account = {cid: _downsample(v, factor) for cid, v in per_account.items()}

    speed = {
        "bucket_sec": bucket_sec,
        "t0": m0 * 60,
        "total": total,
        "by_account": per_account,
    }
    return speed, peak_per_min


def build_payload(
    thread: dict,
    q1_rows: list[dict],
    q2_rows: list[dict],
    gap_rows: list[dict],
    median_gap_by_channel: dict[str, int],
    total_reply_count: int | None = None,
    now_epoch: int | None = None,
) -> dict:
    """ksk_thread_stats.payload を組み立てる。

    thread: ksk_threads の1行相当
    q1_rows: Q1_ACCOUNTS_SQL の結果(件数降順)
    q2_rows: Q2_MINUTE_BUCKETS_SQL の結果
    gap_rows: q3_gap_stats_sql() の結果(上位アカウントのみ)
    median_gap_by_channel: {channel_id: 中央値秒}(上位アカウントのみ)
    total_reply_count: Pass1 で得た YouTube 側の返信数。取得件数との差を
      count_discrepancy として出すために使う(1000件上限や結果整合で乖離し得る)。
    """
    now = int(now_epoch if now_epoch is not None else time.time())
    fetched_count = sum(int(r["c"]) for r in q1_rows)
    reply_count = int(total_reply_count) if total_reply_count is not None else fetched_count

    top_rows = q1_rows[:TOP_ACCOUNTS]
    top_ids = [r["author_channel_id"] for r in top_rows]
    speed, peak_per_min = build_speed(q2_rows, top_ids)

    gap_by_channel = {r["author_channel_id"]: r for r in gap_rows}

    accounts = []
    for r in top_rows:
        cid = r["author_channel_id"]
        c = int(r["c"])
        g = gap_by_channel.get(cid) or {}
        accounts.append({
            "channel_id": cid,
            "handle": r.get("handle"),
            "count": c,
            "share": (c / fetched_count) if fetched_count else 0.0,
            "first_at": int(r["first_at"]) if r.get("first_at") is not None else None,
            "last_at": int(r["last_at"]) if r.get("last_at") is not None else None,
            "median_gap_sec": median_gap_by_channel.get(cid),
            "min_gap_sec": int(g["min_gap"]) if g.get("min_gap") is not None else None,
            "same_second_pairs": int(g.get("same_second_pairs") or 0),
            "peak_per_min": peak_per_min.get(cid, 0),
        })

    rest = q1_rows[TOP_ACCOUNTS:]
    others = {
        "count": sum(int(r["c"]) for r in rest),
        "accounts": len(rest),
    }

    return {
        "v": PAYLOAD_VERSION,
        "thread_id": thread["thread_id"],
        "title": thread.get("title"),
        "owner": {
            "channel_id": thread.get("owner_channel_id"),
            "handle": thread.get("owner_handle"),
        },
        "state": thread.get("state", STATE_ACTIVE),
        "started_at": int(thread["started_at"]),
        "ended_at": thread.get("ended_at"),
        "ended_reason": thread.get("ended_reason"),
        "updated_at": now,
        "reply_count": reply_count,
        "cap": REPLY_CAP,
        "remaining": max(0, REPLY_CAP - reply_count),
        "unique_accounts": len(q1_rows),
        "count_discrepancy": reply_count - fetched_count,
        "accounts": accounts,
        "others": others,
        "speed": speed,
    }


def build_index_payload(threads: list[dict], now_epoch: int | None = None) -> dict:
    """ksk_index.payload を組み立てる(一覧ページ用の要約)。"""
    now = int(now_epoch if now_epoch is not None else time.time())
    return {
        "v": PAYLOAD_VERSION,
        "generated_at": now,
        "threads": [
            {
                "thread_id": t["thread_id"],
                "title": t.get("title"),
                "owner_handle": t.get("owner_handle"),
                "owner_channel_id": t.get("owner_channel_id"),
                "state": t.get("state"),
                "started_at": int(t["started_at"]),
                "ended_at": t.get("ended_at"),
                "ended_reason": t.get("ended_reason"),
                "reply_count": int(t.get("last_reply_count") or 0),
                "unique_accounts": int(t.get("unique_accounts") or 0),
                "updated_at": int(t.get("updated_at") or now),
            }
            for t in threads
        ],
    }


# ------------------------------------------------------------------ #
# Turso アクセス
# ------------------------------------------------------------------ #

def get_banned_channels(turso: TursoClient) -> set[str]:
    """BAN 済み channel_id の集合。ksk_bans は運用者が手で入れる小テーブル。"""
    rows = turso.query("SELECT channel_id FROM ksk_bans")
    return {r["channel_id"] for r in rows}


def get_threads_by_state(turso: TursoClient, state: str) -> list[dict]:
    return turso.query(
        "SELECT * FROM ksk_threads WHERE state = ? ORDER BY started_at DESC", [state]
    )


def get_recent_threads(turso: TursoClient, limit: int = INDEX_PAST_LIMIT) -> list[dict]:
    return turso.query(
        "SELECT * FROM ksk_threads WHERE state = ? ORDER BY started_at DESC LIMIT ?",
        [STATE_ENDED, limit],
    )


def count_registrations_since(turso: TursoClient, channel_id: str, since_epoch: int) -> int:
    rows = turso.query(
        "SELECT COUNT(*) AS c FROM ksk_threads WHERE owner_channel_id = ? AND registered_at >= ?",
        [channel_id, since_epoch],
    )
    return int(rows[0]["c"]) if rows else 0


def register_thread(
    turso: TursoClient,
    thread_id: str,
    owner_channel_id: str,
    owner_handle,
    title,
    started_at: int,
    now_epoch: int,
) -> None:
    """新しい ksk スレッドを登録する。既存 thread_id は何もしない(冪等)。"""
    turso.execute(
        "INSERT INTO ksk_threads "
        "(thread_id, owner_channel_id, owner_handle, title, started_at, registered_at, "
        " state, last_reply_count, last_growth_at, unique_accounts, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, 0, ?) "
        "ON CONFLICT(thread_id) DO NOTHING",
        [thread_id, owner_channel_id, owner_handle, title, started_at,
         now_epoch, STATE_ACTIVE, now_epoch, now_epoch],
    )


def end_thread(turso: TursoClient, thread_id: str, reason: str, now_epoch: int) -> None:
    """active スレッドを終了状態にする。既に ended なら何もしない。"""
    turso.execute(
        "UPDATE ksk_threads SET state = ?, ended_at = ?, ended_reason = ?, updated_at = ? "
        "WHERE thread_id = ? AND state = ?",
        [STATE_ENDED, now_epoch, reason, now_epoch, thread_id, STATE_ACTIVE],
    )


def write_thread_stats(turso: TursoClient, thread_id: str, payload: dict, now_epoch: int) -> None:
    turso.execute(
        "INSERT INTO ksk_thread_stats (thread_id, payload, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(thread_id) DO UPDATE SET "
        "payload = excluded.payload, updated_at = excluded.updated_at",
        [thread_id, json.dumps(payload, ensure_ascii=False), now_epoch],
    )


def write_index(turso: TursoClient, payload: dict, now_epoch: int) -> None:
    turso.execute(
        "INSERT INTO ksk_index (id, payload, updated_at) VALUES (1, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET "
        "payload = excluded.payload, updated_at = excluded.updated_at",
        [json.dumps(payload, ensure_ascii=False), now_epoch],
    )


def update_pass1_progress(
    turso: TursoClient, thread_id: str, reply_count: int, now_epoch: int, grew: bool
) -> None:
    """T1 の結果を書き戻す。last_growth_at は件数が実際に増えたときだけ進める。

    アイドル判定を last_pass1_at で行うと毎分更新されて永久にアイドルにならないため、
    「最後に増えた時刻」を別に持つ(evaluate_end_reason 参照)。
    """
    if grew:
        turso.execute(
            "UPDATE ksk_threads SET last_reply_count = ?, last_pass1_at = ?, "
            "last_growth_at = ?, updated_at = ? WHERE thread_id = ?",
            [reply_count, now_epoch, now_epoch, now_epoch, thread_id],
        )
    else:
        turso.execute(
            "UPDATE ksk_threads SET last_reply_count = ?, last_pass1_at = ?, updated_at = ? "
            "WHERE thread_id = ?",
            [reply_count, now_epoch, now_epoch, thread_id],
        )


def update_pass2_progress(
    turso: TursoClient, thread_id: str, resume_token, unique_accounts: int, now_epoch: int
) -> None:
    turso.execute(
        "UPDATE ksk_threads SET last_pass2_at = ?, resume_token = ?, unique_accounts = ?, "
        "updated_at = ? WHERE thread_id = ?",
        [now_epoch, resume_token, unique_accounts, now_epoch, thread_id],
    )


def pass2_interval_min(reply_count: int) -> int:
    """返信数に応じた T2 の実行間隔(分)。

    comments.list は100件=1 unit なので、返信数が増えるほど1回のコストが上がる。
    間隔を ceil(件数/100) 分にすると「1分あたりのコスト」がほぼ一定になる。
    """
    pages = max(1, math.ceil(max(reply_count, 1) / 100))
    return max(PASS2_MIN_INTERVAL_MIN, min(PASS2_MAX_INTERVAL_MIN, pages))


def pass2_due(thread: dict, reply_count: int, now_epoch: int) -> bool:
    last = thread.get("last_pass2_at")
    if not last:
        return True
    return now_epoch - int(last) >= pass2_interval_min(reply_count) * 60


# ------------------------------------------------------------------ #
# 日次 unit 予算
# ------------------------------------------------------------------ #

def read_quota_state(turso: TursoClient, now_epoch: int) -> dict:
    """今日(JST)の消費 units を返す。日付が変わっていれば0から数え直す。"""
    today = jst_date_str(now_epoch)
    rows = turso.query("SELECT quota_date, units_used FROM ksk_state WHERE id = 1")
    if rows and rows[0]["quota_date"] == today:
        return {"quota_date": today, "units_used": int(rows[0]["units_used"])}
    return {"quota_date": today, "units_used": 0}


def write_quota_state(turso: TursoClient, now_epoch: int, units_used: int) -> None:
    turso.execute(
        "INSERT INTO ksk_state (id, quota_date, units_used, updated_at) VALUES (1, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET quota_date = excluded.quota_date, "
        "units_used = excluded.units_used, updated_at = excluded.updated_at",
        [jst_date_str(now_epoch), units_used, now_epoch],
    )


# ------------------------------------------------------------------ #
# 集計（T2 の書き込み後に呼ぶ）
# ------------------------------------------------------------------ #

def aggregate_thread(turso: TursoClient, thread_id: str, top_n: int = TOP_ACCOUNTS):
    """1スレッドぶんの集計を Turso 側で回す。

    戻り値: (q1_rows, q2_rows, gap_rows, median_gap_by_channel)
    そのまま build_payload() に渡せる。
    """
    q1_rows = turso.query(Q1_ACCOUNTS_SQL, [thread_id])
    q2_rows = turso.query(Q2_MINUTE_BUCKETS_SQL, [thread_id])
    gap_rows = turso.query(Q3_GAP_STATS_SQL, [thread_id])

    gap_by_channel = {r["author_channel_id"]: r for r in gap_rows}
    statements, targets = [], []
    for r in q1_rows[:top_n]:
        cid = r["author_channel_id"]
        g = gap_by_channel.get(cid)
        n = int(g["gap_n"]) if g and g.get("gap_n") else 0
        if n <= 0:
            continue
        # 昇順に並べた gap の真ん中の1行。偶数個なら小さい側(下位中央値)を採る
        statements.append({"sql": Q3_MEDIAN_GAP_SQL, "args": [thread_id, cid, (n - 1) // 2]})
        targets.append(cid)

    # query_batch()(複数SELECTを1パイプラインにまとめる)は turso_client.py の
    # コピーによって実装の有無が分かれる(flaskr側にはあるが comment_sync側には無い、
    # 2026-08-03に本番エラーで発覚)。最大 TOP_ACCOUNTS(=8)件の単発 query() ループに
    # 留め、コピー間の差異に依存しないようにする。Pass2 自体が1〜5分に1回しか
    # 走らないので、往復が増えるコストは無視できる。
    medians: dict[str, int] = {}
    for stmt, cid in zip(statements, targets):
        rows = turso.query(stmt["sql"], stmt["args"])
        if rows and rows[0].get("gap") is not None:
            medians[cid] = int(rows[0]["gap"])
    return q1_rows, q2_rows, gap_rows, medians


def known_reply_ids(turso: TursoClient, thread_id: str) -> set[str]:
    """そのスレッドの既知の返信 comment_id。

    差分 UPSERT のために使う。値が変わらない行まで UPSERT すると
    ON CONFLICT DO UPDATE が行を書き換えて rows_written に計上されるため
    (1000件×毎分 = 日次予算の36%)、実際に増えた返信だけを書く。
    """
    rows = turso.query("SELECT comment_id FROM comments WHERE parent_id = ?", [thread_id])
    return {r["comment_id"] for r in rows}


def scan_window_rows(turso: TursoClient, start_epoch: int, end_epoch: int) -> list[dict]:
    """T0 の窓スキャン。

    idx_comments_published(published_at) の SEARCH。親コメント(登録コマンド)と
    返信(解除コマンド)の両方を見たいので parent_id では絞らない — 5分幅なら
    通常数十行、加速中でも数百行程度で、絞っても読む行数は変わらない。
    """
    return turso.query(
        "SELECT comment_id, parent_id, author_channel_id, handle, published_at, text "
        "FROM comments WHERE published_at >= ? AND published_at < ? "
        "ORDER BY published_at",
        [start_epoch, end_epoch],
    )
