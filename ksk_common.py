"""ksk(加速)スレッドの検出・追跡・集計の共通ロジック。

「ksk(加速)」= 誰かがスレッド(親コメント)を立て、参加者がそこに返信を連投しまくって
動画全体のコメント数・個人のコメント数を稼ぐ文化。flaskr の `/ksk` ページ用のデータを作る。

対象は**オプトイン**。スレ主が `!ksk`(または `/ksk`、あるいは `EXTRA_TRIGGER_PHRASES`
の決まり文句)を含む親コメントを立てたスレッドだけを追跡する。理由は2つ:
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

# 1回の窓スキャン(DETECT_WINDOW_MIN=5分)の中で、同一アカウントが立てた
# 「登録コマンド入りの親コメント」がこの件数以上なら自動BANする。
#
# 注意: 検出仕様が「本文のどこかに !ksk / /ksk が含まれていれば該当」なので、
# ksk について語っただけのコメントも数に入る(例: 「みんな /ksk で立てて」)。
# 加速が盛り上がっている最中に熱心な人が短時間で何度も言及すると誤BANになり得る。
# そのため閾値は「明らかに機械的な連投」と言える水準に置き、BANの理由文へ
# 件数と根拠を残して後から人手で取り消せるようにしている(ksk_bans から DELETE)。
AUTO_BAN_THREADS_PER_SCAN = 4

# 上のAUTO_BAN_THREADS_PER_SCANは1回の窓スキャン(=直近5分)の中でしか見ないため、
# ちょうどそのスキャン間隔をわずかに超えるペースで分散して打たれた連投は
# 単発のスキャンでは閾値に届かず素通りし得る(例: 1分ごとに1件、10分で12件のペース
# だと5分スキャンには最大でも5〜6件しか映らない)。そこで author_channel_id ごとに
# もう少し長い時間軸(TRIGGER_BAN_WINDOW_MIN分)での絶対件数も別途チェックする。
# ユーザー指定: 10分間に12件以上ならBAN。
TRIGGER_BAN_WINDOW_MIN = 10
TRIGGER_BAN_THRESHOLD = 12

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

# Pass1 で「リクエストした thread_id が応答に含まれなかった = 削除された」と
# 判定する際の誤検知対策(sync.py の _MAX_DEAD_RATIO/_MIN_BATCH_FOR_DEAD_RATIO_GUARD
# と同じ思想)。ksk の同時稼働数は MAX_ACTIVE_THREADS(=20)止まりで、本体同期の
# 巡回チェック(50件単位)より一桁小さいため、フロアも合わせて下げる。
KSK_MAX_DEAD_RATIO = 0.5
KSK_MIN_BATCH_FOR_DEAD_RATIO_GUARD = 5

# 円グラフ・速度グラフに個別の色で載せるアカウント数。残りは "others" に畳む
# (色パレット(flaskr/public/static/ksk.js の COLORS)の数・スライスの見やすさが
# 上限。表(all_accounts)は絞らず全員載せるので、ここは「グラフに乗る人数」の話に限る)。
TOP_ACCOUNTS = 15

# 速度系列を1分バケットで持つ上限。超えたら5分バケットにダウンサンプルする。
SPEED_MINUTE_BUCKET_LIMIT = 120

# 一覧ページ(ksk_index)に載せる「過去の加速」の件数。active分に加えて直近
# ended分をこれだけ載せる。MAX_ACTIVE_THREADSを上げるほど終了済みスレッドも
# 速く積み上がるので、それに合わせて余裕を持たせておく。
# 全履歴ではなく直近分のみ — ksk_index は1行JSONなので、際限なく増やすと
# その1行の読み書きコストと payload サイズが両方膨らむ。
INDEX_PAST_LIMIT = 30

# ------------------------------------------------------------------ #
# 休眠中(ended)スレッドの復活検知
#
# Pass2 は active なスレッドにしか走らないため、一度 ended になると
# ksk_thread_stats の payload は二度と自動更新されない。アイドルタイムアウト
# (30分)後に遅れて数件だけ返信が付くケースは普通に起き得るが、これまでは
# 検知する手段が無く、ページは古いまま固まって表示され続けていた。
#
# check_dormant_ksk_threads() が低頻度(1時間おき)で、直近 ended のスレッドの
# comments 上の実返信数を last_reply_count と比較し、増えていたら:
#   増加分 < DORMANT_REACTIVATE_THRESHOLD → 内訳だけ再集計、state は ended のまま
#   増加分 >= DORMANT_REACTIVATE_THRESHOLD → active に戻し、以後は通常の
#     Pass1/Pass2 サイクルに合流する(evaluate_end_reason() の REPLY_CAP/
#     MAX_DURATION_SEC が started_at 基準で効くので、6時間経過済みなら
#     次のPass1で即座に ended へ戻る = 枠を永久に占有する心配は無い)
#
# 閾値はユーザーの勘による初期値。実データで「終了後どれくらい遅れて
# 何件付くか」の分布を見てから調整する想定。
DORMANT_REACTIVATE_THRESHOLD = 10
DORMANT_CHECK_LIMIT = INDEX_PAST_LIMIT

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

# `!ksk`/`/ksk` 以外にも、この文化圏で実際に使われる決まり文句を含むだけで
# 登録対象にする（ユーザー指定の完全一致文字列、NFKC正規化後）。
# `!ksk` と違ってコマンド語の後ろにタイトルが続く形を仮定していない自然文なので、
# マッチ後の残りをタイトルとして取らない（title は常に None）。
# バリエーションは増やさず、必要になれば都度足す。
EXTRA_TRIGGER_PHRASES = [
    "kskチャレンジ",
    "加速チャレンジ",
    "連投チャレンジ",
    "ここだけ1000コメいく",
]

_STOP_ARG_RE = re.compile(r"^stop\b", re.IGNORECASE)

# `!ksk stop` / `/ksk stop` に加えて、単に「終了」と返信するだけでも止められるようにする
# （スマホから記号を打つのが面倒なため）。
#
# **こちらだけは「行全体が終了」を要求する**（`!ksk` 系の「本文のどこかにあればOK」とは
# 意図的に扱いを変えている）。「終了」は普通の日本語なので、含むだけで発火させると
# 「そろそろ終了かな」「終了までもう少し」のような何気ない発言でスレッドが止まる。
# 一度 ended にしたスレッドは同じ comment_id では復活できない（新しくスレッドを
# 立て直してもらうことになる）ため、ここは誤爆しない側に倒す。
# 末尾の句読点・感嘆符・「w」は自然な打ち方なので許容する。
_STOP_WORD_RE = re.compile(r"\A終了[\s!！?？。．、,.ｗw]*\Z")

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
      {"action": "start", "title": str}  … `!ksk` / `!ksk タイトル` / `加速するぞ /ksk` /
                                             EXTRA_TRIGGER_PHRASES のいずれかを含む行
      {"action": "stop"}                 … `!ksk stop` / `/ksk stop` / 行全体が「終了」
      None                               … コマンドではない

    タイトルはコマンド語より後ろの、同じ行の残りを使う。**それが空なら、
    検出したトリガーワード自体をタイトルとして使う**（`!ksk`/`/ksk`/
    `加速チャレンジ` 等）。一覧・詳細ページはタイトル未設定時「加速」とだけ
    表示していたが、全スレッドが同じ「加速」になって見分けがつかず、
    わざわざ表示する意味が無かった — せめて何のワードで検出されたかを
    出す方が有益（2026-08-04、ユーザー指摘で変更）。
    EXTRA_TRIGGER_PHRASES 側は自然文の一部としてマッチするため、後ろの文字列を
    タイトルとして取らない（例えば「今日は加速チャレンジやります」の
    「やります」をタイトルにしても意味が無い。マッチしたフレーズそのものを使う）。
    text が None(削除済み行の text は NULL)や非文字列なら None。

    停止コマンドが実際に効くのは「スレ主本人が自分の稼働中スレッドに返信した場合」だけ
    （判定は呼び出し側の check_ksk_commands）。ここでは文面の判定しかしない。
    """
    if not isinstance(text, str) or not text:
        return None
    for line in normalize_text(text).splitlines():
        m = _COMMAND_RE.search(line)
        if m is not None:
            rest = line[m.end():].strip()
            if _STOP_ARG_RE.match(rest):
                return {"action": ACTION_STOP}
            # NFKC正規化後なので先頭は必ず半角 "!" か "/"。表記ゆれ(全角・空白)
            # を吸収した綺麗な表示形にする。
            trigger = m.group(0)[0] + "ksk"
            return {"action": ACTION_START, "title": rest[:TITLE_MAX_LEN] or trigger}
        if _STOP_WORD_RE.match(line.strip()):
            return {"action": ACTION_STOP}
        for phrase in EXTRA_TRIGGER_PHRASES:
            if phrase in line:
                return {"action": ACTION_START, "title": phrase}
    return None


def select_start_candidates(candidates: list) -> tuple[list, dict[str, int]]:
    """登録コマンド候補をアカウントごとに1件へ絞る（純関数）。

    candidates: [(row, cmd), ...]。row は comments の1行、cmd は parse_command() の戻り値。

    戻り値: (採用する [(row, cmd), ...], {channel_id: そのスキャンでの該当件数})

    **同一アカウントが1回のスキャンで複数該当した場合は最新(published_at最大)を採る。**
    短時間に連続して `!ksk` を打つのは打ち直し・言い直しであることが多く、
    最後に書いたものが本人の意図に一番近いため。
    ただしこれは「まだどれも登録されていない」場合の話で、既に登録済みの
    スレッドは呼び出し側が known_ids で弾く（走っているスレッドを後から
    立てたスレッドで置き換えたりはしない）。

    2つ目の戻り値は自動BAN判定用の件数。採用しなかった分も数える
    （連投そのものを検知したいので、絞る前の数が必要）。
    """
    per_author: dict[str, tuple] = {}
    counts: dict[str, int] = {}
    for row, cmd in candidates:
        if row.get("parent_id") is not None or cmd.get("action") != ACTION_START:
            continue
        author = row.get("author_channel_id")
        if not author:
            continue
        counts[author] = counts.get(author, 0) + 1
        prev = per_author.get(author)
        if prev is None or int(row["published_at"]) >= int(prev[0]["published_at"]):
            per_author[author] = (row, cmd)
    # 元の候補順(published_at昇順)を保ったまま返す
    selected = sorted(per_author.values(), key=lambda rc: int(rc[0]["published_at"]))
    return selected, counts


def auto_ban_targets(scan_counts: dict[str, int]) -> list[str]:
    """1スキャン内の該当件数から自動BAN対象の channel_id を返す（純関数）。"""
    return sorted(
        cid for cid, n in scan_counts.items() if n >= AUTO_BAN_THREADS_PER_SCAN
    )


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


def trigger_ban_window_bounds(now_epoch: int) -> tuple[int, int]:
    """10分閾値の自動BAN判定で見る [start, end) の epoch 秒。"""
    return now_epoch - TRIGGER_BAN_WINDOW_MIN * 60, now_epoch


def count_recent_trigger_threads(client: TursoClient, channel_id: str, now_epoch: int) -> int:
    """そのアカウントが直近 TRIGGER_BAN_WINDOW_MIN 分に立てた、登録コマンド入りの
    親コメント(スレッド)の件数。

    `WHERE author_channel_id = ? AND published_at >= ? AND published_at < ?` は
    idx_comments_author_published (author_channel_id, published_at) の SEARCH になる
    ――account_profile_updater.rebuild_profile() と同じ、この索引の意図した使い方
    (AGENTS.md 参照)。トリガー文字列の判定は SQL では出来ないので、対象アカウントの
    直近ぶんの本文だけを読んで Python 側で parse_command() にかける。呼び出し元が
    「このスキャンで1件以上検知したアカウントだけ」に絞ってから呼ぶので、
    毎分の追加コストは無視できる小さい件数になる。
    """
    start_epoch, end_epoch = trigger_ban_window_bounds(now_epoch)
    rows = client.query(
        "SELECT text FROM comments WHERE author_channel_id = ? "
        "AND published_at >= ? AND published_at < ? AND parent_id IS NULL",
        [channel_id, start_epoch, end_epoch],
    )
    count = 0
    for row in rows:
        cmd = parse_command(row.get("text"))
        if cmd is not None and cmd.get("action") == ACTION_START:
            count += 1
    return count


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
# 集計（T2 用）
#
# **そのスレッドの返信の生データを1回だけ引き、集計は全部 Python で行う。**
#
# 以前は Turso 側で GROUP BY させる設計で、件数(Q1)・分バケット(Q2)・間隔サマリ(Q3)・
# 中央値(上位8アカウント各1本)の計12本のクエリを投げていた。返る行数は確かに小さく
# なるが、**課金されるのは返る行数ではなくスキャンされる行数(rows_read)**であり、
# どのクエリも `WHERE parent_id = ?` でそのスレッドの返信を頭から舐め直すため、
# 1000返信のスレッドで 12 × 1000 = 約12,000 rows_read/回になっていた。
#
# 生データを1回引けば 1 × 1000 行で済み、しかも中央値を**全アカウント分**出せる
# (以前は上位8件に限定していた。複数アカウントを使った連投を見るのが目的なので、
# 2〜8番手の間隔を捨てるのは本末転倒だった)。集計先は GitHub Actions ランナーで
# CPU 課金が無いので、Python 側で回してもコスト上のデメリットは無い。
#
# 「集計は SQL に寄せる」という判断が正しいのは **Vercel**(Active CPU 課金あり)の
# 話であって、ランナー上のバッチには当てはまらない。取り違えないこと。
#
# ORDER BY published_at は idx_parent_published (parent_id, published_at) の索引順
# そのものなので追加ソートは発生しない(EXPLAIN QUERY PLAN で SEARCH のみを確認済み)。
#
# 【索引の罠・記録】このクエリに `author_channel_id` の条件を足してはいけない。
# 足すと SQLite が idx_parent_published ではなく idx_comments_author_published を
# 選び、「このスレッドの返信」ではなく「そのアカウントの全履歴コメント」を舐める
# 形になる(連投の上位アカウントほど全履歴も多いので最悪ケース。2026-07-06 の
# 「結果が小さいから軽いと誤判断して2,000万行読んだ」インシデントと同型)。
# どうしてもアカウントで絞る必要が出たら `+author_channel_id = ?` と書いて
# 索引使用を抑制する(random_comment.py の `+parent_id IS NULL` と同じ手)。
# ------------------------------------------------------------------ #

Q_THREAD_REPLIES_SQL = """
SELECT author_channel_id, handle, published_at
FROM comments
WHERE parent_id = ? AND is_deleted = 0
ORDER BY published_at
"""


# ------------------------------------------------------------------ #
# payload 生成（純関数）
# ------------------------------------------------------------------ #

def _downsample(series: list[int], factor: int) -> list[int]:
    out = []
    for i in range(0, len(series), factor):
        out.append(sum(series[i:i + factor]))
    return out


def build_speed(q2_rows: list[dict], all_ids: list[str]) -> tuple[dict, dict[str, int]]:
    """Q2 の (分バケット, アカウント, 件数) から速度系列を組み立てる。

    all_ids: そのスレッドの全アカウント(上位に限らない)。peak_per_min は
    表(参加アカウント、全員表示)にも使うため全員ぶん計算する必要がある
    (円グラフ・速度グラフに埋め込む時系列は、呼び出し側で上位ぶんだけに
    絞り込む — payloadが不必要に膨らまないようにするため)。

    戻り値: (speed dict, {channel_id: そのアカウントの最大 rpm})
    バケット数が SPEED_MINUTE_BUCKET_LIMIT を超えたら5分バケットにダウンサンプルする。
    """
    if not q2_rows:
        return {"bucket_sec": 60, "t0": 0, "total": [], "by_account": {}}, {}

    minutes = [int(r["m"]) for r in q2_rows]
    m0, m1 = min(minutes), max(minutes)
    n = m1 - m0 + 1

    total = [0] * n
    per_account: dict[str, list[int]] = {cid: [0] * n for cid in all_ids}
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
    q1_rows: Q1_ACCOUNTS_SQL の結果(件数降順、全アカウント)
    q2_rows: Q2_MINUTE_BUCKETS_SQL の結果(全アカウント)
    gap_rows: q3_gap_stats_sql() の結果(全アカウント)
    median_gap_by_channel: {channel_id: 中央値秒}(全アカウント)
    total_reply_count: Pass1 で得た YouTube 側の返信数。取得件数との差を
      count_discrepancy として出すために使う(1000件上限や結果整合で乖離し得る)。

    payload には2種類のアカウント一覧を持つ:
      accounts     … 上位 TOP_ACCOUNTS 件のみ。円グラフ・速度グラフの内訳用
                      (これ以上載せてもグラフが読めなくなるので絞る)
      all_accounts … 全アカウント。「参加アカウント」表用(絞らない —
                      複数アカウントを使った連投を見るのがこの機能の目的なので、
                      2番手以降を「ほか」に畳んで隠すのは本末転倒)
    """
    now = int(now_epoch if now_epoch is not None else time.time())
    fetched_count = sum(int(r["c"]) for r in q1_rows)
    reply_count = int(total_reply_count) if total_reply_count is not None else fetched_count

    top_rows = q1_rows[:TOP_ACCOUNTS]
    top_ids = [r["author_channel_id"] for r in top_rows]
    all_ids = [r["author_channel_id"] for r in q1_rows]

    full_speed, peak_per_min = build_speed(q2_rows, all_ids)
    # 円グラフ・速度グラフに埋め込む時系列は上位ぶんだけに絞る。全員ぶん載せると
    # payloadが不必要に膨らみ、グラフ自体も読めなくなる
    # (peak_per_min は表(全員)にも使うのでフル計算のまま持っておく、上の注記参照)。
    # q2_rows が空の場合 build_speed() は by_account={} を早期returnするので、
    # .get() で欠損に備える(そのアカウントの時系列が単に無い=0件と同義)。
    full_by_account = full_speed.get("by_account", {})
    speed = dict(full_speed, by_account={cid: full_by_account.get(cid, []) for cid in top_ids})

    gap_by_channel = {r["author_channel_id"]: r for r in gap_rows}

    def _account_entry(r: dict) -> dict:
        cid = r["author_channel_id"]
        c = int(r["c"])
        g = gap_by_channel.get(cid) or {}
        return {
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
        }

    accounts = [_account_entry(r) for r in top_rows]
    all_accounts = [_account_entry(r) for r in q1_rows]

    rest = q1_rows[TOP_ACCOUNTS:]
    others = {
        "count": sum(int(r["c"]) for r in rest),
        "accounts": len(rest),
    }

    # 全アカウント(上位に限らない q1_rows 全体)の中で最後に返信が来た時刻。
    # 「全体の分速」の分母を「開始〜最後の返信」に限定するために使う
    # (「開始〜今/追跡終了時刻」だと、既に止まっているのに追跡がまだ続いている
    # だけの時間が分母に混じり、分速が実態と無関係に薄まってしまう)。
    last_reply_at = max(
        (int(r["last_at"]) for r in q1_rows if r.get("last_at") is not None),
        default=None,
    )

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
        "last_reply_at": last_reply_at,
        "updated_at": now,
        "reply_count": reply_count,
        "cap": REPLY_CAP,
        "remaining": max(0, REPLY_CAP - reply_count),
        "unique_accounts": len(q1_rows),
        "count_discrepancy": reply_count - fetched_count,
        "accounts": accounts,
        "all_accounts": all_accounts,
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
    """BAN 済み channel_id の集合。ksk_bans は運用者と自動BANが書く小テーブル。"""
    rows = turso.query("SELECT channel_id FROM ksk_bans")
    return {r["channel_id"] for r in rows}


def add_ban(turso: TursoClient, channel_id: str, reason: str, now_epoch: int) -> None:
    """BAN を登録する。既存なら理由だけ更新する（冪等）。

    取り消しは `DELETE FROM ksk_bans WHERE channel_id = ?`。自動BANは誤検知し得る
    ので、理由文に根拠（件数・スキャン時刻）を必ず残すこと。
    """
    turso.execute(
        "INSERT INTO ksk_bans (channel_id, reason, created_at) VALUES (?, ?, ?) "
        "ON CONFLICT(channel_id) DO UPDATE SET reason = excluded.reason",
        [channel_id, reason, now_epoch],
    )


def end_threads_of_owner(turso: TursoClient, channel_id: str, reason: str, now_epoch: int) -> int:
    """そのアカウントの active スレッドを全部終了する。戻り値は対象件数。

    BAN は「以後コマンドを無視する」だけの意味なので、BAN 前から走っている
    スレッドは放置すると最大6時間追跡され続ける。BAN した以上は追跡も止める。
    """
    rows = turso.query(
        "SELECT thread_id FROM ksk_threads WHERE owner_channel_id = ? AND state = ?",
        [channel_id, STATE_ACTIVE],
    )
    for r in rows:
        end_thread(turso, r["thread_id"], reason, now_epoch)
    return len(rows)


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


def classify_dormant_growth(increase: int) -> str:
    """ended スレッドの返信増加分から次のアクションを判定する(純関数)。

    戻り値: "none"(何もしない) / "reaggregate"(内訳だけ再集計) / "reactivate"(activeに戻す)
    """
    if increase <= 0:
        return "none"
    if increase >= DORMANT_REACTIVATE_THRESHOLD:
        return "reactivate"
    return "reaggregate"


def count_current_replies(turso: TursoClient, thread_id: str) -> int:
    """そのスレッドの comments 上の現在の生存返信数(is_deleted=0)。"""
    rows = turso.query(
        "SELECT COUNT(*) AS c FROM comments WHERE parent_id = ? AND is_deleted = 0",
        [thread_id],
    )
    return int(rows[0]["c"]) if rows else 0


def update_last_reply_count(turso: TursoClient, thread_id: str, reply_count: int, now_epoch: int) -> None:
    """state を変えずに last_reply_count だけ更新する(再集計のみのケース用)。"""
    turso.execute(
        "UPDATE ksk_threads SET last_reply_count = ?, updated_at = ? WHERE thread_id = ?",
        [reply_count, now_epoch, thread_id],
    )


def reactivate_thread(turso: TursoClient, thread_id: str, now_epoch: int) -> None:
    """ended スレッドを active に戻す。既に active なら何もしない。

    last_growth_at を「今」に更新するのが要点 — 忘れると evaluate_end_reason() の
    アイドル判定(30分)が古い値のままなので、復活しても次のPass1で即座に
    ended(idle) へ逆戻りしてしまう。
    """
    turso.execute(
        "UPDATE ksk_threads SET state = ?, ended_at = NULL, ended_reason = NULL, "
        "last_growth_at = ?, updated_at = ? WHERE thread_id = ? AND state = ?",
        [STATE_ACTIVE, now_epoch, now_epoch, thread_id, STATE_ENDED],
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


def dead_ratio_is_suspect(total: int, dead_count: int) -> bool:
    """Pass1 の「消滅」判定を信用してよいか。

    True なら、この回は削除マーキングをスキップすべき(実データ破損より
    取りこぼしを選ぶ — 次回の Pass1 で再検知できるので恒久的な見逃しにはならない)。
    バッチが小さいうちは1件の本当の削除でも比率が跳ねるため、
    KSK_MIN_BATCH_FOR_DEAD_RATIO_GUARD 件未満では常に信用する(比率判定自体が
    無意味なほど小さいサンプルなので、直接の signal を優先する)。
    """
    if total < KSK_MIN_BATCH_FOR_DEAD_RATIO_GUARD:
        return False
    return (dead_count / total) > KSK_MAX_DEAD_RATIO


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

def aggregate_thread(turso: TursoClient, thread_id: str):
    """1スレッドぶんの集計。**Turso へのクエリはこの1本だけ**(上の注記参照)。

    戻り値: (q1_rows, q2_rows, gap_rows, median_gap_by_channel)
    そのまま build_payload() に渡せる。
    """
    rows = turso.query(Q_THREAD_REPLIES_SQL, [thread_id])
    return aggregate_rows(rows)


def aggregate_rows(rows: list[dict]):
    """返信の生データから集計を作る純関数(Turso 非依存、単体テスト可能)。

    rows: [{"author_channel_id":..., "handle":..., "published_at":...}, ...]
    戻り値: (q1_rows, q2_rows, gap_rows, median_gap_by_channel)

    中央値は**全アカウント分**出す。以前は上位8件に限っていたが、複数アカウントを
    使った連投を見るのがこの機能の目的なので、2番手以降の間隔を捨ててはいけない。
    生データを1回引く方式なら全員分を出しても追加の rows_read はゼロ。
    """
    by_account: dict[str, dict] = {}
    buckets: dict[tuple, int] = {}

    for r in rows:
        cid = r.get("author_channel_id")
        ts = int(r["published_at"])
        entry = by_account.setdefault(cid, {"handle": None, "times": []})
        if r.get("handle"):
            entry["handle"] = r["handle"]
        entry["times"].append(ts)
        key = (ts // 60, cid)
        buckets[key] = buckets.get(key, 0) + 1

    q1_rows, gap_rows = [], []
    medians: dict[str, int] = {}

    for cid, entry in by_account.items():
        # 呼び出し元は published_at 昇順で渡す想定だが、順序に依存しないよう明示的に
        # ソートする(間隔の計算は順序が狂うと負の値になる)
        times = sorted(entry["times"])
        q1_rows.append({
            "author_channel_id": cid,
            "handle": entry["handle"],
            "c": len(times),
            "first_at": times[0],
            "last_at": times[-1],
        })

        gaps = [b - a for a, b in zip(times, times[1:])]
        if not gaps:
            continue
        nonzero = [g for g in gaps if g > 0]
        gap_rows.append({
            "author_channel_id": cid,
            "avg_gap": sum(gaps) / len(gaps),
            # published_at は秒精度なので同秒投稿が普通に起きる。0を「最速0秒」として
            # 見せると無限速度に見えるため除外し、0の個数は別指標にする。
            "min_gap": min(nonzero) if nonzero else None,
            "same_second_pairs": sum(1 for g in gaps if g == 0),
            "gap_n": len(gaps),
        })
        ordered = sorted(gaps)
        # 偶数個なら小さい側(下位中央値)。旧SQLの LIMIT 1 OFFSET (n-1)//2 と同じ規約
        medians[cid] = ordered[(len(ordered) - 1) // 2]

    # 件数降順。同数はチャンネルIDで安定させる(表示順が実行ごとにぶれないように)
    q1_rows.sort(key=lambda r: (-r["c"], r["author_channel_id"] or ""))
    q2_rows = [
        {"m": m, "author_channel_id": cid, "c": n}
        for (m, cid), n in sorted(buckets.items(), key=lambda kv: (kv[0][0], kv[0][1] or ""))
    ]
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
