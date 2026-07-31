"""Keep the cheap account-page core fields fresh as comments are inserted.

The account page is a record of comments that the toolbox has observed, so a
later YouTube deletion never decrements these values.  The trigger only fires
for a real INSERT; the normal comment_id UPSERT conflict path therefore cannot
double-count rows that comment_sync sees again.

Detailed profile analytics remain owned by analytics_aggregates_updater.  Its
full per-account rebuild also repairs these lightweight values and replaces the
blank core_hash written here.
"""


_PROFILE_TABLE_EXISTS_SQL = (
    "SELECT 1 AS present FROM sqlite_master "
    "WHERE type = 'table' AND name = 'account_profile_data' LIMIT 1"
)


_CREATE_TRIGGER_SQL = r"""
CREATE TRIGGER IF NOT EXISTS account_profile_live_core_after_comment_insert_v1
AFTER INSERT ON comments
WHEN NEW.author_channel_id IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM account_profile_data WHERE channel_id = NEW.author_channel_id
  )
BEGIN
  UPDATE account_profile_data
  SET core_payload = json_patch(
        core_payload,
        json_object(
          'handle_snapshot', CASE
            WHEN NEW.handle IS NOT NULL AND NEW.handle <> '' THEN NEW.handle
            ELSE json_extract(core_payload, '$.handle_snapshot')
          END,
          'first_comment_date', CASE
            WHEN json_extract(core_payload, '$.first_comment_date') IS NULL
              OR date(NEW.published_at, 'unixepoch', '+9 hours')
                 < json_extract(core_payload, '$.first_comment_date')
            THEN date(NEW.published_at, 'unixepoch', '+9 hours')
            ELSE json_extract(core_payload, '$.first_comment_date')
          END,
          'last_comment_date', CASE
            WHEN json_extract(core_payload, '$.last_comment_date') IS NULL
              OR date(NEW.published_at, 'unixepoch', '+9 hours')
                 > json_extract(core_payload, '$.last_comment_date')
            THEN date(NEW.published_at, 'unixepoch', '+9 hours')
            ELSE json_extract(core_payload, '$.last_comment_date')
          END,
          'total_comments',
            CAST(COALESCE(json_extract(core_payload, '$.total_comments'), 0) AS INTEGER) + 1,
          'peak_total_comments', MAX(
            CAST(COALESCE(json_extract(core_payload, '$.peak_total_comments'), 0) AS INTEGER),
            CAST(COALESCE(json_extract(core_payload, '$.total_comments'), 0) AS INTEGER) + 1
          ),
          'thread_count',
            CAST(COALESCE(json_extract(core_payload, '$.thread_count'), 0) AS INTEGER)
              + CASE WHEN NEW.parent_id IS NULL THEN 1 ELSE 0 END,
          'reply_count',
            CAST(COALESCE(json_extract(core_payload, '$.reply_count'), 0) AS INTEGER)
              + CASE WHEN NEW.parent_id IS NULL THEN 0 ELSE 1 END,
          'thread_ratio',
            CAST(
              CAST(COALESCE(json_extract(core_payload, '$.thread_count'), 0) AS INTEGER)
                + CASE WHEN NEW.parent_id IS NULL THEN 1 ELSE 0 END
              AS REAL
            ) / MAX(
              1,
              CAST(COALESCE(json_extract(core_payload, '$.thread_count'), 0) AS INTEGER)
                + CAST(COALESCE(json_extract(core_payload, '$.reply_count'), 0) AS INTEGER) + 1
            )
        )
      ),
      -- The JSON changed without running Python's SHA-256 helper.  An empty
      -- hash deliberately forces the next authoritative rebuild to rewrite
      -- and re-hash the complete core/analytics payload.
      core_hash = '',
      core_updated_at = unixepoch()
  WHERE channel_id = NEW.author_channel_id;
END
"""


def ensure_live_core_trigger(turso) -> bool:
    """Install the trigger when account_profile_data has been initialized.

    comment_sync can also run against a fresh comments-only database.  In that
    case we skip installation rather than making comment ingestion depend on
    another repository having created its tables already.
    """
    if not turso.query(_PROFILE_TABLE_EXISTS_SQL):
        return False
    turso.execute(_CREATE_TRIGGER_SQL)
    return True
