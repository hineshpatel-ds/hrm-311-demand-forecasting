with src as (
    select *
    from raw.raw_311_call_volumes
)

select
    "ObjectId"::bigint                        as object_id,
    ("CALL_DATE"::timestamptz)                as call_ts,
    "MILITARY_HOUR"::int                      as military_hour,
    "INTERVAL"::text                          as interval_label,

    coalesce("OFFERED", 0)::int               as offered,
    coalesce("HANDLED", 0)::int               as handled,
    coalesce("ABANDONED", 0)::int             as abandoned,
    coalesce("PROCESSED_IN_IVR", 0)::int      as processed_in_ivr,

    coalesce("TOTAL_TALK_TIME", 0)::int       as total_talk_time_sec,
    coalesce("AVERAGE_TALK_TIME", 0)::int     as avg_talk_time_sec,

    _ingested_at_utc                          as ingested_at_utc
from src
