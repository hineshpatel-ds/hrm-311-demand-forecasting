with src as (
    select *
    from raw.raw_311_call_volumes
),

typed as (
    select
        "ObjectId"::bigint                        as object_id,
        "CALL_DATE"::timestamptz                  as call_date,
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
)

-- CALL_DATE only carries the calendar day (it's constant across all 48
-- intervals in a day); the real time-of-day lives in MILITARY_HOUR / INTERVAL
-- and has to be added back in, or every row for a day collapses to one bucket.
select
    object_id,
    date_trunc('day', call_date)
        + (military_hour || ' hours')::interval
        + (substring(interval_label from ':(\d{2}) ') || ' minutes')::interval
                                               as call_ts,
    military_hour,
    interval_label,

    offered,
    handled,
    abandoned,
    processed_in_ivr,

    total_talk_time_sec,
    avg_talk_time_sec,

    ingested_at_utc
from typed
