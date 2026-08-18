{{ config(materialized='table') }}

-- date_bin (not date_trunc, which only rounds to whole hours) is what actually
-- produces 30-minute buckets to match this mart's grain.
SELECT
    date_bin('30 minutes', call_ts, TIMESTAMPTZ '2001-01-01') as bucket_ts,
    SUM(offered) as offered,
    SUM(handled) as handled,
    SUM(abandoned) as abandoned,
    SUM(processed_in_ivr) as processed_in_ivr,
    SUM(total_talk_time_sec) as total_talk_time_sec,
    AVG(avg_talk_time_sec) as avg_talk_time_sec
FROM {{ ref('stg_311_call_volumes') }}
GROUP BY 1