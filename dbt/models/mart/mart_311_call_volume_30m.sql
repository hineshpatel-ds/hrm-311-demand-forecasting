{{ config(materialized='table') }}

SELECT 
    DATE_TRUNC('hour', call_ts) as bucket_ts, 
    SUM(offered) as offered,
    SUM(handled) as handled,
    SUM(abandoned) as abandoned,
    SUM(processed_in_ivr) as processed_in_ivr,
    SUM(total_talk_time_sec) as total_talk_time_sec,
    AVG(avg_talk_time_sec) as avg_talk_time_sec
FROM {{ ref('stg_311_call_volumes') }}
GROUP BY 1