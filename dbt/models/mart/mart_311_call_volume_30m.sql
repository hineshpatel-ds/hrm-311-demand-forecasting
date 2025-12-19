select
    date_trunc('minute', call_ts) as bucket_ts,
    offered,
    handled,
    abandoned,
    processed_in_ivr,
    total_talk_time_sec,
    avg_talk_time_sec
from staging.stg_311_call_volumes
