WITH daily AS (
    SELECT * FROM {{ ref('stg_satvicoder_daily') }}
)

SELECT
    day_index,
    source_centre,
    incoming_calls,
    answered_calls,
    abandoned_calls,
    (incoming_calls - answered_calls - abandoned_calls)                                    AS other_calls,
    answer_rate_pct,
    ROUND(
        (abandoned_calls::DOUBLE / NULLIF(incoming_calls, 0)) * 100, 2
    )                                                                                      AS abandonment_rate_pct,
    service_level_pct,
    avg_answer_speed_secs,
    avg_talk_duration_secs,
    avg_waiting_time_secs,
    avg_answer_speed_secs + avg_talk_duration_secs                                         AS avg_handle_time_secs
FROM daily
