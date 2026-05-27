WITH ops AS (
    SELECT * FROM {{ ref('int_daily_operations') }}
)

SELECT
    day_index,
    source_centre,
    incoming_calls,
    answered_calls,
    abandoned_calls,
    other_calls,
    ROUND(answer_rate_pct, 2)                                                              AS answer_rate_pct,
    ROUND(abandonment_rate_pct, 2)                                                         AS abandonment_rate_pct,
    ROUND(service_level_pct, 2)                                                            AS service_level_pct,
    ROUND(avg_answer_speed_secs, 2)                                                        AS avg_answer_speed_secs,
    ROUND(avg_talk_duration_secs, 2)                                                       AS avg_talk_duration_secs,
    ROUND(avg_waiting_time_secs, 2)                                                        AS avg_waiting_time_secs,
    ROUND(avg_handle_time_secs, 2)                                                         AS avg_handle_time_secs,
    ROUND(AVG(answer_rate_pct) OVER (
        ORDER BY day_index ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                                                  AS answer_rate_7d_avg,
    ROUND(AVG(service_level_pct) OVER (
        ORDER BY day_index ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                                                  AS service_level_7d_avg,
    ROUND(AVG(abandonment_rate_pct) OVER (
        ORDER BY day_index ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                                                  AS abandonment_rate_7d_avg
FROM ops
ORDER BY day_index
