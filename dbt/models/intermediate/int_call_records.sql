WITH calls AS (
    SELECT * FROM {{ ref('stg_pwc_calls') }}
)

SELECT
    call_id,
    agent,
    call_date,
    call_time,
    topic,
    is_answered,
    is_resolved,
    speed_of_answer_secs,
    avg_talk_duration_secs,
    satisfaction_rating,
    source_centre,
    CASE WHEN is_answered THEN speed_of_answer_secs <= 30 ELSE FALSE END AS meets_sla_30s,
    COALESCE(speed_of_answer_secs, 0) + COALESCE(avg_talk_duration_secs, 0) AS total_handle_secs
FROM calls
