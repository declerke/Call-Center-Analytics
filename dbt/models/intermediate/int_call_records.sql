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
    speed_of_answer_secs <= 30                         AS meets_sla_30s,
    speed_of_answer_secs + avg_talk_duration_secs      AS total_handle_secs
FROM calls
