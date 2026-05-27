WITH calls AS (
    SELECT * FROM {{ ref('int_call_records') }}
)

SELECT
    source_centre,
    EXTRACT(HOUR FROM call_time)::INTEGER                                               AS call_hour,
    COUNT(*)                                                                            AS total_calls,
    SUM(CASE WHEN is_answered THEN 1 ELSE 0 END)                                       AS answered_calls,
    SUM(CASE WHEN is_answered AND speed_of_answer_secs <= 30 THEN 1 ELSE 0 END)        AS sla_met_calls,
    ROUND(
        SUM(CASE WHEN is_answered AND speed_of_answer_secs <= 30 THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(SUM(CASE WHEN is_answered THEN 1 ELSE 0 END), 0) * 100, 2
    )                                                                                   AS sla_compliance_pct,
    ROUND(AVG(CASE WHEN is_answered THEN speed_of_answer_secs END), 2)                 AS avg_speed_of_answer_secs,
    ROUND(AVG(CASE WHEN satisfaction_rating > 0 THEN satisfaction_rating END), 2)      AS avg_satisfaction
FROM calls
GROUP BY source_centre, EXTRACT(HOUR FROM call_time)
