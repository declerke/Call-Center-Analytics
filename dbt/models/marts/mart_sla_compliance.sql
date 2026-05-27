-- SLA threshold: calls answered within 30 seconds = compliant (industry standard)
WITH calls AS (
    SELECT * FROM {{ ref('int_call_records') }}
)

SELECT
    source_centre,
    call_date,
    EXTRACT(HOUR FROM call_time)::INTEGER                                               AS call_hour,
    topic,
    COUNT(*)                                                                            AS total_calls,
    SUM(CASE WHEN is_answered THEN 1 ELSE 0 END)                                       AS answered_calls,
    SUM(CASE WHEN is_answered AND speed_of_answer_secs <= 30 THEN 1 ELSE 0 END)        AS sla_met_calls,
    SUM(CASE WHEN is_answered AND speed_of_answer_secs > 30 THEN 1 ELSE 0 END)         AS sla_breach_calls,
    ROUND(
        COALESCE(
            SUM(CASE WHEN is_answered AND speed_of_answer_secs <= 30 THEN 1 ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN is_answered THEN 1 ELSE 0 END), 0) * 100,
            0
        ), 2
    )                                                                                   AS sla_compliance_pct,
    ROUND(AVG(CASE WHEN is_answered THEN speed_of_answer_secs END), 2)                 AS avg_speed_of_answer_secs
FROM calls
GROUP BY source_centre, call_date, EXTRACT(HOUR FROM call_time), topic
