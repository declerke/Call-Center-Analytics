WITH calls AS (
    SELECT * FROM {{ ref('int_unified_calls') }}
    WHERE is_answered = TRUE
)

SELECT
    source_centre,
    topic,
    EXTRACT(MONTH FROM call_date)::INTEGER                                              AS call_month,
    CASE
        WHEN satisfaction_rating = 5 THEN 'Promoter (5)'
        WHEN satisfaction_rating = 4 THEN 'Satisfied (4)'
        WHEN satisfaction_rating = 3 THEN 'Neutral (3)'
        WHEN satisfaction_rating = 2 THEN 'Dissatisfied (2)'
        WHEN satisfaction_rating = 1 THEN 'Detractor (1)'
        ELSE 'Not Rated'
    END                                                                                 AS satisfaction_band,
    COUNT(*)                                                                            AS call_count,
    ROUND(AVG(avg_talk_duration_secs), 2)                                               AS avg_handle_time_secs,
    ROUND(AVG(speed_of_answer_secs), 2)                                                 AS avg_speed_of_answer_secs
FROM calls
GROUP BY
    source_centre,
    topic,
    EXTRACT(MONTH FROM call_date),
    CASE
        WHEN satisfaction_rating = 5 THEN 'Promoter (5)'
        WHEN satisfaction_rating = 4 THEN 'Satisfied (4)'
        WHEN satisfaction_rating = 3 THEN 'Neutral (3)'
        WHEN satisfaction_rating = 2 THEN 'Dissatisfied (2)'
        WHEN satisfaction_rating = 1 THEN 'Detractor (1)'
        ELSE 'Not Rated'
    END
