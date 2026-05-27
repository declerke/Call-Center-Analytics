WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_pwc_calls') }}
),

renamed AS (
    SELECT
        -- PWC uses "Call Id" (lowercase d) vs satvicoder "Call ID" — normalise to call_id
        CAST("Call Id" AS VARCHAR)                                              AS call_id,
        TRIM(CAST("Agent" AS VARCHAR))                                          AS agent,
        STRPTIME(CAST("Date" AS VARCHAR), '%m/%d/%Y')::DATE                     AS call_date,
        CAST("Time" AS TIME)                                                    AS call_time,
        TRIM(CAST("Topic" AS VARCHAR))                                          AS topic,
        UPPER(TRIM(CAST("Answered (Y/N)" AS VARCHAR))) = 'Y'                    AS is_answered,
        UPPER(TRIM(CAST("Resolved" AS VARCHAR))) = 'Y'                          AS is_resolved,
        CAST("Speed of answer in seconds" AS DOUBLE)                            AS speed_of_answer_secs,
        EPOCH(CAST("AvgTalkDuration" AS TIME))                                  AS avg_talk_duration_secs,
        CAST("Satisfaction rating" AS DOUBLE)                                   AS satisfaction_rating,
        'pwc'                                                                   AS source_centre
    FROM source
    WHERE "Call Id" IS NOT NULL
)

SELECT * FROM renamed
