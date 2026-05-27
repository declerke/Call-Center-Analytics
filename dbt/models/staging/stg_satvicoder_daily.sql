WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_satvicoder_calls') }}
),

renamed AS (
    SELECT
        CAST("Index" AS INTEGER)                                                                   AS day_index,
        CAST("Incoming Calls" AS INTEGER)                                                          AS incoming_calls,
        CAST("Answered Calls" AS INTEGER)                                                          AS answered_calls,
        CAST("Abandoned Calls" AS INTEGER)                                                         AS abandoned_calls,
        -- Answer Rate may be stored as 85.30 or "85.30%" — strip % to be safe
        CAST(REPLACE(CAST("Answer Rate" AS VARCHAR), '%', '') AS DOUBLE)                           AS answer_rate_pct,
        -- Service Level (20 Seconds) similarly stored as percent value
        CAST(REPLACE(CAST("Service Level (20 Seconds)" AS VARCHAR), '%', '') AS DOUBLE)           AS service_level_pct,
        EPOCH(CAST("Answer Speed (AVG)" AS TIME))                                                  AS avg_answer_speed_secs,
        EPOCH(CAST("Talk Duration (AVG)" AS TIME))                                                 AS avg_talk_duration_secs,
        EPOCH(CAST("Waiting Time (AVG)" AS TIME))                                                  AS avg_waiting_time_secs,
        'satvicoder'                                                                               AS source_centre
    FROM source
    WHERE "Incoming Calls" IS NOT NULL
)

SELECT * FROM renamed
