WITH satvicoder AS (
    SELECT * FROM {{ ref('stg_satvicoder_calls') }}
),

pwc AS (
    SELECT * FROM {{ ref('stg_pwc_calls') }}
),

unified AS (
    SELECT * FROM satvicoder
    UNION ALL
    SELECT * FROM pwc
)

SELECT * FROM unified
