{{
    config(
        materialized='table',
        schema='silver',
        tags=['staging', 'mapping']
    )
}}

SELECT
    lga_code::VARCHAR AS lga_code,
    lga_name::VARCHAR AS lga_name,
    ingested_at
FROM {{ source('bronze', 'nsw_lga_code_raw') }}
WHERE lga_code IS NOT NULL