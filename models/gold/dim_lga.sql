{{ config(materialized='table', schema='gold') }}

SELECT
    CAST(lga.lga_code AS TEXT) AS lga_code,
    TRIM(lga.lga_name) AS lga_name,
    lga.snapshot_month,
    CURRENT_TIMESTAMP::timestamp AS load_date
FROM {{ ref('lga_snapshot') }} AS lga
WHERE lga.lga_code IS NOT NULL