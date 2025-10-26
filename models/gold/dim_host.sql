{{ config(materialized='table', schema='gold') }}

SELECT
    h.host_id,
    h.host_name,
    h.host_since,
    h.host_is_superhost,
    COALESCE(CAST(h.total_listings_count AS INT), 0) AS total_listings_count,
    h.snapshot_month,
    CURRENT_TIMESTAMP::timestamp AS load_date
FROM {{ ref('host_snapshot') }} AS h
WHERE h.host_id IS NOT NULL