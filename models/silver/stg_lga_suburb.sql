{{
    config(
        materialized='table',
        schema='silver',
        tags=['staging', 'mapping']
    )
}}

SELECT
    suburb_name::varchar AS suburb_name,
    lga_name::varchar AS lga_name,
    ingested_at,
    source_file,
    snapshot_month
FROM {{ source('bronze', 'nsw_lga_suburb_raw') }}
WHERE suburb_name IS NOT NULL AND lga_name IS NOT NULL