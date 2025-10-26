{{ config(materialized='table', schema='gold', tags=['dimension']) }}

WITH host_data AS (
    SELECT
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        scraped_date,
        DATE_TRUNC('month', scraped_date::TIMESTAMP) AS listing_date,
        -- Deduplicate: keep the latest record per host per month
        ROW_NUMBER() OVER (
            PARTITION BY host_id, DATE_TRUNC('month', scraped_date::TIMESTAMP)
            ORDER BY scraped_date DESC
        ) AS rn
    FROM {{ ref('stg_airbnb_listings') }}
    WHERE host_id IS NOT NULL
),

deduped AS (
    SELECT
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        listing_date,
        -- Create SCD2 columns manually
        listing_date AS dbt_valid_from,
        LEAD(listing_date) OVER (PARTITION BY host_id ORDER BY listing_date) AS dbt_valid_to
    FROM host_data
    WHERE rn = 1
)

SELECT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    dbt_valid_from,
    COALESCE(dbt_valid_to, '9999-12-31'::TIMESTAMP) AS dbt_valid_to
FROM deduped