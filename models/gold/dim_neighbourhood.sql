{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    listing_neighbourhood AS neighbourhood_name,
    room_type,
    DATE_TRUNC('month', CURRENT_TIMESTAMP)::date AS snapshot_month,
    CURRENT_TIMESTAMP::timestamp AS load_date
FROM {{ source('bronze', 'airbnb_listings_raw') }}
WHERE listing_neighbourhood IS NOT NULL