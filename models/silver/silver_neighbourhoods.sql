{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', 'neighbourhood']
    )
}}

SELECT
    MD5(CONCAT_WS('-', listing_neighbourhood, room_type, property_type)) AS neighbourhood_unique_key,
    listing_neighbourhood AS neighbourhood_name,
    room_type,
    property_type,
    MAX(scraped_date)::timestamp AS latest_listing_date
FROM {{ source('bronze', 'airbnb_listings_raw') }}
WHERE listing_neighbourhood IS NOT NULL
GROUP BY listing_neighbourhood, room_type, property_type
