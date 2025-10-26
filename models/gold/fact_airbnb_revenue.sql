{{ config(materialized='table', schema='gold') }}

SELECT
    l.listing_id,
    l.host_id,
    l.room_type,
    l.price,
    l.number_of_reviews,
    l.availability_30,
    h.host_name,
    h.host_is_superhost,
    h.total_listings_count,
    n.neighbourhood_name,
    lga.lga_name,
    DATE_TRUNC('month', CURRENT_TIMESTAMP)::date AS snapshot_month,
    CURRENT_TIMESTAMP::timestamp AS load_date
FROM {{ ref('dim_neighbourhood') }} n
LEFT JOIN {{ ref('dim_lga') }} lga
    ON LOWER(n.neighbourhood_name) LIKE LOWER('%' || lga.lga_name || '%')
LEFT JOIN {{ source('bronze', 'airbnb_listings_raw') }} l
    ON n.neighbourhood_name = l.listing_neighbourhood
LEFT JOIN {{ ref('dim_host') }} h
    ON l.host_id = h.host_id
WHERE l.listing_neighbourhood IS NOT NULL
  AND l.price IS NOT NULL
  AND l.host_id IS NOT NULL