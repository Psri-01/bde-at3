{{
    config(
        materialized='table',
        schema='silver'
    )
}}

SELECT
    listing_id::bigint AS listing_id,
    scrape_id::bigint AS scrape_id,
    scraped_date::timestamp AS scraped_date,
    host_id::bigint AS host_id,
    host_name,
    host_since::date AS host_since,
    CASE WHEN host_is_superhost = 't' THEN TRUE ELSE FALSE END AS host_is_superhost,
    host_neighbourhood,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates::integer AS accommodates,
    price::numeric AS price,
    CASE WHEN has_availability = 't' THEN TRUE ELSE FALSE END AS has_availability,
    availability_30::integer AS availability_30,
    number_of_reviews::integer AS number_of_reviews,
    review_scores_rating::numeric AS review_scores_rating,
    review_scores_accuracy::numeric AS review_scores_accuracy,
    review_scores_cleanliness::numeric AS review_scores_cleanliness,
    review_scores_checkin::numeric AS review_scores_checkin,
    review_scores_communication::numeric AS review_scores_communication,
    review_scores_value::numeric AS review_scores_value,
    snapshot_month,
    source_file,
    ingested_at

FROM {{ source('bronze', 'airbnb_listings_raw') }}
WHERE listing_id IS NOT NULL