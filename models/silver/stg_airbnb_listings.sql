{{
 	config(
 		materialized='table',
 		schema='silver'
 	)
}}

-- CTE to look up LGA code and name using the mapping table
WITH lga_lookup AS (
    SELECT 
        lgs.suburb_name,
        lgs.lga_name AS suburb_lga_name,
        lgc.lga_code 
    FROM {{ ref('stg_lga_suburb') }} lgs
    JOIN {{ ref('stg_lga_code') }} lgc 
        -- Joins the suburb mapping table to the LGA code table on LGA name
        ON UPPER(TRIM(lgs.lga_name)) = UPPER(TRIM(lgc.lga_name))
),

final_listings AS (
    SELECT
        a.listing_id::bigint AS listing_id,
        a.scrape_id::bigint AS scrape_id,
        a.scraped_date::timestamp AS scraped_date,
        a.host_id::bigint AS host_id,
        a.host_name,
        a.host_since::date AS host_since,
        CASE WHEN a.host_is_superhost = 't' THEN TRUE ELSE FALSE END AS host_is_superhost,
        a.host_neighbourhood,
        a.listing_neighbourhood,
        a.property_type,
        a.room_type,
        a.accommodates::integer AS accommodates,
        a.price::numeric AS price,
        CASE WHEN a.has_availability = 't' THEN TRUE ELSE FALSE END AS has_availability,
        a.availability_30::integer AS availability_30,
        a.number_of_reviews::integer AS number_of_reviews,
        a.review_scores_rating::numeric AS review_scores_rating,
        a.review_scores_accuracy::numeric AS review_scores_accuracy,
        a.review_scores_cleanliness::numeric AS review_scores_cleanliness,
        a.review_scores_checkin::numeric AS review_scores_checkin,
        a.review_scores_communication::numeric AS review_scores_communication,
        a.review_scores_value::numeric AS review_scores_value,
        a.snapshot_month,
        a.source_file,
        a.ingested_at,
        
        -- Add LGA Code and Name resolved via join
        l.lga_code AS listing_lga_code, 
        l.suburb_lga_name AS listing_lga_name 
        
    FROM {{ source('bronze', 'airbnb_listings_raw') }} a
    LEFT JOIN lga_lookup l
        -- Joins the listing to the lookup table on the neighbourhood/suburb name
        ON UPPER(TRIM(a.listing_neighbourhood)) = UPPER(TRIM(l.suburb_name))
    WHERE a.listing_id IS NOT NULL
)

SELECT 
    -- Explicitly list ALL columns to prevent ambiguity with SELECT *
    listing_id,
    scrape_id,
    scraped_date,
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value,
    snapshot_month,
    source_file,
    ingested_at,
    listing_lga_code,
    listing_lga_name 
FROM final_listings