{{ config(materialized='table', schema='gold', tags=['fact']) }}

SELECT
    stg.listing_id,
    stg.scraped_date AS listing_date,
    stg.price,
    stg.has_availability,
    stg.number_of_reviews,
    stg.review_scores_rating,
    
    -- Foreign Key Aliases (FKs)
    host.host_id AS host_fk,
    lga.lga_code AS lga_fk,
    sub.neighbourhood_unique_key AS neighbourhood_fk, -- Correct FK based on snapshot unique_key

    -- Calculated Metrics
    (30 - stg.availability_30) AS number_of_stays,
    (CASE WHEN stg.has_availability = TRUE THEN (30 - stg.availability_30) * stg.price ELSE 0 END) AS estimated_revenue,
    
    stg.snapshot_month
    
FROM {{ ref('stg_airbnb_listings') }} stg

-- 1. Join to dim_host (SCD2 Logic)
JOIN {{ ref('dim_host') }} host
  ON stg.host_id = host.host_id
 AND stg.scraped_date BETWEEN host.dbt_valid_from AND COALESCE(host.dbt_valid_to, '9999-12-31'::TIMESTAMP)

-- 2. Join to dim_lga (SCD2 Logic)
-- Join to LGA Dimension (SCD2)
JOIN {{ ref('dim_lga') }} lga
  -- Use the exact quoted column name from the staging model output
  ON stg.listing_lga_code = lga.lga_code
 AND stg.scraped_date BETWEEN lga.dbt_valid_from AND COALESCE(lga.dbt_valid_to, '9999-12-31'::TIMESTAMP)
 
-- 3. Join to dim_neighbourhood (SCD2 Logic)
JOIN {{ ref('dim_neighbourhood') }} sub
  -- Join using the MD5 hash (unique key) calculated in the snapshot logic
  ON MD5(CAST(COALESCE(CAST(stg.listing_neighbourhood AS VARCHAR), '') || '-' || COALESCE(CAST(stg.room_type AS VARCHAR), '') AS VARCHAR)) = sub.neighbourhood_unique_key
 AND stg.scraped_date BETWEEN sub.dbt_valid_from AND COALESCE(sub.dbt_valid_to, '9999-12-31'::TIMESTAMP)
 
WHERE stg.listing_id IS NOT NULL