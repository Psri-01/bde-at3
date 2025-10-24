{{ config(materialized='table', schema='gold', tags=['fact']) }}

SELECT
    stg.listing_id,
    stg.scraped_date AS listing_date, -- The date of the fact event
    stg.price,
    (30 - stg.availability_30) AS number_of_stays,
    (CASE WHEN stg.has_availability = TRUE THEN (30 - stg.availability_30) * stg.price ELSE 0 END) AS estimated_revenue,
    
    -- Foreign Keys linked via SCD2 logic
    host.host_id AS host_fk,
    lga.lga_code AS lga_fk,
    sub.neighbourhood_unique_key AS neighbourhood_fk,

    stg.snapshot_month
    
FROM {{ ref('stg_airbnb_listings') }} stg
-- 1. Join to Host Dimension (SCD2)
JOIN {{ ref('dim_host') }} host
  ON stg.host_id = host.host_id
 AND stg.scraped_date BETWEEN host.dbt_valid_from AND COALESCE(host.dbt_valid_to, '9999-12-31'::TIMESTAMP)

-- 2. Join to LGA Dimension (SCD2)
JOIN {{ ref('dim_lga') }} lga
  -- You need a way to link neighbourhood to LGA code. Use the mapping tables to find the LGA code 
  -- for the neighbourhood name, then join based on the code/name validity
  -- For now, we will join to the dim_lga on its unique key
  ON stg.lga_code = lga.lga_code -- Assuming you add lga_code to stg_airbnb_listings later
 AND stg.scraped_date BETWEEN lga.dbt_valid_from AND COALESCE(lga.dbt_valid_to, '9999-12-31'::TIMESTAMP)

-- 3. Join to Neighbourhood Dimension (SCD2)
JOIN {{ ref('dim_neighbourhood') }} sub
  -- Join using the fields that form the unique key for the neighbourhood/suburb (from the snapshot logic)
  ON MD5(CAST(COALESCE(CAST(stg.listing_neighbourhood AS VARCHAR), '') || '-' || COALESCE(CAST(stg.room_type AS VARCHAR), '') AS VARCHAR)) = sub.neighbourhood_unique_key 
 AND stg.scraped_date BETWEEN sub.dbt_valid_from AND COALESCE(sub.dbt_valid_to, '9999-12-31'::TIMESTAMP)

WHERE stg.listing_id IS NOT NULL