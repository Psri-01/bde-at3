{{ config(
    materialized='incremental',
    unique_key='listing_id',
    schema='gold', 
    tags=['fact']
) }}

-- Re-create LGA lookup within the fact model for guaranteed column existence
WITH lga_lookup AS (
    SELECT 
        lgs.suburb_name,
        lgc.lga_code 
    FROM {{ ref('stg_lga_suburb') }} lgs
    JOIN {{ ref('stg_lga_code') }} lgc 
        -- Joins the suburb mapping table to the LGA code table on LGA name
        ON UPPER(TRIM(lgs.lga_name)) = UPPER(TRIM(lgc.lga_name))
),

final_fact AS (
    SELECT
        stg.listing_id,
        stg.scraped_date AS listing_date,
        stg.price,
        stg.has_availability,
        stg.number_of_reviews,
        stg.review_scores_rating,
        
        -- Foreign Key Aliases (FKs)
        host.host_id AS host_fk,
        lga.lga_code AS lga_fk, -- This is the key from the dim_lga
        sub.neighbourhood_unique_key AS neighbourhood_fk, -- Correct FK based on snapshot unique_key

        -- Calculated Metrics
        (30 - stg.availability_30) AS number_of_stays,
        (CASE WHEN stg.has_availability = TRUE THEN (30 - stg.availability_30) * stg.price ELSE 0 END) AS estimated_revenue,
        
        stg.snapshot_month
        
    FROM {{ ref('stg_airbnb_listings') }} stg
    
    -- Need to join to lookup table first to get LGA code (lkg)
    LEFT JOIN lga_lookup lkg
        ON UPPER(TRIM(stg.listing_neighbourhood)) = UPPER(TRIM(lkg.suburb_name))

    -- 1. Join to dim_host (SCD2 Logic)
    JOIN {{ ref('dim_host') }} host
      ON stg.host_id = host.host_id
     AND stg.scraped_date BETWEEN host.dbt_valid_from AND COALESCE(host.dbt_valid_to, '9999-12-31'::TIMESTAMP)

    -- 2. Join to dim_lga (SCD2 Logic)
    JOIN {{ ref('dim_lga') }} lga
      -- Use the LGA code derived in the CTE for the SCD2 lookup
      ON lkg.lga_code = lga.lga_code 
     AND stg.scraped_date BETWEEN lga.dbt_valid_from AND COALESCE(lga.dbt_valid_to, '9999-12-31'::TIMESTAMP)

    -- 3. Join to dim_neighbourhood (SCD2 Logic)
    JOIN {{ ref('dim_neighbourhood') }} sub
      ON MD5(CAST(COALESCE(CAST(stg.listing_neighbourhood AS VARCHAR), '') || '-' || COALESCE(CAST(stg.room_type AS VARCHAR), '') AS VARCHAR)) = sub.neighbourhood_unique_key
     AND stg.scraped_date BETWEEN sub.dbt_valid_from AND COALESCE(sub.dbt_valid_to, '9999-12-31'::TIMESTAMP)
     
    WHERE stg.listing_id IS NOT NULL
)

{% if is_incremental() %}
    -- Filter on the snapshot_month column for incremental loads
    WHERE snapshot_month > (SELECT MAX(snapshot_month) FROM {{ this }})
{% endif %}

SELECT * FROM final_fact