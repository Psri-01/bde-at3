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
        lga.lga_code AS lga_fk,
        sub.neighbourhood_unique_key AS neighbourhood_fk,
        
        -- Calculated Metrics
        (30 - stg.availability_30) AS number_of_stays,
        (CASE WHEN stg.has_availability = TRUE THEN (30 - stg.availability_30) * stg.price ELSE 0 END) AS estimated_revenue,

        stg.snapshot_month

    FROM {{ ref('stg_airbnb_listings') }} stg

    -- Need to join to lookup table first to get LGA code
    LEFT JOIN lga_lookup lkg
        ON UPPER(TRIM(stg.listing_neighbourhood)) = UPPER(TRIM(lkg.suburb_name))
    
    -- 1. Join to dim_host (SCD2 Logic - this dimension has dbt_valid_from/to)
    JOIN {{ ref('dim_host') }} host
        ON stg.host_id = host.host_id
       AND stg.scraped_date BETWEEN host.dbt_valid_from AND COALESCE(host.dbt_valid_to, '9999-12-31'::TIMESTAMP)
    
    -- 2. Join to dim_lga (SCD2 Logic - this dimension has dbt_valid_from/to from snapshot)
    JOIN {{ ref('dim_lga') }} lga
        ON lkg.lga_code = lga.lga_code
       AND stg.scraped_date BETWEEN lga.dbt_valid_from AND COALESCE(lga.dbt_valid_to, '9999-12-31'::TIMESTAMP)
        
    -- 3. Join to dim_neighbourhood (NO SCD2 - this dimension doesn't track history)
    JOIN {{ ref('dim_neighbourhood') }} sub
        ON MD5(CAST(COALESCE(CAST(stg.listing_neighbourhood AS VARCHAR), '') || '-' || COALESCE(CAST(stg.room_type AS VARCHAR), '') AS VARCHAR)) = sub.neighbourhood_unique_key

    WHERE stg.listing_id IS NOT NULL
)

SELECT * FROM final_fact

{% if is_incremental() %}
WHERE snapshot_month > (SELECT MAX(snapshot_month) FROM {{ this }})
{% endif %}