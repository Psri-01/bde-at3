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

        -- Clean price column before use
        CASE
    WHEN stg.price IS NULL THEN 0
    WHEN stg.price::TEXT ~ '^[0-9\.\$]+$'
         THEN CAST(REPLACE(stg.price::TEXT, '$', '') AS DOUBLE PRECISION)
    ELSE 0
END AS price_clean,

        stg.has_availability,
        stg.number_of_reviews,
        stg.review_scores_rating,
        
        -- Foreign Key Aliases (FKs)
        host.host_id AS host_fk,
        lga.lga_code AS lga_fk,
        sub.neighbourhood_unique_key AS neighbourhood_fk,

        -- Use cleaned numeric price in calculations
        (30 - stg.availability_30) AS number_of_stays,
        CASE 
    WHEN stg.has_availability = TRUE 
    THEN (30 - stg.availability_30) *
         CASE
             WHEN stg.price::TEXT ~ '^[0-9\.\$]+$'
             THEN CAST(REPLACE(stg.price::TEXT, '$', '') AS DOUBLE PRECISION)
             ELSE 0
         END
    ELSE 0 
END AS estimated_revenue,
        
        stg.snapshot_month
        
    FROM {{ ref('stg_airbnb_listings') }} stg
    
    LEFT JOIN lga_lookup lkg
        ON UPPER(TRIM(stg.listing_neighbourhood)) = UPPER(TRIM(lkg.suburb_name))

    JOIN {{ ref('dim_host') }} host
      ON stg.host_id = host.host_id
     AND stg.scraped_date BETWEEN host.dbt_valid_from AND COALESCE(host.dbt_valid_to, '9999-12-31'::TIMESTAMP)

    JOIN {{ ref('dim_lga') }} lga
      ON lkg.lga_code = lga.lga_code 
     AND stg.scraped_date BETWEEN lga.dbt_valid_from AND COALESCE(lga.dbt_valid_to, '9999-12-31'::TIMESTAMP)

    JOIN {{ ref('dim_neighbourhood') }} sub
      ON MD5(CAST(COALESCE(CAST(stg.listing_neighbourhood AS VARCHAR), '') || '-' || COALESCE(CAST(stg.room_type AS VARCHAR), '') AS VARCHAR)) = sub.neighbourhood_unique_key
     AND stg.scraped_date BETWEEN sub.dbt_valid_from AND COALESCE(sub.dbt_valid_to, '9999-12-31'::TIMESTAMP)
     
    WHERE stg.listing_id IS NOT NULL
)

{% if is_incremental() %}
    WHERE snapshot_month > (SELECT MAX(snapshot_month) FROM {{ this }})
{% endif %}

SELECT * FROM final_fact
