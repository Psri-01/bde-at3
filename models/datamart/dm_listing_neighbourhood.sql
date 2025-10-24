{{ config(materialized='view', schema='datamart', tags=['datamart']) }}

WITH base AS (
    SELECT
        sub.neighbourhood_name AS listing_neighbourhood,
        DATE_TRUNC('month', f.listing_date::TIMESTAMP) AS month_year, -- FIX: Use listing_date and explicit TIMESTAMP cast
        
        COUNT(f.listing_id) AS total_listings,
        SUM(CASE WHEN f.has_availability = TRUE THEN 1 ELSE 0 END) AS active_listings,
        
        -- FIX 1: Use the correct column name from dim_host (host_is_superhost)
        SUM(CASE WHEN d.host_is_superhost = TRUE THEN 1 ELSE 0 END) AS superhosts_count,
        COUNT(DISTINCT f.host_fk) AS distinct_hosts,
        
        AVG(f.review_scores_rating) AS avg_rating,
        SUM(f.number_of_stays) AS total_stays,
        
        AVG(CASE WHEN f.has_availability = TRUE THEN f.estimated_revenue END) AS avg_estimated_revenue,
        MIN(f.price) AS min_price,
        MAX(f.price) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.price) AS median_price,
        AVG(f.price) AS avg_price
        
    FROM {{ ref('fact_airbnb_revenue') }} f
    
    -- FIX: Ensure joins are correct (using listing_date from FACT table)
    JOIN {{ ref('dim_host') }} d 
        ON f.host_fk = d.host_id
       AND f.listing_date BETWEEN d.dbt_valid_from AND COALESCE(d.dbt_valid_to, '9999-12-31'::TIMESTAMP)
       
    JOIN {{ ref('dim_neighbourhood') }} sub
        ON f.neighbourhood_fk = sub.neighbourhood_unique_key
       AND f.listing_date BETWEEN sub.dbt_valid_from AND COALESCE(sub.dbt_valid_to, '9999-12-31'::TIMESTAMP)

    GROUP BY 1, 2
)

SELECT
    *,
    ROUND((active_listings::NUMERIC / total_listings::NUMERIC) * 100, 2) AS active_listing_rate,
    ROUND((superhosts_count::NUMERIC / distinct_hosts::NUMERIC) * 100, 2) AS superhost_rate
    -- NOTE: Percentage change must be calculated here using LAG() or similar window functions
    
FROM base
ORDER BY listing_neighbourhood, month