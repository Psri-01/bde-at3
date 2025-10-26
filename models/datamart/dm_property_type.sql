{{ config(materialized='view', schema='datamart', tags=['datamart']) }}

WITH base AS (
    SELECT
        f.listing_id,
        f.snapshot_month,
        h.host_id,
        h.host_is_superhost,
        f.price,
        f.number_of_stays,
        f.estimated_revenue,
        f.review_scores_rating,
        f.has_availability,
        n.neighbourhood_name,
        DATE_TRUNC('month', f.snapshot_month::TIMESTAMP) AS month_year
        
    FROM {{ ref('fact_airbnb_revenue') }} f
    
    -- Join to dim_host (SCD2 Logic)
    JOIN {{ ref('dim_host') }} h
        ON f.host_fk = h.host_id
       AND f.snapshot_month BETWEEN h.dbt_valid_from AND COALESCE(h.dbt_valid_to, '9999-12-31'::TIMESTAMP)
       
    -- Join to dim_neighbourhood (NO SCD2)
    JOIN {{ ref('dim_neighbourhood') }} n
        ON f.neighbourhood_fk = n.neighbourhood_unique_key
)

SELECT
    neighbourhood_name,
    DATE_TRUNC('month', month_year) AS month_year,
    COUNT(DISTINCT listing_id) AS total_listings,
    COUNT(DISTINCT host_id) AS distinct_hosts,
    SUM(CASE WHEN has_availability THEN 1 ELSE 0 END) AS active_listings,
    SUM(CASE WHEN host_is_superhost THEN 1 ELSE 0 END) AS superhosts_count,
    ROUND(AVG(review_scores_rating)::numeric, 2) AS avg_rating,
    ROUND(AVG(estimated_revenue)::numeric, 2) AS avg_estimated_revenue,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::numeric, 2) AS median_price
    
FROM base
GROUP BY 1, 2
ORDER BY 1, 2