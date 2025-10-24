{{ config(materialized='view', schema='datamart', tags=['datamart']) }}

WITH base AS (
    SELECT
        COALESCE(l.lga_name, sub.neighbourhood_name) AS host_neighbourhood_lga,
        DATE_TRUNC('month', f.listing_date::TIMESTAMP) AS month_year, -- FIX: Use listing_date and explicit TIMESTAMP cast
        
        COUNT(DISTINCT f.host_fk) AS distinct_hosts,
        SUM(f.estimated_revenue) AS total_estimated_revenue,
        
        -- FIX: Use ROUND correctly on the ratio
        ROUND(SUM(f.estimated_revenue) / COUNT(DISTINCT f.host_fk), 2) AS est_revenue_per_host
        
    FROM {{ ref('fact_airbnb_revenue') }} f
    
    -- Join to dim_lga for LGA name/grouping (SCD2 Logic)
    LEFT JOIN {{ ref('dim_lga') }} l
        ON f.lga_fk = l.lga_code
        AND f.listing_date BETWEEN l.dbt_valid_from AND COALESCE(l.dbt_valid_to, '9999-12-31'::TIMESTAMP)
        
    -- Join to dim_neighbourhood to get the name if LGA is missing
    JOIN {{ ref('dim_neighbourhood') }} sub
        ON f.neighbourhood_fk = sub.neighbourhood_unique_key
        AND f.listing_date BETWEEN sub.dbt_valid_from AND COALESCE(sub.dbt_valid_to, '9999-12-31'::TIMESTAMP)

    GROUP BY 1, 2
)
SELECT * FROM base
ORDER BY host_neighbourhood_lga, month_year