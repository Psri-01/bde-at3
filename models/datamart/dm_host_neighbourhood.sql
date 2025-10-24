{{ config(materialized='view', schema='datamart', tags=['datamart']) }}

-- Note: The logic below assumes dim_lga contains the LGA name, and the fact table's
-- listing_neighbourhood can be joined to it. This structural join is complex 
-- and may need refinement during testing.

WITH base AS (
    SELECT
        -- Assuming dim_lga contains the LGA name needed for grouping
        COALESCE(l.lga_name, f.listing_neighbourhood) AS host_neighbourhood_lga,
        DATE_TRUNC('month', f.listing_date) AS month,
        
        COUNT(DISTINCT f.host_fk) AS distinct_hosts, -- Use host_fk from fact
        SUM(f.estimated_revenue) AS total_estimated_revenue,
        
        -- FIX: Use ROUND correctly on the ratio
        ROUND(SUM(f.estimated_revenue) / COUNT(DISTINCT f.host_fk), 2) AS est_revenue_per_host
        
    FROM {{ ref('fact_airbnb_revenue') }} f
    
    -- NOTE: SCD2 join to dim_lga is not needed here unless LGA name/code changes,
    -- but it's required for the datamart to link correctly.
    LEFT JOIN {{ ref('dim_lga') }} l
        ON f.lga_fk = l.lga_code -- Assuming lga_fk holds lga_code
        AND f.listing_date BETWEEN l.dbt_valid_from AND COALESCE(l.dbt_valid_to, '9999-12-31'::TIMESTAMP)

    GROUP BY 1, 2
)
SELECT * FROM base
ORDER BY host_neighbourhood_lga, month