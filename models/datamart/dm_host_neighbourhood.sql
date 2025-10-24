{{ config(materialized='view', schema='datamart', tags=['datamart']) }}

WITH base AS (
    SELECT
        COALESCE(l.lga_name, f.listing_neighbourhood) AS host_neighbourhood_lga,
        DATE_TRUNC('month', f.period_month) AS month,
        COUNT(DISTINCT f.host_id) AS distinct_hosts,
        SUM(f.estimated_revenue) AS total_estimated_revenue,
        ROUND(SUM(f.estimated_revenue) / COUNT(DISTINCT f.host_id), 2) AS est_revenue_per_host
    FROM {{ ref('fact_airbnb_revenue') }} f
    LEFT JOIN {{ ref('dim_lga') }} l
      ON f.listing_neighbourhood = l.lga_name
    GROUP BY 1, 2
)
SELECT * FROM base
ORDER BY host_neighbourhood_lga, month;
