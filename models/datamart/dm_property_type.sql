{{ config(materialized='view', schema='datamart', tags=['datamart']) }}

WITH base AS (
    SELECT
        f.property_type,
        f.room_type,
        f.accommodates,
        DATE_TRUNC('month', f.period_month) AS month,
        COUNT(*) AS total_listings,
        SUM(f.is_active) AS active_listings,
        COUNT(DISTINCT f.host_id) AS distinct_hosts,
        SUM(CASE WHEN d.host_is_superhost THEN 1 ELSE 0 END) AS superhosts,
        AVG(f.review_scores_rating) AS avg_rating,
        SUM(f.number_of_stays) AS total_stays,
        AVG(f.estimated_revenue) AS avg_estimated_revenue,
        MIN(f.price) AS min_price,
        MAX(f.price) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.price) AS median_price,
        AVG(f.price) AS avg_price
    FROM {{ source('silver_gold', 'fact_airbnb_revenue') }} f
    JOIN {{ ref('dim_host') }} d ON f.host_id = d.host_id
    GROUP BY 1, 2, 3, 4
)

SELECT
    *,
    ROUND((active_listings::NUMERIC / total_listings::NUMERIC) * 100, 2) AS active_listing_rate,
    ROUND((superhosts::NUMERIC / distinct_hosts::NUMERIC) * 100, 2) AS superhost_rate
FROM base
ORDER BY property_type, room_type, accommodates, month
