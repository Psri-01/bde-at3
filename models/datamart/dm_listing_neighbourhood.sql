{{ config(materialized='table') }}

WITH listing_neighbourhood AS (
    SELECT
        listing_neighbourhood,
        COUNT(DISTINCT listing_id) AS active_listings,
        SUM(estimated_revenue) AS total_estimated_revenue
    FROM {{ ref('fact_listing_monthly') }}
    GROUP BY 1
)

SELECT
    listing_neighbourhood,
    active_listings,
    total_estimated_revenue,
    CASE WHEN active_listings > 0 THEN total_estimated_revenue / active_listings ELSE 0 END AS avg_revenue_per_listing
FROM listing_neighbourhood
ORDER BY total_estimated_revenue DESC