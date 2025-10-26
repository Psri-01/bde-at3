{{ config(materialized='view') }}

WITH property_summary AS (
    SELECT
        property_sk,
        property_type,
        COUNT(*) AS total_listings,
        SUM(estimated_revenue) AS total_revenue
    FROM {{ ref('fact_listing_monthly') }}
    GROUP BY 1,2
)

SELECT
    property_sk,
    property_type,
    total_listings,
    total_revenue,
    CASE WHEN total_listings > 0 THEN total_revenue / total_listings ELSE 0 END AS avg_revenue_per_property
FROM property_summary
ORDER BY total_revenue DESC