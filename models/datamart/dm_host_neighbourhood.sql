{{ config(materialized='table') }}

WITH host_summary AS (
    SELECT
        host_id,
        SUM(estimated_revenue) AS total_estimated_revenue
    FROM {{ ref('fact_listing_monthly') }}
    GROUP BY 1
)

SELECT
    COUNT(DISTINCT host_id) AS total_hosts,
    SUM(total_estimated_revenue) AS total_revenue,
    CASE WHEN COUNT(DISTINCT host_id) > 0 THEN SUM(total_estimated_revenue) / COUNT(DISTINCT host_id) ELSE 0 END AS avg_revenue_per_host
FROM host_summary