{{ config(materialized='view', schema='silver_datamart') }}

SELECT
    f.room_type,
    h.host_id,
    h.host_name,
    l.lga_name,
    COUNT(DISTINCT f.listing_id) AS total_listings,
    ROUND(AVG(f.price), 2) AS avg_price,
    ROUND(AVG(f.number_of_reviews), 2) AS avg_reviews,
    ROUND(AVG(f.availability_30), 2) AS avg_availability,
    f.snapshot_month
FROM {{ ref('fact_airbnb_revenue') }} f
LEFT JOIN {{ ref('dim_host') }} h
    ON f.host_id = h.host_id
LEFT JOIN {{ ref('dim_lga') }} l
    ON f.lga_name = l.lga_name
GROUP BY 1, 2, 3, 4, 9