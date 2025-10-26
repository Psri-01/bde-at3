{{ config(materialized='view', schema='datamart') }}

SELECT
    f.host_id,
    h.host_name,
    h.host_is_superhost,
    n.neighbourhood_name,
    l.lga_name,
    COUNT(DISTINCT f.listing_id) AS total_listings,
    ROUND(AVG(f.price), 2) AS avg_price,
    ROUND(AVG(f.number_of_reviews), 2) AS avg_reviews,
    ROUND(AVG(f.availability_30), 2) AS avg_availability,
    f.snapshot_month
FROM {{ ref('fact_airbnb_revenue') }} f
LEFT JOIN {{ ref('dim_host') }} h
    ON f.host_id = h.host_id
LEFT JOIN {{ ref('dim_neighbourhood') }} n
    ON f.neighbourhood_name = n.neighbourhood_name
LEFT JOIN {{ ref('dim_lga') }} l
    ON f.lga_name = l.lga_name
GROUP BY 1, 2, 3, 4, 5, 10