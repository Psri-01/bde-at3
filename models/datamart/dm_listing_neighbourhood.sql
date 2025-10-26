{{ config(materialized='view', schema='silver_datamart') }}

SELECT
    f.listing_id,
    f.host_id,
    d.host_name,
    f.neighbourhood_name,
    f.lga_name,
    f.room_type,
    f.price,
    f.number_of_reviews,
    f.availability_30,
    f.snapshot_month
FROM {{ ref('fact_airbnb_revenue') }} f
LEFT JOIN {{ ref('dim_host') }} d
    ON f.host_id = d.host_id
LEFT JOIN {{ ref('dim_neighbourhood') }} n
    ON f.neighbourhood_name = n.neighbourhood_name
LEFT JOIN {{ ref('dim_lga') }} l
    ON f.lga_name = l.lga_name