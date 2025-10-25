{{
    config(
        materialized='table',
        schema='dwh_gold'
    )
}}

SELECT
    sn.neighbourhood_unique_key,
    sn.neighbourhood_name,
    sn.room_type,
    sn.property_type,
    lg.lga_code,
    lg.lga_name,
    sn.latest_listing_date
FROM {{ ref('silver_neighbourhoods') }} AS sn
LEFT JOIN {{ ref('lga_snapshot') }} AS lg
    ON LOWER(sn.neighbourhood_name) = LOWER(lg.suburb_name)
