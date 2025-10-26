{{
    config(
        materialized='table',
        schema='dwh_gold'
    )
}}

WITH neighbourhoods AS (
    SELECT
        neighbourhood_unique_key,
        neighbourhood_name,
        room_type,
        COALESCE(property_type, 'Unknown') AS property_type,
        latest_listing_date
    FROM {{ ref('silver_neighbourhoods') }}
)

SELECT
    n.neighbourhood_unique_key,
    n.neighbourhood_name,
    n.room_type,
    n.property_type,
    CAST(NULL AS VARCHAR) AS lga_code,
    CAST(NULL AS VARCHAR) AS lga_name,
    n.latest_listing_date
FROM neighbourhoods AS n