{{ config(materialized='table', schema='gold', tags=['dimension']) }}

SELECT 
    neighbourhood_unique_key,
    neighbourhood_name,
    room_type,
    property_type,
    accommodates,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('suburb_snapshot') }}