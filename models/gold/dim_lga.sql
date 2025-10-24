{{ config(materialized='table', schema='gold', tags=['dimension']) }}

SELECT 
    lga_code,
    lga_name,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('lga_snapshot') }}