{{ config(materialized='table', schema='gold', tags=['dimension']) }}

SELECT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('host_snapshot') }}