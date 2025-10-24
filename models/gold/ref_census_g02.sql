{{ config(materialized='table', schema='gold', tags=['reference']) }}

SELECT *
FROM {{ ref('stg_census_g02') }}