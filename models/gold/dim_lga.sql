{{ config(tags=['gold','dim']) }}
select
  lga_code,
  lga_name,
  dbt_valid_from::date as valid_from,
  dbt_valid_to::date   as valid_to
from {{ ref('snap_lga') }}