{{ config(tags=['staging','mapping']) }}

select
  trim(lga_code::text) as lga_code,
  trim(lga_name::text) as lga_name,
  ingested_at
from {{ source('bronze','nsw_lga_code_raw') }}
where lga_code is not null