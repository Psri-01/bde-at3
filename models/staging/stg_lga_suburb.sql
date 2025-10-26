{{ config(tags=['staging','mapping']) }}
select
  lower(trim(suburb_name))::varchar  as suburb_name_lower,
  trim(lga_name)::varchar            as lga_name,
  ingested_at, source_file, snapshot_month
from {{ source('bronze','nsw_lga_suburb_raw') }}
where suburb_name is not null and lga_name is not null
