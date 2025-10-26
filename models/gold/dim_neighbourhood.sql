{{ config(tags=['gold','dim']) }}

with dom as (
  select distinct listing_neighbourhood as neighbourhood_name
  from {{ ref('stg_listings') }}
  where listing_neighbourhood is not null
)
select
  d.neighbourhood_name,
  n.lga_name
from dom d
left join {{ ref('nbhd_to_lga') }} m on lower(d.neighbourhood_name) = m.suburb_name_lower
left join {{ ref('stg_lga_code') }} n on m.lga_name = n.lga_name