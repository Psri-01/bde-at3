{{ config(tags=['silver','mapping']) }}

-- Map listing_neighbourhood (free text suburb) → LGA via suburb map.
with nb as (
  select distinct lower(listing_neighbourhood) as suburb_name_lower
  from {{ ref('stg_listings') }}
  where listing_neighbourhood is not null
)
select
  nb.suburb_name_lower,
  m.lga_name
from nb
left join {{ ref('stg_lga_suburb') }} m
  on nb.suburb_name_lower = m.suburb_name_lower