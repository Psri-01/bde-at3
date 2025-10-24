{% snapshot lga_snapshot %}

{{
    config(
      target_schema='silver',
      unique_key='lga_code',
      strategy='timestamp',
      updated_at='listing_date',
      invalidate_hard_deletes=True
    )
}}

-- 1) Suburb -> LGA (name) -> LGA code
with suburb_lga as (
  select
      c.lga_code::varchar as lga_code,
      c.lga_name,
      s.suburb_name
  from {{ source('bronze','nsw_lga_suburb_raw') }} s
  join {{ source('bronze','nsw_lga_code_raw') }}  c
    on upper(trim(s.lga_name)) = upper(trim(c.lga_name))
),

-- 2) Link Airbnb listing_neighbourhood to suburb, create monthly timestamp
lga_context as (
  select distinct
      sl.lga_code,
      sl.lga_name,
      cast(date_trunc('month', a.scraped_date) as timestamp) as listing_date
  from suburb_lga sl
  join {{ source('bronze','airbnb_listings_raw') }} a
    on upper(trim(a.listing_neighbourhood)) = upper(trim(sl.suburb_name))
)

select
  lga_code,
  lga_name,
  listing_date
from lga_context
where lga_code is not null

{% endsnapshot %}