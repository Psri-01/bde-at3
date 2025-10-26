{{ config(tags=['staging']) }}

select
  listing_id::bigint,
  scrape_id::bigint,
  scraped_date::timestamp,
  host_id::bigint,
  nullif(trim(host_name),'')                       as host_name,
  case
      when nullif(trim(host_since::text),'') is not null
      then to_date(trim(host_since::text),'YYYY-MM-DD')
  end                                              as host_since,
  (host_is_superhost = 't')                        as host_is_superhost,
  nullif(trim(host_neighbourhood),'')              as host_neighbourhood,
  nullif(trim(listing_neighbourhood),'')           as listing_neighbourhood,
  nullif(trim(property_type),'')                   as property_type,
  nullif(trim(room_type),'')                       as room_type,
  coalesce(accommodates::int, 0)                   as accommodates,
  coalesce(nullif(regexp_replace(price::text, '[^0-9\.]', '', 'g'),'')::numeric(12,2), 0) as price,
  (has_availability = 't')                         as has_availability,
  nullif(trim(availability_30::text),'')::int      as availability_30,
  nullif(trim(number_of_reviews::text),'')::int    as number_of_reviews,
  nullif(trim(review_scores_rating::text),'')::numeric(5,2) as review_scores_rating,
  snapshot_month::date                             as snapshot_month,
  source_file, ingested_at
from {{ source('bronze','airbnb_listings_raw') }}
where listing_id is not null
