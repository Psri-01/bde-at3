{{ config(tags=['gold','fact']) }}

with base as (
  select
    listing_id,
    host_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    coalesce(review_scores_rating, 0) as review_scores_rating,
    snapshot_month::date               as snapshot_month
  from {{ ref('stg_listings') }}
),
with_metrics as (
  select
    *,
    case when has_availability
         then greatest(0, 30 - coalesce(availability_30,0))
         else 0 end                                  as stays,
    case when has_availability
         then greatest(0, 30 - coalesce(availability_30,0)) * coalesce(price,0)
         else 0 end                                  as estimated_revenue
  from base
),
with_property_sk as (
  select
    f.*,
    p.property_sk
  from with_metrics f
  left join {{ ref('dim_property') }} p
    on  p.property_type = f.property_type
    and p.room_type     = f.room_type
    and p.accommodates  = f.accommodates
)
select
  -- natural keys / references
  listing_id,
  host_id,
  listing_neighbourhood,

  -- property surrogate key (new)
  property_sk,

  -- keep these for existing DMs (don’t break downstream)
  property_type,
  room_type,
  accommodates,

  -- metrics
  price,
  number_of_reviews,
  has_availability,
  availability_30,
  review_scores_rating,
  stays,
  estimated_revenue,

  -- time
  snapshot_month
from with_property_sk
