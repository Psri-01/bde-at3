{{ config(tags=['gold','dim']) }}

with dom as (
  select distinct
    nullif(trim(property_type),'')   as property_type,
    nullif(trim(room_type),'')       as room_type,
    accommodates
  from {{ ref('stg_listings') }}
  where property_type is not null
    and room_type     is not null
    and accommodates  is not null
)
select
  md5(
    coalesce(property_type,'') || '|' ||
    coalesce(room_type,'')     || '|' ||
    accommodates::text
  )                                as property_sk,
  property_type,
  room_type,
  accommodates
from dom
