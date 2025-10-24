{% snapshot suburb_snapshot %}

{{
    config(
      target_schema='silver',
      unique_key='neighbourhood_unique_key', 
      strategy='timestamp',
      updated_at='listing_date',
      invalidate_hard_deletes=True
    )
}}

SELECT
    -- Create a unique key based on the identifying dimension attributes
    MD5(CAST(COALESCE(CAST(listing_neighbourhood AS VARCHAR), '') || '-' || COALESCE(CAST(room_type AS VARCHAR), '') AS VARCHAR)) AS neighbourhood_unique_key,
    listing_neighbourhood AS neighbourhood_name,
    room_type,
    property_type,
    accommodates,
    CAST(date_trunc('month', scraped_date) AS TIMESTAMP) AS listing_date -- Changed scrapeddate to scraped_date
FROM {{ source('bronze', 'airbnb_listings_raw') }}
WHERE listing_neighbourhood IS NOT NULL

{% endsnapshot %}