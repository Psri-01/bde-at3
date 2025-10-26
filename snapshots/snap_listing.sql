{% snapshot snap_listing %}
{{
    config(
        target_schema='silver_snap',
        unique_key='listing_id',
        strategy='timestamp',
        updated_at='scraped_date',
        invalidate_hard_deletes=False
    )
}}

WITH limited_source AS (
    SELECT
        CAST(listing_id AS TEXT) AS listing_id,
        CAST(scrape_id AS TEXT) AS scrape_id,
        scraped_date::timestamp AS scraped_date,
        CAST(host_id AS TEXT) AS host_id,
        listing_neighbourhood::text AS listing_neighbourhood,
        property_type::text AS property_type,
        room_type::text AS room_type,
        accommodates::int AS accommodates,
        price::numeric AS price,
        has_availability::text AS has_availability,
        number_of_reviews::int AS number_of_reviews,
        review_scores_rating::numeric AS review_scores_rating,
        snapshot_month::date AS snapshot_month
    FROM {{ source('bronze', 'airbnb_listings_raw') }}
    WHERE snapshot_month = '{{ var("snapshot_month") }}'::date
      AND scraped_date IS NOT NULL
      AND listing_id IS NOT NULL
      AND price IS NOT NULL
      AND listing_neighbourhood IS NOT NULL
    ORDER BY scraped_date DESC, listing_id ASC
    LIMIT 5000
)

SELECT * FROM limited_source
{% endsnapshot %}
