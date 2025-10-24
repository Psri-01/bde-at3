{% snapshot host_snapshot %}

{{
    config(
      target_schema='silver',
      unique_key='host_id',
      strategy='timestamp',
      updated_at='listing_date',
      invalidate_hard_deletes=True
    )
}}

WITH source_data AS (
    SELECT
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        scraped_date,
        -- Calculate the monthly timestamp for SCD2 tracking
        CAST(date_trunc('month', scraped_date) AS TIMESTAMP) AS listing_date,
        
        -- Deduplicate logic remains the same
        ROW_NUMBER() OVER (
            PARTITION BY host_id, CAST(date_trunc('month', scraped_date) AS TIMESTAMP)
            ORDER BY scraped_date DESC
        ) AS rn
        
    FROM {{ source('bronze', 'airbnb_listings_raw') }}
)

SELECT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    listing_date
FROM source_data
-- Filter for the most recent record of the host within that month
WHERE rn = 1

{% endsnapshot %}