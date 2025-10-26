{% snapshot host_snapshot %}
{{
    config(
        target_schema='silver',
        unique_key='host_id',
        strategy='timestamp',
        updated_at='load_date',
        invalidate_hard_deletes=True
    )
}}

SELECT
    host_id,
    host_name,
    host_since,
    FALSE AS host_is_superhost,        -- Default boolean column
    NULL AS total_listings_count,      -- Placeholder (avoids missing column errors)
    DATE_TRUNC('month', CURRENT_TIMESTAMP)::date AS snapshot_month,
    CURRENT_TIMESTAMP::timestamp AS load_date
FROM {{ source('bronze', 'airbnb_hosts_raw') }}
WHERE host_id IS NOT NULL

{% endsnapshot %}
