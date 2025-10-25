{% snapshot host_snapshot %}

{{
    config(
        target_schema='silver',
        unique_key='host_id',
        strategy='timestamp',
        updated_at='record_timestamp',
        invalidate_hard_deletes=True
    )
}}

WITH normalized AS (
    SELECT
        host_id,
        host_name,
        host_location,
        CURRENT_TIMESTAMP AS record_timestamp
    FROM {{ source('bronze', 'airbnb_hosts_raw') }}
)

SELECT
    host_id,
    host_name,
    host_location,
    record_timestamp
FROM normalized

{% endsnapshot %}
