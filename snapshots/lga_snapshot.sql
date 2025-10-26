{% snapshot lga_snapshot %}
{{
    config(
        target_schema='silver',
        unique_key='lga_code',
        strategy='timestamp',
        updated_at='load_date',
        invalidate_hard_deletes=True
    )
}}

SELECT
    CAST(lga_code AS TEXT) AS lga_code,
    TRIM(lga_name) AS lga_name,
    DATE_TRUNC('month', CURRENT_TIMESTAMP)::date AS snapshot_month,
    CURRENT_TIMESTAMP::timestamp AS load_date
FROM {{ source('bronze', 'nsw_lga_code_raw') }}
WHERE lga_code IS NOT NULL

{% endsnapshot %}
