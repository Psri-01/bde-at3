{% snapshot snap_lga %}
{{
    config(
        target_schema='silver_snap',
        unique_key='lga_code',
        strategy='timestamp',
        updated_at='snapshot_month',
        invalidate_hard_deletes=True
    )
}}

SELECT
    CAST(lga_code AS TEXT) AS lga_code,
    lga_name,
    CAST(snapshot_month AS TIMESTAMP) AS snapshot_month
FROM {{ source('bronze', 'nsw_lga_code_raw') }}
WHERE snapshot_month = '{{ var("snapshot_month") }}'::date

{% endsnapshot %}
