WITH ranked_hosts AS (
    SELECT
        host_id,
        host_name,
        host_since,
        COALESCE(host_location, 'Unknown') AS host_location,
        CURRENT_DATE AS record_date,  -- synthetic fallback
        ROW_NUMBER() OVER (
            PARTITION BY host_id
            ORDER BY host_since DESC NULLS LAST
        ) AS rn
    FROM {{ source('bronze', 'airbnb_hosts_raw') }}
    WHERE host_id IS NOT NULL
)

SELECT
    host_id,
    host_name,
    host_since,
    host_location,
    record_date
FROM ranked_hosts
WHERE rn = 1
