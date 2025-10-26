{{ config(tags=['gold','dim']) }}
SELECT
    h.host_id,
    h.host_name,
    h.host_since,
    h.host_location,
    CASE
        WHEN LOWER(h.host_location) LIKE '%sydney%' THEN 'Sydney'
        WHEN LOWER(h.host_location) LIKE '%nsw%' THEN 'NSW'
        ELSE 'Other'
    END AS host_region
FROM {{ ref('host_entity') }} AS h
WHERE h.host_id IS NOT NULL