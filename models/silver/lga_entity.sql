{{ config(tags=['silver','entity']) }}
select
  c.lga_code,
  c.lga_name,
  -- use a stable snapshot date (e.g., 2016 census date) if null
  coalesce(min(s.snapshot_month), date '2016-08-09') as snapshot_month
from {{ ref('stg_lga_code') }} c
left join {{ ref('stg_lga_suburb') }} s on c.lga_name = s.lga_name
group by c.lga_code, c.lga_name