{{ config(tags=['staging','census']) }}

select
  trim(lga_code_2016::text)                         as lga_code,
  nullif(trim(median_age_persons::text),'')::numeric(6,2)     as median_age_persons,
  nullif(trim(median_mortgage_repay_monthly::text),'')::numeric(12,2)
      as median_mortgage_monthly,
  nullif(trim(average_household_size::text),'')::numeric(6,3)
      as average_household_size,
  nullif(trim(median_tot_prsnl_inc_weekly::text),'')::numeric(12,2)
      as median_personal_income_weekly,
  nullif(trim(median_tot_fam_inc_weekly::text),'')::numeric(12,2)
      as median_family_income_weekly,
  nullif(trim(median_rent_weekly::text),'')::numeric(12,2)
      as median_rent_weekly,
  nullif(trim(median_tot_hhd_inc_weekly::text),'')::numeric(12,2)
      as median_household_income_weekly,
  nullif(trim(average_num_psns_per_bedroom::text),'')::numeric(6,3)
      as avg_persons_per_bedroom,
  source_file, snapshot_month, ingested_at
from {{ source('bronze','census_g02_raw') }}
where lga_code_2016 is not null