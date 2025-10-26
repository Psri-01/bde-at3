{{ config(tags=['staging','census']) }}

select
  trim(lga_code_2016::text)                         as lga_code,
  nullif(trim(tot_p_p::text),'')::numeric(12,0)           as total_persons,
  nullif(trim(age_0_4_yr_p::text),'')::numeric(12,0)      as age_0_4,
  nullif(trim(age_5_14_yr_p::text),'')::numeric(12,0)     as age_5_14,
  nullif(trim(age_15_19_yr_p::text),'')::numeric(12,0)    as age_15_19,
  nullif(trim(age_20_24_yr_p::text),'')::numeric(12,0)    as age_20_24,
  nullif(trim(age_25_34_yr_p::text),'')::numeric(12,0)    as age_25_34,
  nullif(trim(age_35_44_yr_p::text),'')::numeric(12,0)    as age_35_44,
  nullif(trim(age_45_54_yr_p::text),'')::numeric(12,0)    as age_45_54,
  nullif(trim(age_55_64_yr_p::text),'')::numeric(12,0)    as age_55_64,
  nullif(trim(age_65_74_yr_p::text),'')::numeric(12,0)    as age_65_74,
  nullif(trim(age_75_84_yr_p::text),'')::numeric(12,0)    as age_75_84,
  nullif(trim(age_85ov_p::text),'')::numeric(12,0)        as age_85_plus,
  nullif(trim(count_psns_occ_priv_dwgs_p::text),'')::numeric(12,0)
      as occupied_private_dwellings,
  source_file, snapshot_month, ingested_at
from {{ source('bronze','census_g01_raw') }}
where lga_code_2016 is not null