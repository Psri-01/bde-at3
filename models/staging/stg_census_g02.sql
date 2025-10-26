{{
    config(
        materialized='table',
        schema='silver',
        tags=['staging', 'census']
    )
}}

SELECT
    lga_code_2016::varchar  AS lga_code,
    median_age_persons::integer              AS median_age_persons,
    median_mortgage_repay_monthly::numeric   AS median_mortgage_monthly,
    average_household_size::numeric          AS average_household_size,
    median_tot_prsnl_inc_weekly::numeric     AS median_personal_income_weekly,
    median_tot_fam_inc_weekly::numeric       AS median_family_income_weekly,
    median_rent_weekly::numeric              AS median_rent_weekly,
    median_tot_hhd_inc_weekly::numeric       AS median_household_income_weekly,
    average_num_psns_per_bedroom::numeric    AS avg_persons_per_bedroom,
    source_file,
    snapshot_month,
    ingested_at
FROM {{ source('bronze', 'census_g02_raw') }}
WHERE lga_code_2016 IS NOT NULL