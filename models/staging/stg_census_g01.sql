{{
    config(
        materialized='table',
        schema='silver',
        tags=['staging', 'census']
    )
}}

SELECT
    lga_code_2016::varchar         AS lga_code,
    tot_p_p::integer               AS total_persons,

    -- Age bands (population distribution)
    age_0_4_yr_p::integer          AS age_0_4,
    age_5_14_yr_p::integer         AS age_5_14,
    age_15_19_yr_p::integer        AS age_15_19,
    age_20_24_yr_p::integer        AS age_20_24,
    age_25_34_yr_p::integer        AS age_25_34,
    age_35_44_yr_p::integer        AS age_35_44,
    age_45_54_yr_p::integer        AS age_45_54,
    age_55_64_yr_p::integer        AS age_55_64,
    age_65_74_yr_p::integer        AS age_65_74,
    age_75_84_yr_p::integer        AS age_75_84,
    age_85ov_p::integer            AS age_85_plus,

    count_psns_occ_priv_dwgs_p::integer AS occupied_private_dwellings,

    source_file,
    snapshot_month,
    ingested_at
FROM {{ source('bronze', 'census_g01_raw') }}
WHERE lga_code_2016 IS NOT NULL