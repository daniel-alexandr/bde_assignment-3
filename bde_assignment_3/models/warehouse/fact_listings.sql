{{
    config(
        unique_key=['listing_id','scraped_date']
    )
}}

with host as
(
    select * from {{ ref('dim_host') }}
),
neighbourhood as 
(
    select * from {{ ref('dim_neighbourhood') }}
),
property as 
(
    select * from {{ ref('dim_property') }}
),
room as 
(
    select * from {{ ref('dim_room') }}
),
suburb_lga as 
(
    select * from {{ ref('dim_lga_suburb') }}
),
census as
(
    select * from {{ ref('dim_census') }}
),
facts as 
(
    select * from {{ ref('listings_stg') }}
)

select  
facts.*,

host.host_name,
host.host_since,
host.host_is_superhost,
host.dbt_valid_from,
host.dbt_valid_to,

property.property_type,
property.accomodates,

room.room_type,

neighbourhood.listing_neighbourhood,
neighbourhood.host_neighbourhood,

suburb_lga_host.lga_name as host_lga,
suburb_lga_host.lga_code as host_lga_code,
suburb_lga_listing.lga_name as listing_lga,
suburb_lga_listing.lga_code as listing_lga_code,


census_host.tot_p_p AS tot_p_p_host,
census_host.tot_p_m AS tot_p_m_host,
census_host.tot_p_f AS tot_p_f_host,
census_host.median_tot_prsnl_inc_weekly AS median_tot_prsnl_inc_weekly_host,
census_host.median_tot_hhd_inc_weekly AS median_tot_hhd_inc_weekly_host,
census_host.median_tot_fam_inc_weekly AS median_tot_fam_inc_weekly_host,
census_host.median_rent_weekly AS median_rent_weekly_host,
census_host.median_mortgage_repay_monthly AS median_mortgage_repay_monthly_host,
census_host.median_age_persons AS median_age_persons_host,
census_host.lga_code AS lga_code_host,
census_host.lang_spoken_home_oth_lang_p AS lang_spoken_home_oth_lang_p_host,
census_host.lang_spoken_home_oth_lang_m AS lang_spoken_home_oth_lang_m_host,
census_host.lang_spoken_home_oth_lang_f AS lang_spoken_home_oth_lang_f_host,
census_host.lang_spoken_home_eng_only_p AS lang_spoken_home_eng_only_p_host,
census_host.lang_spoken_home_eng_only_m AS lang_spoken_home_eng_only_m_host,
census_host.lang_spoken_home_eng_only_f AS lang_spoken_home_eng_only_f_host,
census_host.indigenous_psns_aboriginal_p AS indigenous_psns_aboriginal_p_host,
census_host.indigenous_psns_aboriginal_m AS indigenous_psns_aboriginal_m_host,
census_host.indigenous_psns_aboriginal_f AS indigenous_psns_aboriginal_f_host,
census_host.indigenous_p_tot_p AS indigenous_p_tot_p_host,
census_host.indigenous_p_tot_m AS indigenous_p_tot_m_host,
census_host.indigenous_p_tot_f AS indigenous_p_tot_f_host,
census_host.indig_psns_torres_strait_is_p AS indig_psns_torres_strait_is_p_host,
census_host.indig_psns_torres_strait_is_m AS indig_psns_torres_strait_is_m_host,
census_host.indig_psns_torres_strait_is_f AS indig_psns_torres_strait_is_f_host,
census_host.indig_bth_abor_torres_st_is_p AS indig_bth_abor_torres_st_is_p_host,
census_host.indig_bth_abor_torres_st_is_m AS indig_bth_abor_torres_st_is_m_host,
census_host.indig_bth_abor_torres_st_is_f AS indig_bth_abor_torres_st_is_f_host,
census_host.high_yr_schl_comp_yr_9_eq_p AS high_yr_schl_comp_yr_9_eq_p_host,
census_host.high_yr_schl_comp_yr_9_eq_m AS high_yr_schl_comp_yr_9_eq_m_host,
census_host.high_yr_schl_comp_yr_9_eq_f AS high_yr_schl_comp_yr_9_eq_f_host,
census_host.high_yr_schl_comp_yr_8_belw_p AS high_yr_schl_comp_yr_8_belw_p_host,
census_host.high_yr_schl_comp_yr_8_belw_m AS high_yr_schl_comp_yr_8_belw_m_host,
census_host.high_yr_schl_comp_yr_8_belw_f AS high_yr_schl_comp_yr_8_belw_f_host,
census_host.high_yr_schl_comp_yr_12_eq_p AS high_yr_schl_comp_yr_12_eq_p_host,
census_host.high_yr_schl_comp_yr_12_eq_m AS high_yr_schl_comp_yr_12_eq_m_host,
census_host.high_yr_schl_comp_yr_12_eq_f AS high_yr_schl_comp_yr_12_eq_f_host,
census_host.high_yr_schl_comp_yr_11_eq_p AS high_yr_schl_comp_yr_11_eq_p_host,
census_host.high_yr_schl_comp_yr_11_eq_m AS high_yr_schl_comp_yr_11_eq_m_host,
census_host.high_yr_schl_comp_yr_11_eq_f AS high_yr_schl_comp_yr_11_eq_f_host,
census_host.high_yr_schl_comp_yr_10_eq_p AS high_yr_schl_comp_yr_10_eq_p_host,
census_host.high_yr_schl_comp_yr_10_eq_m AS high_yr_schl_comp_yr_10_eq_m_host,
census_host.high_yr_schl_comp_yr_10_eq_f AS high_yr_schl_comp_yr_10_eq_f_host,
census_host.high_yr_schl_comp_d_n_g_sch_p AS high_yr_schl_comp_d_n_g_sch_p_host,
census_host.high_yr_schl_comp_d_n_g_sch_m AS high_yr_schl_comp_d_n_g_sch_m_host,
census_host.high_yr_schl_comp_d_n_g_sch_f AS high_yr_schl_comp_d_n_g_sch_f_host,
census_host.counted_census_night_home_p AS counted_census_night_home_p_host,
census_host.counted_census_night_home_m AS counted_census_night_home_m_host,
census_host.counted_census_night_home_f AS counted_census_night_home_f_host,
census_host.count_psns_occ_priv_dwgs_p AS count_psns_occ_priv_dwgs_p_host,
census_host.count_psns_occ_priv_dwgs_m AS count_psns_occ_priv_dwgs_m_host,
census_host.count_psns_occ_priv_dwgs_f AS count_psns_occ_priv_dwgs_f_host,
census_host.count_persons_other_dwgs_p AS count_persons_other_dwgs_p_host,
census_host.count_persons_other_dwgs_m AS count_persons_other_dwgs_m_host,
census_host.count_persons_other_dwgs_f AS count_persons_other_dwgs_f_host,
census_host.count_census_nt_ewhere_aust_p AS count_census_nt_ewhere_aust_p_host,
census_host.count_census_nt_ewhere_aust_m AS count_census_nt_ewhere_aust_m_host,
census_host.count_census_nt_ewhere_aust_f AS count_census_nt_ewhere_aust_f_host,
census_host.birthplace_elsewhere_p AS birthplace_elsewhere_p_host,
census_host.birthplace_elsewhere_m AS birthplace_elsewhere_m_host,
census_host.birthplace_elsewhere_f AS birthplace_elsewhere_f_host,
census_host.birthplace_australia_p AS birthplace_australia_p_host,
census_host.birthplace_australia_m AS birthplace_australia_m_host,
census_host.birthplace_australia_f AS birthplace_australia_f_host,
census_host.average_num_psns_per_bedroom AS average_num_psns_per_bedroom_host,
census_host.average_household_size AS average_household_size_host,
census_host.australian_citizen_p AS australian_citizen_p_host,
census_host.australian_citizen_m AS australian_citizen_m_host,
census_host.australian_citizen_f AS australian_citizen_f_host,
census_host.age_psns_att_educ_inst_5_14_p AS age_psns_att_educ_inst_5_14_p_host,
census_host.age_psns_att_educ_inst_5_14_m AS age_psns_att_educ_inst_5_14_m_host,
census_host.age_psns_att_educ_inst_5_14_f AS age_psns_att_educ_inst_5_14_f_host,
census_host.age_psns_att_educ_inst_0_4_p AS age_psns_att_educ_inst_0_4_p_host,
census_host.age_psns_att_educ_inst_0_4_m AS age_psns_att_educ_inst_0_4_m_host,
census_host.age_psns_att_educ_inst_0_4_f AS age_psns_att_educ_inst_0_4_f_host,
census_host.age_psns_att_edu_inst_25_ov_p AS age_psns_att_edu_inst_25_ov_p_host,
census_host.age_psns_att_edu_inst_25_ov_m AS age_psns_att_edu_inst_25_ov_m_host,
census_host.age_psns_att_edu_inst_25_ov_f AS age_psns_att_edu_inst_25_ov_f_host,
census_host.age_psns_att_edu_inst_20_24_p AS age_psns_att_edu_inst_20_24_p_host,
census_host.age_psns_att_edu_inst_20_24_m AS age_psns_att_edu_inst_20_24_m_host,
census_host.age_psns_att_edu_inst_20_24_f AS age_psns_att_edu_inst_20_24_f_host,
census_host.age_psns_att_edu_inst_15_19_p AS age_psns_att_edu_inst_15_19_p_host,
census_host.age_psns_att_edu_inst_15_19_m AS age_psns_att_edu_inst_15_19_m_host,
census_host.age_psns_att_edu_inst_15_19_f AS age_psns_att_edu_inst_15_19_f_host,
census_host.age_85ov_p AS age_85ov_p_host,
census_host.age_85ov_m AS age_85ov_m_host,
census_host.age_85ov_f AS age_85ov_f_host,
census_host.age_75_84_yr_p AS age_75_84_yr_p_host,
census_host.age_75_84_yr_m AS age_75_84_yr_m_host,
census_host.age_75_84_yr_f AS age_75_84_yr_f_host,
census_host.age_65_74_yr_p AS age_65_74_yr_p_host,
census_host.age_65_74_yr_m AS age_65_74_yr_m_host,
census_host.age_65_74_yr_f AS age_65_74_yr_f_host,
census_host.age_55_64_yr_p AS age_55_64_yr_p_host,
census_host.age_55_64_yr_m AS age_55_64_yr_m_host,
census_host.age_55_64_yr_f AS age_55_64_yr_f_host,
census_host.age_5_14_yr_p AS age_5_14_yr_p_host,
census_host.age_5_14_yr_m AS age_5_14_yr_m_host,
census_host.age_5_14_yr_f AS age_5_14_yr_f_host,
census_host.age_45_54_yr_p AS age_45_54_yr_p_host,
census_host.age_45_54_yr_m AS age_45_54_yr_m_host,
census_host.age_45_54_yr_f AS age_45_54_yr_f_host,
census_host.age_35_44_yr_p AS age_35_44_yr_p_host,
census_host.age_35_44_yr_m AS age_35_44_yr_m_host,
census_host.age_35_44_yr_f AS age_35_44_yr_f_host,
census_host.age_25_34_yr_p AS age_25_34_yr_p_host,
census_host.age_25_34_yr_m AS age_25_34_yr_m_host,
census_host.age_25_34_yr_f AS age_25_34_yr_f_host,
census_host.age_20_24_yr_p AS age_20_24_yr_p_host,
census_host.age_20_24_yr_m AS age_20_24_yr_m_host,
census_host.age_20_24_yr_f AS age_20_24_yr_f_host,
census_host.age_15_19_yr_p AS age_15_19_yr_p_host,
census_host.age_15_19_yr_m AS age_15_19_yr_m_host,
census_host.age_15_19_yr_f AS age_15_19_yr_f_host,
census_host.age_0_4_yr_p AS age_0_4_yr_p_host,
census_host.age_0_4_yr_m AS age_0_4_yr_m_host,
census_host.age_0_4_yr_f AS age_0_4_yr_f_host,


census_listing.tot_p_p AS tot_p_p_listing,
census_listing.tot_p_m AS tot_p_m_listing,
census_listing.tot_p_f AS tot_p_f_listing,
census_listing.median_tot_prsnl_inc_weekly AS median_tot_prsnl_inc_weekly_listing,
census_listing.median_tot_hhd_inc_weekly AS median_tot_hhd_inc_weekly_listing,
census_listing.median_tot_fam_inc_weekly AS median_tot_fam_inc_weekly_listing,
census_listing.median_rent_weekly AS median_rent_weekly_listing,
census_listing.median_mortgage_repay_monthly AS median_mortgage_repay_monthly_listing,
census_listing.median_age_persons AS median_age_persons_listing,
census_listing.lga_code AS lga_code_listing,
census_listing.lang_spoken_home_oth_lang_p AS lang_spoken_home_oth_lang_p_listing,
census_listing.lang_spoken_home_oth_lang_m AS lang_spoken_home_oth_lang_m_listing,
census_listing.lang_spoken_home_oth_lang_f AS lang_spoken_home_oth_lang_f_listing,
census_listing.lang_spoken_home_eng_only_p AS lang_spoken_home_eng_only_p_listing,
census_listing.lang_spoken_home_eng_only_m AS lang_spoken_home_eng_only_m_listing,
census_listing.lang_spoken_home_eng_only_f AS lang_spoken_home_eng_only_f_listing,
census_listing.indigenous_psns_aboriginal_p AS indigenous_psns_aboriginal_p_listing,
census_listing.indigenous_psns_aboriginal_m AS indigenous_psns_aboriginal_m_listing,
census_listing.indigenous_psns_aboriginal_f AS indigenous_psns_aboriginal_f_listing,
census_listing.indigenous_p_tot_p AS indigenous_p_tot_p_listing,
census_listing.indigenous_p_tot_m AS indigenous_p_tot_m_listing,
census_listing.indigenous_p_tot_f AS indigenous_p_tot_f_listing,
census_listing.indig_psns_torres_strait_is_p AS indig_psns_torres_strait_is_p_listing,
census_listing.indig_psns_torres_strait_is_m AS indig_psns_torres_strait_is_m_listing,
census_listing.indig_psns_torres_strait_is_f AS indig_psns_torres_strait_is_f_listing,
census_listing.indig_bth_abor_torres_st_is_p AS indig_bth_abor_torres_st_is_p_listing,
census_listing.indig_bth_abor_torres_st_is_m AS indig_bth_abor_torres_st_is_m_listing,
census_listing.indig_bth_abor_torres_st_is_f AS indig_bth_abor_torres_st_is_f_listing,
census_listing.high_yr_schl_comp_yr_9_eq_p AS high_yr_schl_comp_yr_9_eq_p_listing,
census_listing.high_yr_schl_comp_yr_9_eq_m AS high_yr_schl_comp_yr_9_eq_m_listing,
census_listing.high_yr_schl_comp_yr_9_eq_f AS high_yr_schl_comp_yr_9_eq_f_listing,
census_listing.high_yr_schl_comp_yr_8_belw_p AS high_yr_schl_comp_yr_8_belw_p_listing,
census_listing.high_yr_schl_comp_yr_8_belw_m AS high_yr_schl_comp_yr_8_belw_m_listing,
census_listing.high_yr_schl_comp_yr_8_belw_f AS high_yr_schl_comp_yr_8_belw_f_listing,
census_listing.high_yr_schl_comp_yr_12_eq_p AS high_yr_schl_comp_yr_12_eq_p_listing,
census_listing.high_yr_schl_comp_yr_12_eq_m AS high_yr_schl_comp_yr_12_eq_m_listing,
census_listing.high_yr_schl_comp_yr_12_eq_f AS high_yr_schl_comp_yr_12_eq_f_listing,
census_listing.high_yr_schl_comp_yr_11_eq_p AS high_yr_schl_comp_yr_11_eq_p_listing,
census_listing.high_yr_schl_comp_yr_11_eq_m AS high_yr_schl_comp_yr_11_eq_m_listing,
census_listing.high_yr_schl_comp_yr_11_eq_f AS high_yr_schl_comp_yr_11_eq_f_listing,
census_listing.high_yr_schl_comp_yr_10_eq_p AS high_yr_schl_comp_yr_10_eq_p_listing,
census_listing.high_yr_schl_comp_yr_10_eq_m AS high_yr_schl_comp_yr_10_eq_m_listing,
census_listing.high_yr_schl_comp_yr_10_eq_f AS high_yr_schl_comp_yr_10_eq_f_listing,
census_listing.high_yr_schl_comp_d_n_g_sch_p AS high_yr_schl_comp_d_n_g_sch_p_listing,
census_listing.high_yr_schl_comp_d_n_g_sch_m AS high_yr_schl_comp_d_n_g_sch_m_listing,
census_listing.high_yr_schl_comp_d_n_g_sch_f AS high_yr_schl_comp_d_n_g_sch_f_listing,
census_listing.counted_census_night_home_p AS counted_census_night_home_p_listing,
census_listing.counted_census_night_home_m AS counted_census_night_home_m_listing,
census_listing.counted_census_night_home_f AS counted_census_night_home_f_listing,
census_listing.count_psns_occ_priv_dwgs_p AS count_psns_occ_priv_dwgs_p_listing,
census_listing.count_psns_occ_priv_dwgs_m AS count_psns_occ_priv_dwgs_m_listing,
census_listing.count_psns_occ_priv_dwgs_f AS count_psns_occ_priv_dwgs_f_listing,
census_listing.count_persons_other_dwgs_p AS count_persons_other_dwgs_p_listing,
census_listing.count_persons_other_dwgs_m AS count_persons_other_dwgs_m_listing,
census_listing.count_persons_other_dwgs_f AS count_persons_other_dwgs_f_listing,
census_listing.count_census_nt_ewhere_aust_p AS count_census_nt_ewhere_aust_p_listing,
census_listing.count_census_nt_ewhere_aust_m AS count_census_nt_ewhere_aust_m_listing,
census_listing.count_census_nt_ewhere_aust_f AS count_census_nt_ewhere_aust_f_listing,
census_listing.birthplace_elsewhere_p AS birthplace_elsewhere_p_listing,
census_listing.birthplace_elsewhere_m AS birthplace_elsewhere_m_listing,
census_listing.birthplace_elsewhere_f AS birthplace_elsewhere_f_listing,
census_listing.birthplace_australia_p AS birthplace_australia_p_listing,
census_listing.birthplace_australia_m AS birthplace_australia_m_listing,
census_listing.birthplace_australia_f AS birthplace_australia_f_listing,
census_listing.average_num_psns_per_bedroom AS average_num_psns_per_bedroom_listing,
census_listing.average_household_size AS average_household_size_listing,
census_listing.australian_citizen_p AS australian_citizen_p_listing,
census_listing.australian_citizen_m AS australian_citizen_m_listing,
census_listing.australian_citizen_f AS australian_citizen_f_listing,
census_listing.age_psns_att_educ_inst_5_14_p AS age_psns_att_educ_inst_5_14_p_listing,
census_listing.age_psns_att_educ_inst_5_14_m AS age_psns_att_educ_inst_5_14_m_listing,
census_listing.age_psns_att_educ_inst_5_14_f AS age_psns_att_educ_inst_5_14_f_listing,
census_listing.age_psns_att_educ_inst_0_4_p AS age_psns_att_educ_inst_0_4_p_listing,
census_listing.age_psns_att_educ_inst_0_4_m AS age_psns_att_educ_inst_0_4_m_listing,
census_listing.age_psns_att_educ_inst_0_4_f AS age_psns_att_educ_inst_0_4_f_listing,
census_listing.age_psns_att_edu_inst_25_ov_p AS age_psns_att_edu_inst_25_ov_p_listing,
census_listing.age_psns_att_edu_inst_25_ov_m AS age_psns_att_edu_inst_25_ov_m_listing,
census_listing.age_psns_att_edu_inst_25_ov_f AS age_psns_att_edu_inst_25_ov_f_listing,
census_listing.age_psns_att_edu_inst_20_24_p AS age_psns_att_edu_inst_20_24_p_listing,
census_listing.age_psns_att_edu_inst_20_24_m AS age_psns_att_edu_inst_20_24_m_listing,
census_listing.age_psns_att_edu_inst_20_24_f AS age_psns_att_edu_inst_20_24_f_listing,
census_listing.age_psns_att_edu_inst_15_19_p AS age_psns_att_edu_inst_15_19_p_listing,
census_listing.age_psns_att_edu_inst_15_19_m AS age_psns_att_edu_inst_15_19_m_listing,
census_listing.age_psns_att_edu_inst_15_19_f AS age_psns_att_edu_inst_15_19_f_listing,
census_listing.age_85ov_p AS age_85ov_p_listing,
census_listing.age_85ov_m AS age_85ov_m_listing,
census_listing.age_85ov_f AS age_85ov_f_listing,
census_listing.age_75_84_yr_p AS age_75_84_yr_p_listing,
census_listing.age_75_84_yr_m AS age_75_84_yr_m_listing,
census_listing.age_75_84_yr_f AS age_75_84_yr_f_listing,
census_listing.age_65_74_yr_p AS age_65_74_yr_p_listing,
census_listing.age_65_74_yr_m AS age_65_74_yr_m_listing,
census_listing.age_65_74_yr_f AS age_65_74_yr_f_listing,
census_listing.age_55_64_yr_p AS age_55_64_yr_p_listing,
census_listing.age_55_64_yr_m AS age_55_64_yr_m_listing,
census_listing.age_55_64_yr_f AS age_55_64_yr_f_listing,
census_listing.age_5_14_yr_p AS age_5_14_yr_p_listing,
census_listing.age_5_14_yr_m AS age_5_14_yr_m_listing,
census_listing.age_5_14_yr_f AS age_5_14_yr_f_listing,
census_listing.age_45_54_yr_p AS age_45_54_yr_p_listing,
census_listing.age_45_54_yr_m AS age_45_54_yr_m_listing,
census_listing.age_45_54_yr_f AS age_45_54_yr_f_listing,
census_listing.age_35_44_yr_p AS age_35_44_yr_p_listing,
census_listing.age_35_44_yr_m AS age_35_44_yr_m_listing,
census_listing.age_35_44_yr_f AS age_35_44_yr_f_listing,
census_listing.age_25_34_yr_p AS age_25_34_yr_p_listing,
census_listing.age_25_34_yr_m AS age_25_34_yr_m_listing,
census_listing.age_25_34_yr_f AS age_25_34_yr_f_listing,
census_listing.age_20_24_yr_p AS age_20_24_yr_p_listing,
census_listing.age_20_24_yr_m AS age_20_24_yr_m_listing,
census_listing.age_20_24_yr_f AS age_20_24_yr_f_listing,
census_listing.age_15_19_yr_p AS age_15_19_yr_p_listing,
census_listing.age_15_19_yr_m AS age_15_19_yr_m_listing,
census_listing.age_15_19_yr_f AS age_15_19_yr_f_listing,
census_listing.age_0_4_yr_p AS age_0_4_yr_p_listing,
census_listing.age_0_4_yr_m AS age_0_4_yr_m_listing,
census_listing.age_0_4_yr_f AS age_0_4_yr_f_listing



from facts
left join host on facts.host_id = host.host_id 
    AND facts.scraped_date = host.dbt_valid_from
left join property on facts.listing_id = property.listing_id
    AND facts.scraped_date = property.dbt_valid_from
left join room on facts.listing_id = room.listing_id 
    AND facts.scraped_date = room.dbt_valid_from
left join neighbourhood on facts.listing_id = neighbourhood.listing_id
    and facts.scraped_date = neighbourhood.dbt_valid_from
left join suburb_lga suburb_lga_host on UPPER(suburb_lga_host.suburb_name) = UPPER(neighbourhood.host_neighbourhood)
left join suburb_lga suburb_lga_listing on UPPER(suburb_lga_listing.suburb_name) = UPPER(neighbourhood.listing_neighbourhood)
left join census census_host on suburb_lga_host.lga_code = census_host.lga_code
left join census census_listing on suburb_lga_listing.lga_code = census_listing.lga_code 
