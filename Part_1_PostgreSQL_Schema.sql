create schema raw;

---------------------------
-- Raw
---------------------------



create table raw.listings (
	LISTING_ID varchar,
	SCRAPE_ID varchar NULL,
	SCRAPED_DATE varchar null,
	HOST_ID	varchar null,
	HOST_NAME varchar null, 
	HOST_SINCE varchar null,
	HOST_IS_SUPERHOST varchar null,
	HOST_NEIGHBOURHOOD varchar null,
	LISTING_NEIGHBOURHOOD varchar null,
	PROPERTY_TYPE varchar null,
	ROOM_TYPE varchar null,
	ACCOMMODATES varchar null,
	PRICE varchar null,
	HAS_AVAILABILITY varchar,
	AVAILABILITY_30 varchar null,
	NUMBER_OF_REVIEWS varchar null,
	REVIEW_SCORES_RATING varchar NULL,
	REVIEW_SCORES_ACCURACY varchar null,
	REVIEW_SCORES_CLEANLINESS varchar null,
	REVIEW_SCORES_CHECKIN varchar null,	
	REVIEW_SCORES_COMMUNICATION varchar null,
	REVIEW_SCORES_VALUE varchar null
);


create table raw.nsw_lga_code (
lga_code integer primary key,
lga_name varchar null
);

create table raw.nsw_lga_suburb (
lga_name varchar null,
suburb_name varchar NULL
);



CREATE TABLE raw.Census_G01_NSW_LGA (
	lga_code_2016 varchar(50) NULL,
	tot_p_m integer NULL,
	tot_p_f integer NULL,
	tot_p_p integer NULL,
	age_0_4_yr_m integer NULL,
	age_0_4_yr_f integer NULL,
	age_0_4_yr_p integer NULL,
	age_5_14_yr_m integer NULL,
	age_5_14_yr_f integer NULL,
	age_5_14_yr_p integer NULL,
	age_15_19_yr_m integer NULL,
	age_15_19_yr_f integer NULL,
	age_15_19_yr_p integer NULL,
	age_20_24_yr_m integer NULL,
	age_20_24_yr_f integer NULL,
	age_20_24_yr_p integer NULL,
	age_25_34_yr_m integer NULL,
	age_25_34_yr_f integer NULL,
	age_25_34_yr_p integer NULL,
	age_35_44_yr_m integer NULL,
	age_35_44_yr_f integer NULL,
	age_35_44_yr_p integer NULL,
	age_45_54_yr_m integer NULL,
	age_45_54_yr_f integer NULL,
	age_45_54_yr_p integer NULL,
	age_55_64_yr_m integer NULL,
	age_55_64_yr_f integer NULL,
	age_55_64_yr_p integer NULL,
	age_65_74_yr_m integer NULL,
	age_65_74_yr_f integer NULL,
	age_65_74_yr_p integer NULL,
	age_75_84_yr_m integer NULL,
	age_75_84_yr_f integer NULL,
	age_75_84_yr_p integer NULL,
	age_85ov_m integer NULL,
	age_85ov_f integer NULL,
	age_85ov_p integer NULL,
	counted_census_night_home_m integer NULL,
	counted_census_night_home_f integer NULL,
	counted_census_night_home_p integer NULL,
	count_census_nt_ewhere_aust_m integer NULL,
	count_census_nt_ewhere_aust_f integer NULL,
	count_census_nt_ewhere_aust_p integer NULL,
	indigenous_psns_aboriginal_m integer NULL,
	indigenous_psns_aboriginal_f integer NULL,
	indigenous_psns_aboriginal_p integer NULL,
	indig_psns_torres_strait_is_m integer NULL,
	indig_psns_torres_strait_is_f integer NULL,
	indig_psns_torres_strait_is_p integer NULL,
	indig_bth_abor_torres_st_is_m integer NULL,
	indig_bth_abor_torres_st_is_f integer NULL,
	indig_bth_abor_torres_st_is_p integer NULL,
	indigenous_p_tot_m integer NULL,
	indigenous_p_tot_f integer NULL,
	indigenous_p_tot_p integer NULL,
	birthplace_australia_m integer NULL,
	birthplace_australia_f integer NULL,
	birthplace_australia_p integer NULL,
	birthplace_elsewhere_m integer NULL,
	birthplace_elsewhere_f integer NULL,
	birthplace_elsewhere_p integer NULL,
	lang_spoken_home_eng_only_m integer NULL,
	lang_spoken_home_eng_only_f integer NULL,
	lang_spoken_home_eng_only_p integer NULL,
	lang_spoken_home_oth_lang_m integer NULL,
	lang_spoken_home_oth_lang_f integer NULL,
	lang_spoken_home_oth_lang_p integer NULL,
	australian_citizen_m integer NULL,
	australian_citizen_f integer NULL,
	australian_citizen_p integer NULL,
	age_psns_att_educ_inst_0_4_m integer NULL,
	age_psns_att_educ_inst_0_4_f integer NULL,
	age_psns_att_educ_inst_0_4_p integer NULL,
	age_psns_att_educ_inst_5_14_m integer NULL,
	age_psns_att_educ_inst_5_14_f integer NULL,
	age_psns_att_educ_inst_5_14_p integer NULL,
	age_psns_att_edu_inst_15_19_m integer NULL,
	age_psns_att_edu_inst_15_19_f integer NULL,
	age_psns_att_edu_inst_15_19_p integer NULL,
	age_psns_att_edu_inst_20_24_m integer NULL,
	age_psns_att_edu_inst_20_24_f integer NULL,
	age_psns_att_edu_inst_20_24_p integer NULL,
	age_psns_att_edu_inst_25_ov_m integer NULL,
	age_psns_att_edu_inst_25_ov_f integer NULL,
	age_psns_att_edu_inst_25_ov_p integer NULL,
	high_yr_schl_comp_yr_12_eq_m integer NULL,
	high_yr_schl_comp_yr_12_eq_f integer NULL,
	high_yr_schl_comp_yr_12_eq_p integer NULL,
	high_yr_schl_comp_yr_11_eq_m integer NULL,
	high_yr_schl_comp_yr_11_eq_f integer NULL,
	high_yr_schl_comp_yr_11_eq_p integer NULL,
	high_yr_schl_comp_yr_10_eq_m integer NULL,
	high_yr_schl_comp_yr_10_eq_f integer NULL,
	high_yr_schl_comp_yr_10_eq_p integer NULL,
	high_yr_schl_comp_yr_9_eq_m integer NULL,
	high_yr_schl_comp_yr_9_eq_f integer NULL,
	high_yr_schl_comp_yr_9_eq_p integer NULL,
	high_yr_schl_comp_yr_8_belw_m integer NULL,
	high_yr_schl_comp_yr_8_belw_f integer NULL,
	high_yr_schl_comp_yr_8_belw_p integer NULL,
	high_yr_schl_comp_d_n_g_sch_m integer NULL,
	high_yr_schl_comp_d_n_g_sch_f integer NULL,
	high_yr_schl_comp_d_n_g_sch_p integer NULL,
	count_psns_occ_priv_dwgs_m integer NULL,
	count_psns_occ_priv_dwgs_f integer NULL,
	count_psns_occ_priv_dwgs_p integer NULL,
	count_persons_other_dwgs_m integer NULL,
	count_persons_other_dwgs_f integer NULL,
	count_persons_other_dwgs_p integer NULL
);



CREATE TABLE raw.Census_G02_NSW_LGA (
	lga_code_2016 varchar(50) NULL,
	median_age_persons integer NULL,
	median_mortgage_repay_monthly integer NULL,
	median_tot_prsnl_inc_weekly integer NULL,
	median_rent_weekly integer NULL,
	median_tot_fam_inc_weekly integer NULL,
	average_num_psns_per_bedroom real NULL,
	median_tot_hhd_inc_weekly integer NULL,
	average_household_size real NULL
);




