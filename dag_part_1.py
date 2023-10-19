import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

########################################################
#
#   DAG Settings
#
#########################################################



dag_default_args = {
    'owner': 'assignment_3',
    'start_date': datetime.now(),
    'email': ['akexanderdaniel@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='assignment_3',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data"
DIMENSIONS = AIRFLOW_DATA+"/dimensions/"
FACTS = AIRFLOW_DATA+"/facts/"


#########################################################
#
#   Custom Logics for Operator
#
#########################################################

def import_load_dim_nswlgacode_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    #generate dataframe by combining files
    df = pd.read_csv(DIMENSIONS+'NSW_LGA_CODE.csv')

    if len(df) > 0:
        col_names = ['LGA_CODE','LGA_NAME']

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.nsw_lga_code(LGA_CODE,LGA_NAME)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=1000)
        conn_ps.commit()
    else:
        None

    return None      


def import_load_dim_nswlgasuburb_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    #generate dataframe by combining files
    df = pd.read_csv(DIMENSIONS+'NSW_LGA_SUBURB.csv')

    if len(df) > 0:
        col_names = ['LGA_NAME','SUBURB_NAME']

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.nsw_lga_suburb(LGA_NAME,SUBURB_NAME)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=1000)
        conn_ps.commit()
    else:
        None

    return None       


def import_load_dim_censusG02_func(**kwargs):

     #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    #generate dataframe by combining files
    df = pd.read_csv(DIMENSIONS+'2016Census_G02_NSW_LGA.csv')

    if len(df) > 0:
        col_names = ["LGA_CODE_2016",
    "Median_age_persons",
    "Median_mortgage_repay_monthly",
    "Median_tot_prsnl_inc_weekly",
    "Median_rent_weekly",
    "Median_tot_fam_inc_weekly",
    "Average_num_psns_per_bedroom",
    "Median_tot_hhd_inc_weekly",
    "Average_household_size"
]

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.2016Census_G02_NSW_LGA(
        lga_code_2016,
        median_age_persons,
        median_mortgage_repay_monthly,
        median_tot_prsnl_inc_weekly,
        median_rent_weekly,
        median_tot_fam_inc_weekly,
        average_num_psns_per_bedroom,
        median_tot_hhd_inc_weekly,
        average_household_size)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=1000)
        conn_ps.commit()
    else:
        None

    return None     


def import_load_dim_censusG01_func(**kwargs):
  
   #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    #generate dataframe by combining files
    df = pd.read_csv(DIMENSIONS+'2016Census_G01_NSW_LGA.csv')

    if len(df) > 0:
        col_names = ['LGA_CODE_2016',
 'Tot_P_M',
 'Tot_P_F',
 'Tot_P_P',
 'Age_0_4_yr_M',
 'Age_0_4_yr_F',
 'Age_0_4_yr_P',
 'Age_5_14_yr_M',
 'Age_5_14_yr_F',
 'Age_5_14_yr_P',
 'Age_15_19_yr_M',
 'Age_15_19_yr_F',
 'Age_15_19_yr_P',
 'Age_20_24_yr_M',
 'Age_20_24_yr_F',
 'Age_20_24_yr_P',
 'Age_25_34_yr_M',
 'Age_25_34_yr_F',
 'Age_25_34_yr_P',
 'Age_35_44_yr_M',
 'Age_35_44_yr_F',
 'Age_35_44_yr_P',
 'Age_45_54_yr_M',
 'Age_45_54_yr_F',
 'Age_45_54_yr_P',
 'Age_55_64_yr_M',
 'Age_55_64_yr_F',
 'Age_55_64_yr_P',
 'Age_65_74_yr_M',
 'Age_65_74_yr_F',
 'Age_65_74_yr_P',
 'Age_75_84_yr_M',
 'Age_75_84_yr_F',
 'Age_75_84_yr_P',
 'Age_85ov_M',
 'Age_85ov_F',
 'Age_85ov_P',
 'Counted_Census_Night_home_M',
 'Counted_Census_Night_home_F',
 'Counted_Census_Night_home_P',
 'Count_Census_Nt_Ewhere_Aust_M',
 'Count_Census_Nt_Ewhere_Aust_F',
 'Count_Census_Nt_Ewhere_Aust_P',
 'Indigenous_psns_Aboriginal_M',
 'Indigenous_psns_Aboriginal_F',
 'Indigenous_psns_Aboriginal_P',
 'Indig_psns_Torres_Strait_Is_M',
 'Indig_psns_Torres_Strait_Is_F',
 'Indig_psns_Torres_Strait_Is_P',
 'Indig_Bth_Abor_Torres_St_Is_M',
 'Indig_Bth_Abor_Torres_St_Is_F',
 'Indig_Bth_Abor_Torres_St_Is_P',
 'Indigenous_P_Tot_M',
 'Indigenous_P_Tot_F',
 'Indigenous_P_Tot_P',
 'Birthplace_Australia_M',
 'Birthplace_Australia_F',
 'Birthplace_Australia_P',
 'Birthplace_Elsewhere_M',
 'Birthplace_Elsewhere_F',
 'Birthplace_Elsewhere_P',
 'Lang_spoken_home_Eng_only_M',
 'Lang_spoken_home_Eng_only_F',
 'Lang_spoken_home_Eng_only_P',
 'Lang_spoken_home_Oth_Lang_M',
 'Lang_spoken_home_Oth_Lang_F',
 'Lang_spoken_home_Oth_Lang_P',
 'Australian_citizen_M',
 'Australian_citizen_F',
 'Australian_citizen_P',
 'Age_psns_att_educ_inst_0_4_M',
 'Age_psns_att_educ_inst_0_4_F',
 'Age_psns_att_educ_inst_0_4_P',
 'Age_psns_att_educ_inst_5_14_M',
 'Age_psns_att_educ_inst_5_14_F',
 'Age_psns_att_educ_inst_5_14_P',
 'Age_psns_att_edu_inst_15_19_M',
 'Age_psns_att_edu_inst_15_19_F',
 'Age_psns_att_edu_inst_15_19_P',
 'Age_psns_att_edu_inst_20_24_M',
 'Age_psns_att_edu_inst_20_24_F',
 'Age_psns_att_edu_inst_20_24_P',
 'Age_psns_att_edu_inst_25_ov_M',
 'Age_psns_att_edu_inst_25_ov_F',
 'Age_psns_att_edu_inst_25_ov_P',
 'High_yr_schl_comp_Yr_12_eq_M',
 'High_yr_schl_comp_Yr_12_eq_F',
 'High_yr_schl_comp_Yr_12_eq_P',
 'High_yr_schl_comp_Yr_11_eq_M',
 'High_yr_schl_comp_Yr_11_eq_F',
 'High_yr_schl_comp_Yr_11_eq_P',
 'High_yr_schl_comp_Yr_10_eq_M',
 'High_yr_schl_comp_Yr_10_eq_F',
 'High_yr_schl_comp_Yr_10_eq_P',
 'High_yr_schl_comp_Yr_9_eq_M',
 'High_yr_schl_comp_Yr_9_eq_F',
 'High_yr_schl_comp_Yr_9_eq_P',
 'High_yr_schl_comp_Yr_8_belw_M',
 'High_yr_schl_comp_Yr_8_belw_F',
 'High_yr_schl_comp_Yr_8_belw_P',
 'High_yr_schl_comp_D_n_g_sch_M',
 'High_yr_schl_comp_D_n_g_sch_F',
 'High_yr_schl_comp_D_n_g_sch_P',
 'Count_psns_occ_priv_dwgs_M',
 'Count_psns_occ_priv_dwgs_F',
 'Count_psns_occ_priv_dwgs_P',
 'Count_Persons_other_dwgs_M',
 'Count_Persons_other_dwgs_F',
 'Count_Persons_other_dwgs_P']

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.2016Census_G01_NSW_LGA(lga_code_2016,
                    tot_p_m,
                    tot_p_f,
                    tot_p_p,
                    age_0_4_yr_m,
                    age_0_4_yr_f,
                    age_0_4_yr_p,
                    age_5_14_yr_m,
                    age_5_14_yr_f,
                    age_5_14_yr_p,
                    age_15_19_yr_m,
                    age_15_19_yr_f,
                    age_15_19_yr_p,
                    age_20_24_yr_m,
                    age_20_24_yr_f,
                    age_20_24_yr_p,
                    age_25_34_yr_m,
                    age_25_34_yr_f,
                    age_25_34_yr_p,
                    age_35_44_yr_m,
                    age_35_44_yr_f,
                    age_35_44_yr_p,
                    age_45_54_yr_m,
                    age_45_54_yr_f,
                    age_45_54_yr_p,
                    age_55_64_yr_m,
                    age_55_64_yr_f,
                    age_55_64_yr_p,
                    age_65_74_yr_m,
                    age_65_74_yr_f,
                    age_65_74_yr_p,
                    age_75_84_yr_m,
                    age_75_84_yr_f,
                    age_75_84_yr_p,
                    age_85ov_m,
                    age_85ov_f,
                    age_85ov_p,
                    counted_census_night_home_m,
                    counted_census_night_home_f,
                    counted_census_night_home_p,
                    count_census_nt_ewhere_aust_m,
                    count_census_nt_ewhere_aust_f,
                    count_census_nt_ewhere_aust_p,
                    indigenous_psns_aboriginal_m,
                    indigenous_psns_aboriginal_f,
                    indigenous_psns_aboriginal_p,
                    indig_psns_torres_strait_is_m,
                    indig_psns_torres_strait_is_f,
                    indig_psns_torres_strait_is_p,
                    indig_bth_abor_torres_st_is_m,
                    indig_bth_abor_torres_st_is_f,
                    indig_bth_abor_torres_st_is_p,
                    indigenous_p_tot_m,
                    indigenous_p_tot_f,
                    indigenous_p_tot_p,
                    birthplace_australia_m,
                    birthplace_australia_f,
                    birthplace_australia_p,
                    birthplace_elsewhere_m,
                    birthplace_elsewhere_f,
                    birthplace_elsewhere_p,
                    lang_spoken_home_eng_only_m,
                    lang_spoken_home_eng_only_f,
                    lang_spoken_home_eng_only_p,
                    lang_spoken_home_oth_lang_m,
                    lang_spoken_home_oth_lang_f,
                    lang_spoken_home_oth_lang_p,
                    australian_citizen_m,
                    australian_citizen_f,
                    australian_citizen_p,
                    age_psns_att_educ_inst_0_4_m,
                    age_psns_att_educ_inst_0_4_f,
                    age_psns_att_educ_inst_0_4_p,
                    age_psns_att_educ_inst_5_14_m,
                    age_psns_att_educ_inst_5_14_f,
                    age_psns_att_educ_inst_5_14_p,
                    age_psns_att_edu_inst_15_19_m,
                    age_psns_att_edu_inst_15_19_f,
                    age_psns_att_edu_inst_15_19_p,
                    age_psns_att_edu_inst_20_24_m,
                    age_psns_att_edu_inst_20_24_f,
                    age_psns_att_edu_inst_20_24_p,
                    age_psns_att_edu_inst_25_ov_m,
                    age_psns_att_edu_inst_25_ov_f,
                    age_psns_att_edu_inst_25_ov_p,
                    high_yr_schl_comp_yr_12_eq_m,
                    high_yr_schl_comp_yr_12_eq_f,
                    high_yr_schl_comp_yr_12_eq_p,
                    high_yr_schl_comp_yr_11_eq_m,
                    high_yr_schl_comp_yr_11_eq_f,
                    high_yr_schl_comp_yr_11_eq_p,
                    high_yr_schl_comp_yr_10_eq_m,
                    high_yr_schl_comp_yr_10_eq_f,
                    high_yr_schl_comp_yr_10_eq_p,
                    high_yr_schl_comp_yr_9_eq_m,
                    high_yr_schl_comp_yr_9_eq_f,
                    high_yr_schl_comp_yr_9_eq_p,
                    high_yr_schl_comp_yr_8_belw_m,
                    high_yr_schl_comp_yr_8_belw_f,
                    high_yr_schl_comp_yr_8_belw_p,
                    high_yr_schl_comp_d_n_g_sch_m,
                    high_yr_schl_comp_d_n_g_sch_f,
                    high_yr_schl_comp_d_n_g_sch_p,
                    count_psns_occ_priv_dwgs_m,
                    count_psns_occ_priv_dwgs_f,
                    count_psns_occ_priv_dwgs_p,
                    count_persons_other_dwgs_m,
                    count_persons_other_dwgs_f,
                    count_persons_other_dwgs_p)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=1000)
        conn_ps.commit()
    else:
        None

    return None 

def import_load_facts_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    filelist = [k for k in os.listdir(FACTS) if '.csv' in k]

    #generate dataframe by combining files
    df = pd.concat([pd.read_csv(os.path.join(FACTS, fname)) for fname in filelist], ignore_index=True)

    if len(df) > 0:
        col_names = ['LISTING_ID',
    'SCRAPE_ID',
    'SCRAPED_DATE',
    'HOST_ID',
    'HOST_NAME',
    'HOST_SINCE',
    'HOST_IS_SUPERHOST',
    'HOST_NEIGHBOURHOOD',
    'LISTING_NEIGHBOURHOOD',
    'PROPERTY_TYPE',
    'ROOM_TYPE',
    'ACCOMMODATES',
    'PRICE',
    'HAS_AVAILABILITY',
    'AVAILABILITY_30',
    'NUMBER_OF_REVIEWS',
    'REVIEW_SCORES_RATING',
    'REVIEW_SCORES_ACCURACY',
    'REVIEW_SCORES_CLEANLINESS',
    'REVIEW_SCORES_CHECKIN',
    'REVIEW_SCORES_COMMUNICATION',
    'REVIEW_SCORES_VALUE']

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.listings(
                        LISTING_ID,
                        SCRAPE_ID,
                        SCRAPED_DATE,
                        HOST_ID,
                        HOST_NAME,
                        HOST_SINCE,
                        HOST_IS_SUPERHOST,
                        HOST_NEIGHBOURHOOD,
                        LISTING_NEIGHBOURHOOD,
                        PROPERTY_TYPE,
                        ROOM_TYPE,
                        ACCOMMODATES,
                        PRICE,
                        HAS_AVAILABILITY,
                        AVAILABILITY_30,
                        NUMBER_OF_REVIEWS,
                        REVIEW_SCORES_RATING,
                        REVIEW_SCORES_ACCURACY,
                        REVIEW_SCORES_CLEANLINESS,
                        REVIEW_SCORES_CHECKIN,
                        REVIEW_SCORES_COMMUNICATION,
                        REVIEW_SCORES_VALUE
                    )
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=1000)
        conn_ps.commit()
    else:
        None

    return None      



#########################################################
#
#   DAG Operator Setup
#
#########################################################

import_load_dim_nswlgacode_task= PythonOperator(
    task_id="import_load_dim_nswlgacode",
    python_callable=import_load_dim_nswlgacode_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_dim_nswlgasuburb_task = PythonOperator(
    task_id="import_load_dim_nswlgasuburb",
    python_callable=import_load_dim_nswlgasuburb_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_dim_censusG02_task = PythonOperator(
    task_id="import_load_dim_censusG02",
    python_callable=import_load_dim_censusG02_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_dim_censusG01_task = PythonOperator(
    task_id="import_load_dim_censusG01",
    python_callable=import_load_dim_censusG01_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)


import_load_facts_task = PythonOperator(
    task_id="import_load_facts_id",
    python_callable=import_load_facts_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)


[import_load_dim_nswlgacode_task, import_load_dim_nswlgasuburb_task, import_load_dim_censusG02_task,import_load_dim_censusG01_task ,import_load_facts_task]