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
    'retries': 2,
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
    df = pd.read_csv(DIMENSIONS+'nsw_lga_code.csv')

    if len(df) > 0:
        col_names = ['lga_code','lga_name']

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.nsw_lga_code(lga_code,lga_name)
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
    df = pd.read_csv(DIMENSIONS+'nsw_lga_suburb.csv')

    if len(df) > 0:
        col_names = ['lga_name','suburb_name']

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.nsw_lga_suburb(lga_code,lga_name)
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
        col_names = ["lga_code_2016",
        "median_age_persons",
        "median_mortgage_repay_monthly",
        "median_tot_prsnl_inc_weekly",
        "median_rent_weekly",
        "median_tot_fam_inc_weekly",
        "average_num_psns_per_bedroom",
        "median_tot_hhd_inc_weekly",
        "average_household_size"]

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
        col_names = ["lga_code_2016",
    "tot_p_m",
    "tot_p_f",
    "tot_p_p",
    "age_0_4_yr_m",
    "age_0_4_yr_f",
    "age_0_4_yr_p",
    "age_5_14_yr_m",
    "age_5_14_yr_f",
    "age_5_14_yr_p",
    "age_15_19_yr_m",
    "age_15_19_yr_f",
    "age_15_19_yr_p",
    "age_20_24_yr_m",
    "age_20_24_yr_f",
    "age_20_24_yr_p",
    "age_25_34_yr_m",
    "age_25_34_yr_f",
    "age_25_34_yr_p",
    "age_35_44_yr_m",
    "age_35_44_yr_f",
    "age_35_44_yr_p",
    "age_45_54_yr_m",
    "age_45_54_yr_f",
    "age_45_54_yr_p",
    "age_55_64_yr_m",
    "age_55_64_yr_f",
    "age_55_64_yr_p",
    "age_65_74_yr_m",
    "age_65_74_yr_f",
    "age_65_74_yr_p",
    "age_75_84_yr_m",
    "age_75_84_yr_f",
    "age_75_84_yr_p",
    "age_85ov_m",
    "age_85ov_f",
    "age_85ov_p",
    "counted_census_night_home_m",
    "counted_census_night_home_f",
    "counted_census_night_home_p",
    "count_census_nt_ewhere_aust_m",
    "count_census_nt_ewhere_aust_f",
    "count_census_nt_ewhere_aust_p",
    "indigenous_psns_aboriginal_m",
    "indigenous_psns_aboriginal_f",
    "indigenous_psns_aboriginal_p",
    "indig_psns_torres_strait_is_m",
    "indig_psns_torres_strait_is_f",
    "indig_psns_torres_strait_is_p",
    "indig_bth_abor_torres_st_is_m",
    "indig_bth_abor_torres_st_is_f",
    "indig_bth_abor_torres_st_is_p",
    "indigenous_p_tot_m",
    "indigenous_p_tot_f",
    "indigenous_p_tot_p",
    "birthplace_australia_m",
    "birthplace_australia_f",
    "birthplace_australia_p",
    "birthplace_elsewhere_m",
    "birthplace_elsewhere_f",
    "birthplace_elsewhere_p",
    "lang_spoken_home_eng_only_m",
    "lang_spoken_home_eng_only_f",
    "lang_spoken_home_eng_only_p",
    "lang_spoken_home_oth_lang_m",
    "lang_spoken_home_oth_lang_f",
    "lang_spoken_home_oth_lang_p",
    "australian_citizen_m",
    "australian_citizen_f",
    "australian_citizen_p",
    "age_psns_att_educ_inst_0_4_m",
    "age_psns_att_educ_inst_0_4_f",
    "age_psns_att_educ_inst_0_4_p",
    "age_psns_att_educ_inst_5_14_m",
    "age_psns_att_educ_inst_5_14_f",
    "age_psns_att_educ_inst_5_14_p",
    "age_psns_att_edu_inst_15_19_m",
    "age_psns_att_edu_inst_15_19_f",
    "age_psns_att_edu_inst_15_19_p",
    "age_psns_att_edu_inst_20_24_m",
    "age_psns_att_edu_inst_20_24_f",
    "age_psns_att_edu_inst_20_24_p",
    "age_psns_att_edu_inst_25_ov_m",
    "age_psns_att_edu_inst_25_ov_f",
    "age_psns_att_edu_inst_25_ov_p",
    "high_yr_schl_comp_yr_12_eq_m",
    "high_yr_schl_comp_yr_12_eq_f",
    "high_yr_schl_comp_yr_12_eq_p",
    "high_yr_schl_comp_yr_11_eq_m",
    "high_yr_schl_comp_yr_11_eq_f",
    "high_yr_schl_comp_yr_11_eq_p",
    "high_yr_schl_comp_yr_10_eq_m",
    "high_yr_schl_comp_yr_10_eq_f",
    "high_yr_schl_comp_yr_10_eq_p",
    "high_yr_schl_comp_yr_9_eq_m",
    "high_yr_schl_comp_yr_9_eq_f",
    "high_yr_schl_comp_yr_9_eq_p",
    "high_yr_schl_comp_yr_8_belw_m",
    "high_yr_schl_comp_yr_8_belw_f",
    "high_yr_schl_comp_yr_8_belw_p",
    "high_yr_schl_comp_d_n_g_sch_m",
    "high_yr_schl_comp_d_n_g_sch_f",
    "high_yr_schl_comp_d_n_g_sch_p",
    "count_psns_occ_priv_dwgs_m",
    "count_psns_occ_priv_dwgs_f",
    "count_psns_occ_priv_dwgs_p",
    "count_persons_other_dwgs_m",
    "count_persons_other_dwgs_f",
    "count_persons_other_dwgs_p"
    ]

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
        col_names = ['date','order_id','category_id','subcategory_id','brand_id','price']

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.facts(date,order_id,category_id,subcategory_id,brand_id,price)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None      



#########################################################
#
#   DAG Operator Setup
#
#########################################################

import_load_dim_category_task = PythonOperator(
    task_id="import_load_dim_category_id",
    python_callable=import_load_dim_category_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_dim_sub_category_task = PythonOperator(
    task_id="import_load_dim_sub_category_id",
    python_callable=import_load_dim_sub_category_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_dim_brand_task = PythonOperator(
    task_id="import_load_dim_brand_id",
    python_callable=import_load_dim_brand_func,
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


[import_load_dim_category_task, import_load_dim_sub_category_task, import_load_dim_brand_task, import_load_facts_task]