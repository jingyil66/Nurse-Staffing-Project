from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
IAM_ROLE_ARN = Variable.get("IAM_ROLE_ARN")

create_tables = """
        CREATE TABLE IF NOT EXISTS public.staff_raw (
            PROVNUM VARCHAR(6),
            PROVNAME TEXT,
            CITY TEXT,
            STATE VARCHAR(2),
            COUNTY_NAME TEXT,
            COUNTY_FIPS BIGINT,
            CY_Qtr VARCHAR(6),
            WorkDate TIMESTAMP,
            MDScensus BIGINT,
            Hrs_RNDON FLOAT8,
            Hrs_RNDON_emp FLOAT8,
            Hrs_RNDON_ctr FLOAT8,
            Hrs_RNadmin FLOAT8,
            Hrs_RNadmin_emp FLOAT8,
            Hrs_RNadmin_ctr FLOAT8,
            Hrs_RN FLOAT8,
            Hrs_RN_emp FLOAT8,
            Hrs_RN_ctr FLOAT8,
            Hrs_LPNadmin FLOAT8,
            Hrs_LPNadmin_emp FLOAT8,
            Hrs_LPNadmin_ctr FLOAT8,
            Hrs_LPN FLOAT8,
            Hrs_LPN_emp FLOAT8,
            Hrs_LPN_ctr FLOAT8,
            Hrs_CNA FLOAT8,
            Hrs_CNA_emp FLOAT8,
            Hrs_CNA_ctr FLOAT8,
            Hrs_NAtrn FLOAT8,
            Hrs_NAtrn_emp FLOAT8,
            Hrs_NAtrn_ctr FLOAT8,
            Hrs_MedAide FLOAT8,
            Hrs_MedAide_emp FLOAT8,
            Hrs_MedAide_ctr FLOAT8
    );
"""

copy_sql = """
    COPY public.staff_raw
    FROM 's3://daily-nurse-staffing-data/'
    IAM_ROLE '{IAM_ROLE_ARN}'
    FORMAT AS PARQUET
    ;
"""

transform_tables = """
    CREATE TABLE public.providers AS
    SELECT DISTINCT
        PROVNUM,
        PROVNAME,
        CITY,
        STATE,
        COUNTY_NAME,
        COUNTY_FIPS
    FROM public.staff_raw;

    CREATE TABLE public.resident AS
        SELECT DISTINCT
            PROVNUM,
            WorkDate,
            MDScensus
        FROM
            public.staff_raw;

    CREATE TABLE public.staff_hours AS
        WITH unpivoted AS (
            SELECT
                PROVNUM,
                WorkDate,
                Role,
                Hour,
                CASE 
                    WHEN Role LIKE '%_ctr' THEN 'ctr' 
                    WHEN Role LIKE '%_emp' THEN 'emp' 
                    ELSE 'All' 
                END AS Employment
            FROM
                (SELECT
                    PROVNUM, 
                    WorkDate,
                    Hrs_RNDON,
                    Hrs_RNadmin,
                    Hrs_RN,
                    Hrs_LPNadmin,
                    Hrs_LPN,
                    Hrs_CNA,
                    Hrs_NAtrn,
                    Hrs_MedAide,
                    Hrs_RNDON_ctr,
                    Hrs_RNadmin_ctr,
                    Hrs_RN_ctr,
                    Hrs_LPNadmin_ctr,
                    Hrs_LPN_ctr,
                    Hrs_CNA_ctr,
                    Hrs_NAtrn_ctr,
                    Hrs_MedAide_ctr,
                    Hrs_RNDON_emp,
                    Hrs_RNadmin_emp,
                    Hrs_RN_emp,
                    Hrs_LPNadmin_emp,
                    Hrs_LPN_emp,
                    Hrs_CNA_emp,
                    Hrs_NAtrn_emp,
                    Hrs_MedAide_emp
                FROM public.staff_raw)
            UNPIVOT
            (Hour FOR Role IN (
                Hrs_RNDON,
                Hrs_RNadmin,
                Hrs_RN,
                Hrs_LPNadmin,
                Hrs_LPN,
                Hrs_CNA,
                Hrs_NAtrn,
                Hrs_MedAide,
                Hrs_RNDON_ctr,
                Hrs_RNadmin_ctr,
                Hrs_RN_ctr,
                Hrs_LPNadmin_ctr,
                Hrs_LPN_ctr,
                Hrs_CNA_ctr,
                Hrs_NAtrn_ctr,
                Hrs_MedAide_ctr,
                Hrs_RNDON_emp,
                Hrs_RNadmin_emp,
                Hrs_RN_emp,
                Hrs_LPNadmin_emp,
                Hrs_LPN_emp,
                Hrs_CNA_emp,
                Hrs_NAtrn_emp,
                Hrs_MedAide_emp
            ))
        )
        SELECT 
            PROVNUM,
            WorkDate,
            REPLACE(REPLACE(REPLACE(Role, '_ctr', ''), '_emp', ''), 'hrs_', '') AS Role,
            Employment,
            Hour
        FROM unpivoted;

    CREATE TABLE public.date AS
    SELECT DISTINCT
        WorkDate,
        CY_Qtr
    FROM
        public.staff_raw

"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('s3_to_redshift',
        default_args=default_args,
        schedule_interval = None,
        catchup=False) as dag:


        create_tables = PostgresOperator(
            task_id="Create_tables",
            dag=dag,
            postgres_conn_id="redshift",
            sql=create_tables
        )


        load_staff_data = PostgresOperator(
            task_id="load_staff_data",
            dag=dag,
            postgres_conn_id="redshift",
            sql=copy_sql
        )

        transform_tables = PostgresOperator(
            task_id="transform_tables",
            dag=dag,
            postgres_conn_id="redshift",
            sql=transform_tables
        )

create_tables >> load_staff_data >> transform_tables