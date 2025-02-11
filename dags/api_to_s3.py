from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import requests
import io
import json
from airflow.utils.dates import days_ago
import numpy as np

from airflow.models import Variable

S3_BUCKET = Variable.get("S3_BUCKET")
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")

def api_to_s3():
    float_columns = [
        'Hrs_RNDON', 'Hrs_RNDON_emp', 'Hrs_RNDON_ctr',
        'Hrs_RNadmin', 'Hrs_RNadmin_emp', 'Hrs_RNadmin_ctr',
        'Hrs_RN', 'Hrs_RN_emp', 'Hrs_RN_ctr',
        'Hrs_LPNadmin', 'Hrs_LPNadmin_emp', 'Hrs_LPNadmin_ctr',
        'Hrs_LPN', 'Hrs_LPN_emp', 'Hrs_LPN_ctr',
        'Hrs_CNA', 'Hrs_CNA_emp', 'Hrs_CNA_ctr',
        'Hrs_NAtrn', 'Hrs_NAtrn_emp', 'Hrs_NAtrn_ctr',
        'Hrs_MedAide', 'Hrs_MedAide_emp', 'Hrs_MedAide_ctr'
    ]
    int_columns = ['COUNTY_FIPS', 'MDScensus']
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    size = 1000
    offset = 0
    while True:
        url = f"https://data.cms.gov/data-api/v1/dataset/dcc467d8-5792-4e5d-95be-04bf9fc930a1/data?size={size}&offset={offset}"

        response = requests.get(url)

        data = json.loads(response.text)
            
        df = pd.DataFrame(data)
    
        df['PROVNUM'] = df['PROVNUM'].astype(str)
        df[float_columns] = df[float_columns].apply(pd.to_numeric, errors='coerce').fillna(0).astype(float)
        df[int_columns] = df[int_columns].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
        df['WorkDate'] = df['WorkDate'].astype('datetime64[ms]') 

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        S3_KEY = f"staffing_data_{offset}.parquet"
        s3.upload_fileobj(parquet_buffer, S3_BUCKET, S3_KEY)

        length = len(data)
        print(f"The {offset}th-{offset+length}th row of data is stored in S3({S3_KEY}).")
        offset += length
        if len(data) < size:
            print('Finished.')
            break

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'api_to_s3',
    default_args=default_args,
    description='Extract staffing data from API, convert to Parquet and store in S3.',
    schedule_interval=None,
    catchup=False,
)

api_to_s3_task = PythonOperator(
    task_id='api_to_s3',
    python_callable=api_to_s3,
    dag=dag
)

api_to_s3_task
