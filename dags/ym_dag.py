from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago

#from includes.common.common import get_response
#from includes.vs_modules.funcs i
import includes.common.common #as cmn
import includes.vs_modules.funcs as fncs

import requests
import json
import pandas as pd
import sys 
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import io
import os
from dotenv import load_dotenv

def create_dag(dag_id, metric_id):

    default_args = {
        'owner': 'Diana Solovei',
        'depends_on_past': False,
        'start_date': datetime(2023, 12, 20),
        'email_on_failure': 'tkocatcher@tko-inform.ru',
        'email_on_retry': 'tkocatcher@tko-inform.ru',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
    
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval = '0 0 * * *' # or '@daily' # make this workflow happen every day
    )
    
    with dag:
        create_request = PythonOperator(
            task_id='create_request_to_load_data',
            python_callable=fncs.create_request_to_load_data,
            provide_context=True,
            op_kwargs = {'metric_id': metric_id}
        )
        check_status = fncs.YMDataReadySensor(
            task_id='get_load_data_status',
            metric_id = metric_id,
            poke_interval = 5, #5 sec
            timeout = 1200 #20 min 
        )
        get_data = PythonOperator(
            task_id=f'extract_data',
            python_callable=fncs.extract_data,
            provide_context=True,
            op_kwargs = {'metric_id': metric_id}
        )
        load_data = PythonOperator(
            task_id='load_data_to_database',
            python_callable=fncs.load_data_to_database,
            provide_context=True
        )

        create_request >> check_status
        check_status >> get_data >> load_data

    return dag



######################################

load_dotenv()

with open('/opt/airflow/dags/METRIC_IDS_LIST.json', 'r') as file:
    metric_ids_list = json.load(file)

# Динамическое создание DAGов
for instance in metric_ids_list:
    print('PRINT instance: ', instance)
    val = metric_ids_list[instance]
    print('PRINT metric_id: ', val)
    
    instance_name = instance.replace('/', '-')
    dag_id = f'ym_to_ch_{instance_name}_{val}'
    globals()[dag_id] = create_dag(dag_id, val)

