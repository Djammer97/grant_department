from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.session import create_session
from airflow.exceptions import AirflowNotFoundException

import os
import requests
import json

with open('/opt/airflow/dags/variables.json') as file:
    variables = json.load(file) 

def create_connction(*args, **kwargs):
    try:
        BaseHook.get_connection(kwargs['conn_id'])
    except AirflowNotFoundException:
        new_conn = Connection(
            conn_id=kwargs['conn_id'],
            conn_type=kwargs['conn_type'],
            host=kwargs['host'],
            login=kwargs['login'],
            password=kwargs['password'],
            port=kwargs['port'],
            schema=kwargs['schema']
        )
        with create_session() as session:
            session.add(new_conn)
            session.commit()

def registration_on_metabase(*args, **kwargs):
    response = requests.get('http://metabase:3000/api/session/properties')
    token = response.json()['setup-token']
    print(token)

    data = {
        "token":token,
        "user":
            {
                "password_confirm": variables['metabase']['password'],
                "password": variables['metabase']['password'],
                "site_name": variables['metabase']['site_name'],
                "email": variables['metabase']['email'],
                "last_name": variables['metabase']['last_name'],
                "first_name": variables['metabase']['first_name']
            },
        "prefs":
            {
                "site_name": variables['metabase']['site_name'],
                "site_locale": variables['metabase']['site_locale']
            }
        }

    requests.post('http://metabase:3000/api/setup', json=data)

default_args = {
    'owner': 'Djammer',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(year=2000, month=1, day=1, hour=9, minute=0, second=0)
}

airflow_dag = DAG(
    dag_id='init_airflow', 
    schedule_interval=None,
    catchup=False, 
    default_args=default_args
)

metabase_dag = DAG(
    dag_id='init_metabase', 
    schedule_interval=None,
    catchup=False, 
    default_args=default_args
)

postgres_dag = DAG(
    dag_id='init_postgres', 
    schedule_interval=None,
    catchup=False,
    default_args=default_args
)

postgres_connection_create = PythonOperator(
    task_id = 'postgres_connection_create',
    python_callable=create_connction,
    dag=airflow_dag,
    op_kwargs={
        'conn_id': variables['postgres']['conn_id'],
        'conn_type': variables['postgres']['conn_type'],
        'host': variables['postgres']['host'],
        'login': variables['postgres']['login'],
        'password': variables['postgres']['password'],
        'port': variables['postgres']['port'],
        'schema': variables['postgres']['schema']
    }
)

metabase_connection_create = PythonOperator(
    task_id = 'metabase_connection_create',
    python_callable=create_connction,
    dag=airflow_dag,
    op_kwargs={
        'conn_id': variables['metabase']['conn_id'],
        'conn_type': variables['metabase']['conn_type'],
        'host': variables['metabase']['host'],
        'login': variables['metabase']['email'],
        'password': variables['metabase']['password'],
        'port': variables['metabase']['port'],
        'schema': '',
    }
)

create_schemas = SQLExecuteQueryOperator(
    task_id='create_schemas',
    conn_id=variables['postgres']['conn_id'],
    sql=open('/opt/airflow/sql/create_schemas.sql').read(),
    dag=postgres_dag
)

with TaskGroup(group_id='create_layers', dag=postgres_dag) as create_layers:
    stg = SQLExecuteQueryOperator(
        task_id='stg',
        conn_id=variables['postgres']['conn_id'],
        sql=open('/opt/airflow/sql/create_stg_tables.sql').read(),
        dag=postgres_dag
    )

    dds = SQLExecuteQueryOperator(
        task_id='dds',
        conn_id=variables['postgres']['conn_id'],
        sql=open('/opt/airflow/sql/create_dds_tables.sql').read(),
        dag=postgres_dag
    )

    cdm = SQLExecuteQueryOperator(
        task_id='cdm',
        conn_id=variables['postgres']['conn_id'],
        sql=open('/opt/airflow/sql/create_cdm_tables.sql').read(),
        dag=postgres_dag
    )

    stg >> dds >> cdm

metabase_registration = PythonOperator(
    task_id='metabase_registration',
    python_callable=registration_on_metabase,
    dag=metabase_dag
)

postgres_connection_create 
create_schemas >> create_layers 
metabase_registration