import airflow
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime
from psycopg2.extras import execute_values

#default arguments

default_args = {
    'owner': 'grisell.reyes',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 1),
    'email': ['grisell.reyes@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('insert_data_postgres',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table.
    with open('/Users/grisell.reyes/data-bootcamp-terraforms/kubernetes/username.csv', 'r') as f:
        next(f)
        curr.copy_from(f, 'username_table', sep=',')
        get_postgres_conn.commit()


task1 = PostgresOperator(task_id = 'create_table',
                        sql="""
                        CREATE TABLE IF NOT EXISTS username (    
                            Username VARCHAR,
                            Identifier INTEGER,
                            first_name VARCHAR,
                            last_name VARCHAR);
                            """,
                            postgres_conn_id= 'postgres_default',
                            autocommit=True,
                            dag= dag)

task2 = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)


task1 >> task2