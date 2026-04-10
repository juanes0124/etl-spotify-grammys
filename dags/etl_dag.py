from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow')

from scripts.extract import extract_spotify, extract_grammy
from scripts.transform import transform_spotify, transform_grammy, merge_datasets
from scripts.load import load_to_warehouse, load_to_csv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_spotify_grammy',
    default_args=default_args,
    description='ETL pipeline para Spotify y Grammy Awards',
    schedule_interval='@once',
    catchup=False,
)

# ── Funciones wrapper para XCom ──────────────────────────────

def task_extract_spotify(**context):
    df = extract_spotify()
    context['ti'].xcom_push(key='spotify_raw', value=df.to_json())

def task_extract_grammy(**context):
    df = extract_grammy()
    context['ti'].xcom_push(key='grammy_raw', value=df.to_json())

def task_transform_spotify(**context):
    import pandas as pd
    json_data = context['ti'].xcom_pull(key='spotify_raw', task_ids='extract_spotify')
    df = pd.read_json(json_data)
    df = transform_spotify(df)
    context['ti'].xcom_push(key='spotify_clean', value=df.to_json())

def task_transform_grammy(**context):
    import pandas as pd
    json_data = context['ti'].xcom_pull(key='grammy_raw', task_ids='extract_grammy')
    df = pd.read_json(json_data)
    df = transform_grammy(df)
    context['ti'].xcom_push(key='grammy_clean', value=df.to_json())

def task_merge(**context):
    import pandas as pd
    spotify_json = context['ti'].xcom_pull(key='spotify_clean', task_ids='transform_spotify')
    grammy_json = context['ti'].xcom_pull(key='grammy_clean', task_ids='transform_grammy')
    spotify_df = pd.read_json(spotify_json)
    grammy_df = pd.read_json(grammy_json)
    merged = merge_datasets(spotify_df, grammy_df)
    context['ti'].xcom_push(key='merged', value=merged.to_json())

def task_load(**context):
    import pandas as pd
    json_data = context['ti'].xcom_pull(key='merged', task_ids='merge')
    df = pd.read_json(json_data)
    load_to_warehouse(df)

def task_store(**context):
    import pandas as pd
    json_data = context['ti'].xcom_pull(key='merged', task_ids='merge')
    df = pd.read_json(json_data)
    load_to_csv(df)

# ── Definición de tareas ─────────────────────────────────────

extract_spotify_task = PythonOperator(
    task_id='extract_spotify',
    python_callable=task_extract_spotify,
    dag=dag,
)

extract_grammy_task = PythonOperator(
    task_id='extract_grammy',
    python_callable=task_extract_grammy,
    dag=dag,
)

transform_spotify_task = PythonOperator(
    task_id='transform_spotify',
    python_callable=task_transform_spotify,
    dag=dag,
)

transform_grammy_task = PythonOperator(
    task_id='transform_grammy',
    python_callable=task_transform_grammy,
    dag=dag,
)

merge_task = PythonOperator(
    task_id='merge',
    python_callable=task_merge,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=task_load,
    dag=dag,
)

store_task = PythonOperator(
    task_id='store',
    python_callable=task_store,
    dag=dag,
)

# ── Dependencias ─────────────────────────────────────────────

extract_spotify_task >> transform_spotify_task
extract_grammy_task >> transform_grammy_task
[transform_spotify_task, transform_grammy_task] >> merge_task
merge_task >> [load_task, store_task]