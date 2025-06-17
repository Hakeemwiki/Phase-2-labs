
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta # Import timedelta
from extraction import extract_data
from transformation import transform_data
from loading import load_data # This assumes load_data is the function name in your loading.py

default_args = {
    'owner': 'hakeem',
    'start_date': datetime(2025, 6, 10),
    'retries': 1
}

with DAG(
    dag_id='music_streaming_etl',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5), # Changed schedule to every 5 minutes
    catchup=False,
    description='ETL DAG for music streaming analytics, running every 5 minutes',
    tags=['music', 'ETL', 'Redshift', 'hourly'],
    max_active_runs=1  # Ensure only one active run at a time
) as dag:

    t1 = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={'bucket': 'my-etl-bucket-20250611'}
    )

    t2 = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'bucket': 'my-etl-bucket-20250611'}
    )

    t3 = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    # Define task dependencies
    t1 >> t2 >> t3
