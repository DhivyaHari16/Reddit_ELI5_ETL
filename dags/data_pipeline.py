from airflow import DAG
from airflow.operators.python import PythonOperator
from python_src.Reddit_scrapper import scrape_from_reddit
from python_src.Minio_upload import upload_to_minio
from python_src.Insert_to_landing_zone import postgres_sink
from python_src.Insert_to_DWH import process_and_insert_data  
from python_src.Aggregate_insert import insert_to_aggregate 
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "reddit_scrapper_dag",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # Runs at midnight UTC
    catchup=False
)

# Task 1: Run Reddit scraper
scrape_reddit = PythonOperator(
    task_id="run_scraper",
    python_callable=scrape_from_reddit,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag,
)

# Task 2: Upload the scraped data to MinIO
upload_task = PythonOperator(
    task_id="upload_to_minio",
    python_callable=upload_to_minio,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag,
)

postgres_task = PythonOperator(
    task_id="postgres_sink",
    python_callable=postgres_sink,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag,   
)

process_data_task = PythonOperator(
    task_id='process_and_insert_data',
    python_callable=process_and_insert_data,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag,
)

aggregate_insert_task = PythonOperator(
    task_id='aggregte_insert_task',
    python_callable=insert_to_aggregate,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag,
)

# Set execution order
scrape_reddit >> upload_task >> postgres_task  >> process_data_task >> aggregate_insert_task