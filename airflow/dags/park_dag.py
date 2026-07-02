from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys, os

sys.path.insert(0, '/opt/airflow/src')

from extract_data import extract_data
from transform_data import transform_data
from load_data import load_data

BRONZE_FILE_PATH = Path("/opt/airflow/data") / "parks.json"

@dag(
    dag_id="park_dag",
    description="DAG para extrair, transformar e carregar dados de parques",
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retry_delay': timedelta(minutes=5),
        'retries': 2,     
    },
    schedule='0 */1 * * *',
    start_date=datetime(2026, 7, 2),
    catchup=False,
    tags=['park', 'etl', 'data_pipeline']
)
def park_dag():

    @task()
    def extract_task():
        url = "https://queue-times.com/parks.json"
        output_path = BRONZE_FILE_PATH
        
        extract_data(output_path, url)

    @task()
    def transform_data_task():
        input_file = BRONZE_FILE_PATH
        parquet_path = Path("/opt/airflow/data") / "parks.parquet"

        df = transform_data(input_file)
        df.to_parquet(parquet_path, index=False)

    @task()
    def load_task():
        import pandas as pd
        parquet_path = Path("/opt/airflow/data") / "parks.parquet"
        
        df = pd.read_parquet(parquet_path)
        load_data("parks", df)

    
    # extract_task() >> transform_data_task() >> load_task()
    load_task()

park_dag()