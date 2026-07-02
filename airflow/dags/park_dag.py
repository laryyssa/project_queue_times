from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys, os

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
        print("Hello from extract_task!")


        # url = "https://queue-times.com/parks.json"
        # output_file = "../../data/parks.json"

        # extract_data(url, output_file)

    # @task()
    # def transform_task():
    #     df = transform_data(output_file)
    #     df.to_parquet(Path(__file__).parent / "data" / "parks.parquet", index=False)

    # @task()
    # def load_task():
    #     import pandas as pd
    #     df = pd.read_parquet(Path(__file__).parent / "data" / "parks.parquet", index=False)
    #     load_data(table_name, df)

    # extract_task() >> transform_task() >> load_task()
    
    extract_task()

park_dag()