from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys, os

sys.path.insert(0, '/opt/airflow/src')

from bronze.data_from_api import extract_and_load_data
from silver.transform_parks import transform_groups_and_parks
from utils.db import load_db_data
from utils.load_parquet_data import load_parquet_data
from models import Group, Park

BRONZE_PARKS_FILE_PATH = Path("/opt/airflow/data/bronze") / "parks.json"
SILVER_PARKS_FILE_PATH = Path("/opt/airflow/data/silver") / "parks.parquet"
SILVER_GROUPS_FILE_PATH = Path("/opt/airflow/data/silver") / "groups.parquet"

@dag(
    dag_id="api_parks_dag",
    description="DAG para extrair, transformar e carregar dados de parques",
    tags=['parks', 'bronze'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retry_delay': timedelta(minutes=5),
        'retries': 2,     
    },
    schedule='0 */1 * * *',
    start_date=datetime(2026, 7, 2),
    catchup=False
)
def api_parks_dag():

    @task()
    def extract_data():    
        api_url = os.getenv("URL_PARKS")

        if not api_url:
            raise ValueError("API_URL não está definida no ambiente.") 
        
        extract_and_load_data(
            output_path=BRONZE_PARKS_FILE_PATH, 
            url=api_url
        )

    @task()
    def transform_data():
        df_groups, df_parks = transform_groups_and_parks(
            input_path=BRONZE_PARKS_FILE_PATH,
        )

        load_parquet_data(df_groups, SILVER_GROUPS_FILE_PATH)
        load_parquet_data(df_parks, SILVER_PARKS_FILE_PATH)
        
    @task
    def load_groups_table():
        load_db_data(SILVER_GROUPS_FILE_PATH, Group)

    @task
    def load_parks_table():
        load_db_data(SILVER_PARKS_FILE_PATH, Park)


    extract_data() >> transform_data() >> [
        load_groups_table(), 
        load_parks_table()
    ]

dag_object = api_parks_dag()

if __name__ == "__main__":
    dag_object.test()