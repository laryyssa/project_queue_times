from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys

sys.path.insert(0, '/opt/airflow/src')

from bronze.api_queue_times import run_bronze_extraction
from silver.transform_parks_queue_time import transform_parks_queue_time
from utils.db import load_db_data, get_engine
from utils.load_parquet_data import load_parquet_data
from models import Group, Park

SILVER_PARKS_FILE_PATH = Path("/opt/airflow/data/silver") / "parks.parquet"
SILVER_GROUPS_FILE_PATH = Path("/opt/airflow/data/silver") / "groups.parquet"
BRONZE_BASE_DIR = "/opt/airflow/data/bronze/parks_queue_times"


@dag(
    dag_id="api_queue_times_dag",
    description="DAG para extrair, transformar e carregar dados dos tempost das filas",
    tags=['parks', 'bronze', 'silver'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retry_delay': timedelta(minutes=5),
        'retries': 2,
    },
    schedule='0 */1 * * *',
    start_date=datetime(2026, 7, 2),
    catchup=False,
)
def api_queue_times_dag():


    @task()
    def extract_data():   
        engine = get_engine("silver")
        bronze_files = run_bronze_extraction(engine, output_base_dir=BRONZE_BASE_DIR)
        return bronze_files



    extract_data()

dag_object = api_queue_times_dag()

if __name__ == "__main__":
    dag_object.test()