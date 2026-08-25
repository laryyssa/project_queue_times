from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
from pathlib import Path
import sys

sys.path.insert(0, '/opt/airflow/src')

from bronze.api_queue_times import run_bronze_extraction
from silver.transform_parks_queue_time import transform_parks_queue_times
from utils.db import upsert_db_data
from utils.load_parquet_data import load_parquet_data

from models.Lands import Land
from models.Rides import Ride
# from models.RidesWaitTimes import RidesWaitTime

BRONZE_BASE_DIR = "/opt/airflow/data/bronze/parks_queue_times"

BRONZE_API_PARKS_FILE_PATH = Path("/opt/airflow/data/bronze") / "api_parks_data.json"
SILVER_PARKS_FILE_PATH = Path("/opt/airflow/data/silver") / "parks.parquet"
SILVER_GROUPS_FILE_PATH = Path("/opt/airflow/data/silver") / "groups.parquet"


def create_bronze_folder_path(now: datetime, base_dir: str) -> Path:
    date_path = now.strftime("%Y/%m/%d/%H-%M")

    bronze_folder_path = Path(base_dir) / date_path
    bronze_folder_path.mkdir(parents=True, exist_ok=True)

    return Path(bronze_folder_path)

def get_timestamp(now: datetime) -> str:
    timestamp = now.strftime("%Y%m%d%H%M%S")
    return timestamp


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

    now = datetime.now()
    bronze_folder_path = create_bronze_folder_path(now, BRONZE_BASE_DIR)
    timestamp = get_timestamp(now)

    bronze_folder_path = Path("/opt/airflow/data/bronze/parks_queue_times/2026/08/08/20-34")
    silver_folder_path = Path("/opt/airflow/data/silver/parks_queue_times/2026/08/08/20-34")

    # @task()
    # def extract_data():   
    #     engine = get_engine("silver")

    #     run_bronze_extraction(
    #         engine, 
    #         output_base_dir=bronze_folder_path, 
    #         timestamp=timestamp
    #     )

    # @task()
    # def transform_data():
    #     transform_parks_queue_times( 
    #         bronze_path=bronze_folder_path,
    #         silver_path=silver_folder_path
    #     )

    @task()
    def load_lands_table():
        df_lands = pd.read_parquet(silver_folder_path / "lands.parquet")

        upsert_db_data(df_lands, Land, ["id"])

    @task()
    def load_rides_table():
        df_rides = pd.read_parquet(silver_folder_path / "rides.parquet")

        upsert_db_data(df_rides, Ride, ["id"])

    # @task()
    # def load_rides_wait_times():
    #     df_rides_wait_times = pd.read_parquet(silver_folder_path / "rides_wait_times.parquet")

    #     upsert_db_data(df_rides_wait_times, RidesWaitTime, ["id"])

    (
        # extract_data() >>
        # transform_data() >>
        load_lands_table() >>
        load_rides_table()
    )

dag_object = api_queue_times_dag()

if __name__ == "__main__":
    dag_object.test()