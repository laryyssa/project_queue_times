from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from pathlib import Path
import sys

sys.path.insert(0, '/opt/airflow/src')

from bronze.api_queue_times import run_bronze_extraction
from silver.transform_parks_queue_time import transform_parks_queue_times
from utils.db import insert_db_data_append_only, upsert_db_data
from utils.load_parquet_data import load_parquet_data

from models.Lands import Land
from models.Rides import Ride
from models.RidesWaitTime import RideWaitTimes
from utils.db import get_engine

BRONZE_BASE_DIR = "/opt/airflow/data/bronze/parks_queue_times"

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


def create_silver_folder_path(bronze_folder_path: Path) -> Path:
    parts = list(bronze_folder_path.parts)

    try:
        bronze_index = parts.index("bronze")
    except ValueError:
        raise ValueError(
            f"'bronze' não encontrado no path: {bronze_folder_path}"
        )

    parts[bronze_index] = "silver"

    return Path(*parts)


@dag(
    dag_id="api_queue_times_dag",
    description="DAG para extrair, transformar e carregar dados dos tempos das filas",
    tags=['parks', 'bronze', 'silver'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retry_delay': timedelta(minutes=5),
        'retries': 2,
    },
    schedule='*/5 * * * *',
    start_date=datetime(2026, 7, 2),
    catchup=False,
)
def api_queue_times_dag():

    @task()
    def extract_data() -> str:
        now = datetime.now()
        timestamp = get_timestamp(now)
        bronze_folder_path = create_bronze_folder_path(now, BRONZE_BASE_DIR)

        engine = get_engine("silver")

        run_bronze_extraction(
            engine,
            output_base_dir=bronze_folder_path,
            timestamp=timestamp
        )

        return str(bronze_folder_path)

    @task()
    def transform_data(bronze_folder_path: str) -> str:
        bronze_path = Path(bronze_folder_path)
        silver_path = create_silver_folder_path(bronze_path)

        transform_parks_queue_times(
            bronze_path=bronze_path,
            silver_path=silver_path
        )

        return str(silver_path)

    @task()
    def load_lands_table(silver_folder_path: str):
        df_lands = pd.read_parquet(Path(silver_folder_path) / "lands.parquet")
        upsert_db_data(df_lands, Land, ["id"])
        return silver_folder_path

    @task()
    def load_rides_table(silver_folder_path: str):
        df_rides = pd.read_parquet(Path(silver_folder_path) / "rides.parquet")
        upsert_db_data(df_rides, Ride, ["id"])
        return silver_folder_path

    @task()
    def load_rides_wait_time(silver_folder_path: str):
        df_rides_wait_times = pd.read_parquet(
            Path(silver_folder_path) / "rides_wait_time.parquet"
        )
        df_rides_wait_times.rename(columns={"id": "ride_id"}, inplace=True)
        df_rides_wait_times = df_rides_wait_times.drop(columns=["id"], errors="ignore")

        df_rides_wait_times["wait_time"] = df_rides_wait_times["wait_time"].astype("Int64")
        df_rides_wait_times = df_rides_wait_times.where(pd.notnull(df_rides_wait_times), None)

        insert_db_data_append_only(df_rides_wait_times, RideWaitTimes)

    bronze_path = extract_data()
    silver_path = transform_data(bronze_path)

    lands_done = load_lands_table(silver_path)
    rides_done = load_rides_table(silver_path)

    lands_done >> rides_done >> load_rides_wait_time(silver_path)


dag_object = api_queue_times_dag()

if __name__ == "__main__":
    dag_object.test()