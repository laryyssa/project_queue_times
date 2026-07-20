from datetime import datetime
from pathlib import Path


def create_bronze_file_path() -> Path:
    base_path = Path("/opt/airflow/data/bronze/parks_queue_times")

    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

    partitioned_dir = base_path / f"{now.year}/{now.month:02d}/{now.day:02d}"
    partitioned_dir.mkdir(parents=True, exist_ok=True)

    return partitioned_dir / f"bronze_{timestamp}.parquet"