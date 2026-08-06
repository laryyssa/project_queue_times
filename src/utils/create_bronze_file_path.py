from datetime import datetime
from pathlib import Path
import pendulum


def create_bronze_file_path() -> Path:
    base_path = Path("/opt/airflow/data/bronze/parks_queue_times")

    now = pendulum.now("America/Sao_Paulo")
    timestamp = now.format("YYYY-MM-DD_HH-mm-ss")

    partitioned_dir = base_path / f"{now.year}/{now.month:02d}/{now.day:02d}"
    partitioned_dir.mkdir(parents=True, exist_ok=True)

    return partitioned_dir / f"parks_queue_{timestamp}.json"