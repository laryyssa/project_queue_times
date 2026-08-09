import json
import logging
from pathlib import Path
import pandas as pd
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def extract_park_id_from_filename(file_path: Path) -> str:
    parts = file_path.stem.split("_")

    if len(parts) >= 3 and parts[0] == "bronze" and parts[1].isdigit():
        return parts[1]

    logger.warning(
        f"Nome de arquivo fora do padrão esperado, usando stem completo: {file_path.name}"
    )
    return file_path.stem


def read_bronze_file(file_path: Path) -> Optional[dict]:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
        
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Falha ao ler {file_path}: {e}")

        return None


def _build_ride_row(ride: dict, park_id: str, land_id: Optional[str] = None) -> dict:
    row = {
        "id": ride["id"],
        "name": ride["name"],
        "is_open": ride["is_open"],
        "wait_time": ride["wait_time"],
        "last_updated": ride["last_updated"],
        "park_id": park_id,
    }
    if land_id is not None:
        row["land_id"] = land_id
    return row


def _transform_lands_table(data: list) -> pd.DataFrame:
    df = pd.DataFrame(data)
    return df

def _transform_rides_and_rides_wait_time_table(data: list) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.DataFrame(data)

    columns_table_rides = ["id", "name", "park_id", "land_id"]
    df_rides = df[columns_table_rides].copy()
    df_rides["land_id"] = df_rides["land_id"].astype("Int64")
    df_rides = df_rides.drop_duplicates(subset=["id"])

    columns_table_rides_wait_time = ["id", "is_open", "wait_time", "last_updated"]
    df_rides_wait_time = df[columns_table_rides_wait_time].copy()

    return df_rides, df_rides_wait_time

def build_land_and_ride_dfs(bronze_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    json_files = sorted(bronze_path.rglob("*.json"))

    if not json_files:
        logger.warning(f"Nenhum arquivo .json encontrado em: {bronze_path}")
        return pd.DataFrame(columns=["id", "name"]), pd.DataFrame()

    land_rows = []
    ride_rows = []

    for file_path in json_files:
        park_id = extract_park_id_from_filename(file_path)

        data = read_bronze_file(file_path)
        if data is None:
            continue

        for land in data.get("lands") or []:
            land_rows.append({
                "id": land["id"],
                "name": land["name"],
                "park_id": park_id,
            })

            for ride in land.get("rides") or []:
                ride_rows.append(_build_ride_row(ride, park_id, land_id=land["id"]))

        for ride in data.get("rides") or []:
            ride_rows.append(_build_ride_row(ride, park_id))

    df_land = _transform_lands_table(land_rows)
    (
        df_rides, 
        df_rides_wait_time
    ) = _transform_rides_and_rides_wait_time_table(ride_rows)

    logger.info(
        f"{len(json_files)} arquivos lidos | df_land: {len(df_land)} linhas | df_ride: {len(df_rides)} linhas | df_rides_wait_time: {len(df_rides_wait_time)} linhas"
    )

    return df_land, df_rides, df_rides_wait_time

def save_dataframe_to_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info(f"DataFrame salvo em: {output_path}")

def transform_parks_queue_times(bronze_path: Path, silver_path: Path) -> None:

    df_land, df_rides, df_rides_wait_time = build_land_and_ride_dfs(bronze_path)

    save_dataframe_to_parquet(df_land, Path(f"{silver_path}/lands.parquet"))
    save_dataframe_to_parquet(df_rides, Path(f"{silver_path}/rides.parquet"))
    save_dataframe_to_parquet(df_rides_wait_time, Path(f"{silver_path}/rides_wait_time.parquet"))
