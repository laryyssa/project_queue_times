import pandas as pd
import json
# from typing import Tuple

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

RENAME_COLUMNS_DICT = {
    "id": "group_id",
    "name": "group_name",
    "parks.id": "park_id", 
    "parks.name": "park_name",
    "parks.country": "park_country",
    "parks.continent": "park_continent",
    "parks.latitude": "park_latitude",
    "parks.longitude": "park_longitude",
    "parks.timezone": "park_timezone"
}


def create_dataframe(path_name:str) -> pd.DataFrame:

    if not path_name:
        logging.error(f"Erro: O arquivo {path_name} não existe.")
        return None
    
    with open(path_name, 'r') as f:
        data = json.load(f)

    df = pd.json_normalize(data)
    return df

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:    
    df_normalized = (
        df
        .pipe(lambda d: d.explode("parks").reset_index(drop=True))
        .pipe(lambda d: pd.concat([
            d.drop(columns=["parks"]),
            pd.json_normalize(d["parks"]).add_prefix("parks.")
        ], axis=1))
    )

    logging.info("DataFrame normalizado com sucesso.")

    return df_normalized

def rename_columns(df: pd.DataFrame, rename_dict: dict) -> pd.DataFrame:
    
    df_renamed = df.rename(columns=rename_dict)

    logging.info("Colunas renomeadas com sucesso.")
    return df_renamed

def get_groups_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df_groups = df[["group_id", "group_name"]].drop_duplicates().reset_index(drop=True)
    
    logging.info("DataFrame de grupos criado com sucesso.")
    return df_groups

def get_parks_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df_parks = df[["park_id", "park_name", "park_country", "park_continent", "park_latitude", "park_longitude", "park_timezone"]].drop_duplicates().reset_index(drop=True)
    
    logging.info("DataFrame de parques criado com sucesso.")
    return df_parks

def transform_groups_and_parks(input_path: str): 

    logging.info(f"Iniciando a transformação de dados do arquivo: {input_path}")

    df = create_dataframe(input_path)
    
    if df is None:
        logging.warning(f"Erro: Não foi possível criar o DataFrame a partir do arquivo: {input_path}")
        return None
    
    df = normalize_dataframe(df)
    df = rename_columns(df, rename_dict=RENAME_COLUMNS_DICT)

    logging.info("Transformação de dados concluída com sucesso.")
    
    df_groups = get_groups_dataframe(df)
    df_parks = get_parks_dataframe(df)

    return df_groups, df_parks

