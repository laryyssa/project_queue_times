import pandas as pd
import pathlib as Path
import json

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

RENAME_COLUMNS_DICT = {
    "id": "group_id",
    "name": "group_name",
    "parks.name": "park_name",
    "parks.location": "park_location",
    "parks.type": "park_type",
    "parks.area": "park_area",
    "parks.visitors": "park_visitors"
}

def create_dataframe(path_name:str) -> pd.DataFrame:
    path = Path(path_name)
    
    if not path.exists():
        logging.error(f"Erro: O arquivo {path_name} não existe.")
        return None
    
    with open(path, 'r') as f:
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

def transform_data(path_name: str) -> pd.DataFrame:
    logging.info(f"Iniciando a transformação de dados do arquivo: {path_name}")

    df = create_dataframe(path_name)
    if df is None:
        logging.warning(f"Erro: Não foi possível criar o DataFrame a partir do arquivo: {path_name}")
        return None
    
    df = normalize_dataframe(df)
    df = rename_columns(df, rename_dict=RENAME_COLUMNS_DICT)

    logging.info("Transformação de dados concluída com sucesso.")
    return df