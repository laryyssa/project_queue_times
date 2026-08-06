import pandas as pd
import json

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_dataframe(path_name:str) -> pd.DataFrame:

    if not path_name:
        logging.error(f"Erro: O arquivo {path_name} não existe.")
        return None
    
    with open(path_name, 'r') as f:
        data = json.load(f)

    df = pd.json_normalize(data)
    return df


def transform_parks_queue_time(input_path):

    logging.info(f"Iniciando a transformação de dados do arquivo: {input_path}")

    df = create_dataframe(input_path)
    
    if df is None:
        logging.warning(f"Erro: Não foi possível criar o DataFrame a partir do arquivo: {input_path}")
        return None
    