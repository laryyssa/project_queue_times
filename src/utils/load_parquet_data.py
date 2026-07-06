import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_parquet_data(df: pd.DataFrame, output_path: str) -> None:
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logging.info(f"DataFrame salvo com sucesso em formato Parquet no caminho: {output_path}")
    except Exception as e:
        logging.error(f"Erro ao salvar DataFrame em formato Parquet no caminho: {output_path}. Detalhes do erro: {e}")
        raise