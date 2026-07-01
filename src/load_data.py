from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path(__file__).parent.parent / "config" / ".env"
load_dotenv(dotenv_path=env_path)

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

def get_engine():
    return create_engine(f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:{port}/{database}")


engine = get_engine()

def load_data(table_name: str, df):
    try:
        df.to_sql(
            table_name, 
            engine, 
            if_exists='replace', 
            index=False
        )
        logging.info(f"Dados carregados com sucesso na tabela: {table_name}")

        df_check = pd.read_sql_query(text(f"SELECT count(*) FROM {table_name}"), engine)
        logging.info(f"Total de registros na tabela {table_name}: {df_check.iloc[0, 0]}")

    except Exception as e:
        logging.error(f"Erro ao carregar dados na tabela: {table_name}. Detalhes do erro: {e}")