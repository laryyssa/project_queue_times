from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv(dotenv_path="/opt/airflow/.env")

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

def get_engine():
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")


def check_table_exists(engine:str, table_name: str, schema: str) -> bool:
    query = text(f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables 
            WHERE table_schema = :schema
            AND table_name = :table_name
        );
    """)
    result = engine.execute(query, {"schema": schema, "table_name": table_name}).scalar()
    return result

def load_db_data(parquet_path, model):
    engine = get_engine()
    table_name = model.__tablename__
    schema = model.__table__.schema

    df = pd.read_parquet(parquet_path)

    try:
        df.to_sql(
            table_name, 
            engine,
            schema, 
            if_exists='append', 
            index=False
        )
        logging.info(f"Dados carregados com sucesso na tabela: {table_name}")

        if check_table_exists(engine, table_name, schema):
            df_check = pd.read_sql_query(text(f"SELECT count(*) FROM {table_name}"), engine)
            logging.info(f"Total de registros na tabela {table_name}: {df_check.iloc[0, 0]}")

    except Exception as e:
        logging.error(f"Erro ao carregar dados na tabela: {table_name}. Detalhes do erro: {e}")
        raise