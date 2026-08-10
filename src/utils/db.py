from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
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
db_silver = os.getenv("DB_NAME_SILVER")
db_gold = os.getenv("DB_NAME_GOLD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

def get_db_name(db_name_str):
    db_map = {
        "silver": db_silver, 
        "gold": db_gold
    }
    return db_map.get(db_name_str)


def get_engine(db_name):
    db_name = get_db_name(db_name)

    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}")


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
    model_db = model.db_name

    engine = get_engine(model_db)
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

def get_db_data(query: str, engine) -> pd.DataFrame:
    df = pd.read_sql_query(text(query), engine)
    return df

def upsert_db_data(df: pd.DataFrame, model, unique_columns: list):
    if df.empty:
        return

    engine = get_engine(model.db_name)
    table = model.__table__

    model.metadata.create_all(
        bind=engine,
        checkfirst=True
    )

    records = (
        df
        .where(pd.notnull(df), None)
        .to_dict(orient="records")
    )

    stmt = insert(table).values(records)

    stmt = stmt.on_conflict_do_nothing(
        index_elements=unique_columns
    )

    with engine.begin() as connection:
        connection.execute(stmt)