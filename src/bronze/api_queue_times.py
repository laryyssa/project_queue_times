import os
import json
import logging
import requests
from pathlib import Path
from datetime import datetime

from utils.db import get_db_data

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 15  # segundos


def get_park_ids(engine) -> list:
    query = "SELECT DISTINCT id FROM parks"
    list_park_ids = get_db_data(query, engine)
    return list_park_ids['id'].tolist()


def get_queue_times_api_base_url() -> str:
    url = os.getenv("QUEUE_TIMES_API_URL")
    if not url:
        raise ValueError("A variável de ambiente QUEUE_TIMES_API_URL não está definida.")
    return url


def build_urls_for_park_ids(park_ids: list, base_url: str) -> list:
    return [base_url.format(park_id) for park_id in park_ids]


def extract_and_load_queue_times_data(output_path: str, url: str):
    """Extrai os dados de uma URL e salva como JSON bruto."""
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as e:
        logger.error(f"Falha na requisição para {url}: {e}")
        return None

    if response.status_code != 200:
        logger.error(f"Erro na requisição: {url}. Status code: {response.status_code}")
        return None

    try:
        data = response.json()
    except ValueError:
        logger.error(f"Resposta não é um JSON válido: {url}")
        return None

    if not data:
        logger.warning(f"Nenhum dado retornado da URL: {url}")
        return None

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=4)

    logger.info(f"Dados extraídos e salvos em: {output_path}")
    return output_path


def run_bronze_extraction(engine, output_base_dir: str = "/opt/airflow/data/bronze/parks_queue_times") -> None:

    print("engine", engine)
    
    base_url = get_queue_times_api_base_url()
    park_ids = get_park_ids(engine)
    urls = build_urls_for_park_ids(park_ids, base_url)

    now = datetime.now()
    date_path = now.strftime("%Y/%m/%d/%H/%M")
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S-%SZ")

    saved_count = 0
    for park_id, url in zip(park_ids, urls):
        output_path = f"{output_base_dir}/{date_path}/bronze_{park_id}_{timestamp}.json"
        result = extract_and_load_queue_times_data(output_path, url)
        if result:
            saved_count += 1

    if saved_count == 0:
        logger.warning("Nenhum arquivo bronze foi gerado nesta execução.")
    else:
        logger.info(f"{saved_count}/{len(park_ids)} arquivos bronze salvos com sucesso.")


if __name__ == "__main__":
    from utils.db import get_engine

    engine = get_engine()
    run_bronze_extraction(engine)