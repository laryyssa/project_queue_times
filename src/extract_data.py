import requests
import json
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data(output_path: str, url: str):
    response = requests.get(url)

    data = response.json()

    if response.status_code != 200:
        logging.error(f"Erro na requisição: {url}. Status code: {response.status_code}")
        return None

    if not data:
        logging.warning(f"Erro: Nenhum dado retornado da URL: {url}")
        return None

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=4)

    logging.info(f"Dados extraídos e salvos em: {output_path}")

    return data
