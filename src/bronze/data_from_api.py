import requests
import json
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_and_load_data(output_path: str, url: str):
    response = requests.get(url)
    
    if response.status_code != 200:
        logging.error(f"Erro na requisição: {url}. Status code: {response.status_code}")
        return None
    
    data = response.json()
    
    if not data:
        logging.warning(f"Erro: Nenhum dado retornado da URL: {url}")
        return None
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=4)
    
    logging.info(f"Dados extraídos e salvos em: {output_path}")
