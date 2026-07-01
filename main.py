from src.extract_data import extract_data
from src.load_data import load_data
from src.transform_data import transform_data

import os
from pathlib import Path
from dotenv import load_dotenv

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)


url = os.getenv('URL_PARKS')
table_name = os.getenv('TABLE_NAME')

output_file = Path(__file__).parent / "data" / "parks.json"

def pipeline():
    try:
        logging.info("ETAPA 1: EXTRACT")
        extract_data(url, output_file)
        
        logging.info("ETAPA 2: TRANSFORM")
        df = transform_data(output_file)
        
        logging.info("ETAPA 3: LOAD")
        load_data(table_name, df)
        
        print("\n" + "="*60)
        print("✅ Pipeline concluído com sucesso!")
        print("="*60)
        
    except Exception as e:
        logging.error(f"❌ ERRO no Pipeline: {e}")
        import traceback
        traceback.print_exc()
    
pipeline()