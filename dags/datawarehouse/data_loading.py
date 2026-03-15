import json
from datetime import date
import logging
import os

logger = logging.getLogger(__name__)

def load_data(): 
    file_path = os.path.join('.', 'data', f'youtube_data_{date.today()}.json')

    try:
        logger.info(f"Processing file: youtube_data_{date.today()}.json")

        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)

        return data

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise
