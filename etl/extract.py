import os
import json
import logging
from datetime import datetime
from pymongo import MongoClient
from bson import json_util

def extract_data(uri:str, db_name:str, collection_name:str, result_dir:str, start_date:int, end_date:int) -> str:
    logging.info('Start extracting data from MongoDB')
    client = None
    try:
        client = MongoClient(uri)
        collection = client[db_name][collection_name]
        logging.info('Connect to database')
        query = {"db_created_at": {"$gte": start_date, "$lt": end_date}}

        logging.info(f'Quering data from {collection_name}')
        data = list(collection.find(query))
        
        os.makedirs(result_dir, exist_ok=True)
        today = datetime.now().strftime('%Y-%m-%d')
        result_path = os.path.join(result_dir, f'{collection_name}_{today}.json')

        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, default=json_util.default)
        return result_path

    except Exception as e:
        logging.error('Error Conect to database')
        return []

    finally:
        logging.info(f'Extracted data saved to {result_path}')
        client.close()
       
