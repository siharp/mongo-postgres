import os
import json
import logging
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, errors
from bson import json_util

def extract_data(uri:str, db_name:str, collection_name:str, result_dir:str, start_date:int, end_date:int) -> str:
    
    #convert date to datemillisecond
    start_date_mil = int(datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_date_mil = int(datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)

    logging.info('Start extracting data from MongoDB')
    result_path = None

    try:
        with MongoClient(uri) as  client:
            client.server_info()
            logging.info('Connected to MongoDB')

            db = client[db_name]
            collection = db[collection_name]

            query = {"app_event_utc_timestamp": {"$gte": start_date_mil, "$lt": end_date_mil}}
            logging.info(f'Querying data from collection: {collection_name}')
            data = list(collection.find(query).batch_size(5000))

            if not data:
                logging.warning('No data found for given date range.')

            os.makedirs(result_dir, exist_ok=True)
            result_path = os.path.join(result_dir, f'{collection_name}_{start_date}.json')

            with open(result_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, default=json_util.default)
            logging.info(f'Data successfully extracted dan savd to {result_path}')
            return result_path
        
    except errors.ServerSelectionTimeoutError as conn_err:
        logging.error(f'Failed to connect to MongoDB server: {conn_err}')

    except errors.PyMongoError as mongo_err:
        logging.error(f'MongoDB operation error: {mongo_err}')

    except (OSError, IOError) as file_err:
        logging.error(f'File operation error: {file_err}')

    except Exception as e:
        logging.error(f'Unexpected error during extraction: {e}')

    return result_path
       
