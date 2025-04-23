# main.py
import logging
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.config import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':
    START_DATE = 1713459600000
    END_DATE = 1713546000000
    RAW_DIR = 'data/raw'
    CLEAR_DIR = 'data/clear'

    raw_path = extract_data(MONGO_URI, MONGO_DB, MONGO_COLLECTION, RAW_DIR, START_DATE, END_DATE)
    clear_path = transform_data(raw_path, MONGO_COLLECTION, CLEAR_DIR)
    load_data(clear_path, PG_HOST, PG_DB, PG_PORT, PG_USER, PG_PASSWORD, MONGO_COLLECTION)
