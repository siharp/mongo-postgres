# main.py
import logging
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.config import *
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':

    raw_path = extract_data(MONGO_URI, MONGO_DB, MONGO_COLLECTION, RAW_DIR, START_DATE, END_DATE)

    clear_path = transform_data(raw_path, MONGO_COLLECTION, CLEAN_DIR, FILTER_DUP)

    load_data(clear_path, PG_HOST, PG_DB, PG_PORT, PG_USER, PG_PASSWORD, MONGO_COLLECTION)
