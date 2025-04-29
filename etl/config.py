import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join('config', '.env'))

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

PG_HOST = os.getenv("PG_HOST")
PG_DB = os.getenv("PG_DB")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")
RAW_DIR = os.getenv("RAW_DIR")
CLEAR_DIR = os.getenv("CLEAR_DIR")
FILTER_DUP = os.getenv("FILTER_DUP")