import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging

def load_data(csv_path, host, db_name, port, user, password, table_name):
    logging.info("Start loading data into PostgreSQL")
    conn = psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password)
    df = pd.read_csv(csv_path)
    if df.empty:
        logging.warning("No data to load")
        return

    df = df.where(pd.notnull(df), None)
    with conn.cursor() as cur:
        records = [tuple(row) for row in df.to_numpy()]
        columns = ', '.join(df.columns)
        query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
        execute_values(cur, query, records)
        conn.commit()
    conn.close()
    logging.info("Data loaded successfully")