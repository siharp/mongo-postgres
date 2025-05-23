import os
import pandas as pd
from datetime import datetime
import logging

def transform_data(json_path:str, collection_name:str, result_dir:str, filter_duplicated:str) -> str:
    logging.info('Start transforming data')
    df = pd.read_json(json_path)
    df = df.drop_duplicates(subset=f'{filter_duplicated}')
    df['_id'] = df['_id'].apply(lambda x: x['$oid'] if isinstance(x, dict) and '$oid' in x else x)
    df['app_event_utc_timestamp'] = pd.to_datetime(df['app_event_utc_timestamp'], unit='ms')

    os.makedirs(result_dir, exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')
    result_path = os.path.join(result_dir, f"{collection_name}_{today}.csv")
    df.to_csv(result_path, index=False)

    logging.info(f"Transformed data saved to {result_path}")
    return result_path