import os
import pandas as pd
import logging

def transform_data(json_path:str, collection_name:str, result_dir:str) -> str:
    logging.info('Start transforming data')
    df = pd.read_json(json_path)
    df = df.drop_duplicates(subset='application_event_watermelon_id')
    df['_id'] = df['_id'].apply(lambda x: x['$oid'] if isinstance(x, dict) and '$oid' in x else x)
    df['app_event_utc_timestamp'] = pd.to_datetime(df['app_event_utc_timestamp'], unit='ms')

    os.makedirs(result_dir, exist_ok=True)
    result_path = os.path.join(result_dir, f"{collection_name}.csv")
    df.to_csv(result_path, index=False)

    logging.info(f"Transformed data saved to {result_path}")
    return result_path