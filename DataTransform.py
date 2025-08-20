# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(DataTransform) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints

import logging
import azure.functions as func
import requests
import json
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import AzureError
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime, timedelta
import io
import pandas as pd

DataTransform = func.Blueprint()


@DataTransform.timer_trigger(schedule="0 0 0/6 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def JsonTransform(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    credential = DefaultAzureCredential()
    container_raw = "weather"
    container_flattened = "weather-parquet"
    flattened_file_path = "Pensacola_Weather.parquet"

    service_client = DataLakeServiceClient(
        account_url=f"https://weatherstr.dfs.core.windows.net",
        credential=credential
    )

    def flatten_json(y, timestamp=None):
        out = {}
        def flatten(x, name=''):
            if isinstance(x, dict):
                for a in x:
                    flatten(x[a], f'{name}{a}_')
            elif isinstance(x, list):
                i = 0
                for a in x:
                    flatten(a, f'{name}{i}_')
                    i += 1
            else:
                out[name[:-1]] = x
        flatten(y)
        if timestamp:
            out['timestamp'] = timestamp
        return out

    def get_latest_folder_name(service_client, container_name, path_prefix=""):
        path_prefix = path_prefix.rstrip('/')
        file_system_client = service_client.get_file_system_client(container_name)
        paths = file_system_client.get_paths(path=path_prefix, recursive=False)
        folder_names = [p.name.split('/')[-1] for p in paths if p.is_directory]
        if not folder_names:
            return None
        folder_names.sort(reverse=True)
        return folder_names[0]

    def get_latest_date_path(service_client, container_name):
        base_folder = "raw"
        year = get_latest_folder_name(service_client, container_name, base_folder)
        if not year: return None
        month = get_latest_folder_name(service_client, container_name, f"{base_folder}/{year}")
        if not month: return None
        day = get_latest_folder_name(service_client, container_name, f"{base_folder}/{year}/{month}")
        if not day: return None
        return f"{base_folder}/{year}/{month}/{day}"

    def load_existing_parquet(service_client, container_name, file_path):
        file_system_client = service_client.get_file_system_client(container_name)
        file_client = file_system_client.get_file_client(file_path)
        try:
            download = file_client.download_file()
            parquet_bytes = download.readall()
            buffer = io.BytesIO(parquet_bytes)
            df = pd.read_parquet(buffer, engine="pyarrow")
            return df.to_dict(orient="records")
        except Exception as e:
            logging.warning(f"No existing parquet found or error reading file: {e}")
            return []

    def prune_old_data(data, days=7):
        cutoff = datetime.utcnow() - timedelta(days=days)
        pruned = []
        for record in data:
            ts = record.get('timestamp')
            if ts:
                try:
                    rec_dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
                    if rec_dt >= cutoff:
                        pruned.append(record)
                except:
                    pruned.append(record)
            else:
                pruned.append(record)
        return pruned

    def upload_parquet_to_adls(service_client, container_name, file_path, data):
        df = pd.DataFrame(data)
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)
        file_system_client = service_client.get_file_system_client(container_name)
        file_client = file_system_client.get_file_client(file_path)
        file_client.upload_data(buffer.getvalue(), overwrite=True)

    # --------- Main Logic --------- #
    latest_path = get_latest_date_path(service_client, container_raw)
    if not latest_path:
        logging.warning("No data found in storage")
        return
    logging.info(f"Latest data folder: {latest_path}")

    # Load existing data from Parquet
    all_data = load_existing_parquet(service_client, container_flattened, flattened_file_path)
    all_data = prune_old_data(all_data, days=7)

    # Append new data from latest folder
    file_system_client = service_client.get_file_system_client(container_raw)
    paths = file_system_client.get_paths(path=latest_path, recursive=False)

    for file in paths:
        if not file.is_directory:
            logging.info(f"Processing file: {file.name}")
            file_client = file_system_client.get_file_client(file.name)
            download = file_client.download_file()
            json_data = json.loads(download.readall())
            timestamp = datetime.utcnow().isoformat() + "Z"
            flat_data = flatten_json(json_data, timestamp)
            all_data.append(flat_data)

    # Upload merged and pruned data back as parquet
    upload_parquet_to_adls(service_client, container_flattened, flattened_file_path, all_data)
    logging.info(f"Updated {flattened_file_path} with latest data and pruned old records")
    logging.info('Function completed')   


            
        