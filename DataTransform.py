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

DataTransform = func.Blueprint()


@DataTransform.timer_trigger(schedule="0 0 0/6 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def JsonTransform(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    # Authenticate with Managed Identity
    try:

        credential = DefaultAzureCredential()
    except Exception:
        credential = ManagedIdentityCredential()
    storage_account_name = "weatherstr"
    container_raw = "weather"
    container_flattened = "weather_parquet"

    service_client = DataLakeServiceClient(
        account_url=f"https://weatherstr.dfs.core.windows.net",
        credential= credential
    )


    def flatten_json(y):
        out = {}
        def flatten(x, name = ''):
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
        return out

    def get_latest_folder_name(service_client, container_name, path_prefix = ""):
        path_prefix = path_prefix.rstrip('/') # Normalize slashes
        file_system_client = service_client.get_file_system_client(container_name)
        paths = file_system_client.get_paths(path = path_prefix, recursive = False)

        folder_names = []
        for p in paths:
            if p.isdirectory:
                folder_name = p.name.split('/')[-1]
                folder_names.append(folder_name)
        if not folder_names:
                return None
        folder_names.sort(reverse = True)
        return folder_names[0]
    
    def get_latest_date_path(service_client, container_name):
        year = get_latest_folder_name(service_client, container_name, "")
        if not year:
            return None
        month  = get_latest_folder_name(service_client, container_name, year)
        if not month:
            return None
        day = get_latest_folder_name(service_client, container_name, f'{year}/{month}')
        if not day:
            return None
        latest_path = f"{year}/{month}/{day}"
        return latest_path
    
    def upload_json_to_adls(service_client, container_name, file_path, data):
        file_system_client = service_client.get_file_system_client(container_name)
        directory_path = '/'.join(file_path.split('/')[:-1])
        directory_client = file_system_client.get_directory_client(directory_path)
        try:
            directory_client.create_directory()
        except Exception as e:
            pass
        
        file_client = file_system_client.get_file_client(file_path)
        file_client.upload_data(json.dumps(data).encode('uft-8'), overwrite = True)



    latest_path = get_latest_date_path(service_client, container_raw)
    if not latest_path:
        logging.warning("No data found in storage")
        return
    logging.info(f"Latest data folder: {latest_path}")

    # List files under latest folder
    file_system_client = service_client.get_file_system_client(container_raw)
    paths = file_system_client.get_paths(path = latest_path, recursive= False)

    for file in paths:
        if not file.is_directory:
            logging.info(f"Processing file: {file.name}")
            file_client = file_system_client.get_file_client(file.name)
            download = file_client.download_file()
            downloaded_bytes = download.readall()
            json_data = json.loads(downloaded_bytes)

            # Flatten JSON
            flat_data = flatten_json(json_data)
            # Upload flatten JSON to flatten container
            flattened_file_path = file.name.replace(container_raw, container_flattened)
            upload_json_to_adls(service_client, container_flattened, flattened_file_path, flat_data)
            logging.info(f"Uploaded flattened file to: {flattened_file_path}")

    logging.info('Function completed')       


            
        