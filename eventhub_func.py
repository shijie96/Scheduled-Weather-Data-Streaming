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

eventhub_bp = func.Blueprint()

@eventhub_bp.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="weatherstreamingeventhub",
                               connection="weatherstreamingnamespace_RootManageSharedAccessKey_EVENTHUB") 
def eventhub_trigger(azeventhub: func.EventHubEvent):
    logging.info('Python EventHub trigger processed an event: %s',
                azeventhub.get_body().decode('utf-8'))
    
    try:
        # Get body of incoming message
        message_body = json.loads(azeventhub.get_body().decode('utf-8'))

        # Initialize Storage client
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url="https://keyvault-weather.vault.azure.net/", credential=credential)
        Storage_conn_str = client.get_secret("StorageConnectionStr").value
        Storage_Client = DataLakeServiceClient.from_connection_string(Storage_conn_str)
        file_system = Storage_Client.get_file_system_client("weather")

        if not file_system.exists():
            file_system.create_file_system()

        file_path = f"raw/{datetime.now().strftime('%Y/%m/%d/%H%M%S')}.json"
        file_client = file_system.get_file_client(file_path)
        file_client.upload_data(json.dumps(message_body), overwrite=True)

    except Exception as e:
        logging.error(f"Error in EventHub processing: {str(e)}")
