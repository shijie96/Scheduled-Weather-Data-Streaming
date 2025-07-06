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
from timer_func import timer_bp
from eventhub_func import eventhub_bp


app = func.FunctionApp()

app.register_functions(timer_bp)
app.register_functions(eventhub_bp)


