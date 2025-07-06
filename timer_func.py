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

timer_bp = func.Blueprint()

@timer_bp.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def weatherapi(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
    # Event Hub configuration
    # Configuration - consider moving these to application settings

    EVENT_HUB_NAME = "weatherstreamingeventhub"
    EVENT_HUB_NAMESPACE = "weather-streamingnamespace.servicebus.windows.net"
    VAULT_URL = "https://keyvault-weather.vault.azure.net/"
    API_KEY_SECRET_NAME = "weather-api-key"
    WEATHER_API_BASE_URL = "http://api.weatherapi.com/v1"
    LOCATION = "Pensacola"

    try:
        # Initialize credentials - using Managed Identity explicitly
        credential = ManagedIdentityCredential()
        logging.info("Managed Identity credential initialized")

        # Initialize Event Hub Producer with retry policy
        producer = EventHubProducerClient(
            fully_qualified_namespace=EVENT_HUB_NAMESPACE,
            eventhub_name=EVENT_HUB_NAME,
            credential=credential,
            retry_total=3  # Retry up to 3 times on failure
        )
        logging.info("Event Hub producer client initialized")

        def send_event(event_data):
            """Send event to Event Hub with error handling"""
            try:
                with producer:
                    event_batch = producer.create_batch()
                    event_batch.add(EventData(json.dumps(event_data)))
                    producer.send_batch(event_batch)
                    logging.info(f"Successfully sent event to Event Hub: {str(event_data)[:100]}...")
            except ValueError as ve:
                logging.error(f"ValueError in sending event: {str(ve)}")
            except AzureError as ae:
                logging.error(f"AzureError in sending event: {str(ae)}")
            except Exception as ex:
                logging.error(f"Unexpected error in sending event: {str(ex)}")

        def handle_response(response):
            """Handle API response with proper error handling"""
            try:
                if response.status_code == 200:
                    return response.json()
                else:
                    error_msg = f"API Error: {response.status_code}, {response.text}"
                    logging.error(error_msg)
                    raise Exception(error_msg)
            except json.JSONDecodeError:
                error_msg = f"Invalid JSON response: {response.text}"
                logging.error(error_msg)
                raise Exception(error_msg)

        def get_api_data(url, params):
            """Generic API request function with retry logic"""
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = requests.get(url, params=params, timeout=10)
                    return handle_response(response)
                except requests.exceptions.RequestException as re:
                    if attempt == max_retries - 1:
                        logging.error(f"Final attempt failed for {url}: {str(re)}")
                        raise
                    logging.warning(f"Attempt {attempt + 1} failed for {url}, retrying...")
                    continue

        def get_secret_from_keyvault():
            """Retrieve secret from Key Vault with error handling"""
            try:
                secret_client = SecretClient(vault_url=VAULT_URL, credential=credential)
                secret = secret_client.get_secret(API_KEY_SECRET_NAME)
                logging.info("Successfully retrieved secret from Key Vault")
                return secret.value
            except AzureError as ae:
                logging.error(f"Failed to get secret from Key Vault: {str(ae)}")
                raise
            except Exception as ex:
                logging.error(f"Unexpected error getting secret: {str(ex)}")
                raise

        def fetch_weather_data():
            """Main function to fetch and process weather data"""
            try:
                api_key = get_secret_from_keyvault()
                
                # Get all data with error handling
                current_weather = get_api_data(
                    f"{WEATHER_API_BASE_URL}/current.json",
                    {'key': api_key, 'q': LOCATION, 'aqi': 'yes'}
                )
                
                forecast_weather = get_api_data(
                    f"{WEATHER_API_BASE_URL}/forecast.json",
                    {'key': api_key, 'q': LOCATION, 'days': 3}
                )
                
                alerts = get_api_data(
                    f"{WEATHER_API_BASE_URL}/alerts.json",
                    {'key': api_key, 'q': LOCATION, 'alerts': 'yes'}
                )

                def flatten_data(current_weather, forecast_weather, alerts):
                    location_data = current_weather.get("location", {})
                    current = current_weather.get("current", {})
                    condition = current.get("condition", {})
                    air_quality = current.get("air_quality", {})
                    forecast = forecast_weather.get("forecast", {}).get("forecastday", [])
                    alert_list = alerts.get("alerts", {}).get("alert", [])

                    flattened_data = {
                    'name': location_data.get('name'),
                    'region': location_data.get('region'),
                    'country': location_data.get('country'),
                    'lat': location_data.get('lat'),
                    'lon': location_data.get('lon'),
                    'localtime': location_data.get('localtime'),
                    'temp_c': current.get('temp_c'),
                    'is_day': current.get('is_day'),
                    'condition_text': condition.get('text'),
                    'condition_icon': condition.get('icon'),
                    'wind_kph': current.get('wind_kph'),
                    'wind_degree': current.get('wind_degree'),
                    'wind_dir': current.get('wind_dir'),
                    'pressure_in': current.get('pressure_in'),
                    'precip_in': current.get('precip_in'),
                    'humidity': current.get('humidity'),
                    'cloud': current.get('cloud'),
                    'feelslike_c': current.get('feelslike_c'),
                    'uv': current.get('uv'),
                    'air_quality': {
                        'co': air_quality.get('co'),
                        'no2': air_quality.get('no2'),
                        'o3': air_quality.get('o3'),
                        'so2': air_quality.get('so2'),
                        'pm2_5': air_quality.get('pm2_5'),
                        'pm10': air_quality.get('pm10'),  
                        'gb-defra-index': air_quality.get('gb-defra-index')
                    },
                    'alerts': [
                        {
                        'headline': alert.get('headline'),
                        'severity': alert.get('severity'),
                        'description': alert.get('description'),
                        'instruction': alert.get('instruction')
                        }
                        for alert in alert_list 
                    ],
                    'forecast': [
                        {
                        'date':day.get('date'),
                        'maxtemp_c': day.get('day',{}).get('maxtemp_c'),
                        'mintemp_c': day.get('day', {}).get('mintemp_c'),
                        'condition': day.get('day', {}).get('condition', {}).get('text')
                        }
                        for day in forecast
                    ]
                    }
                    return flattened_data

                # Process and send data
                merged_data = flatten_data(current_weather, forecast_weather, alerts)
                
                send_event(merged_data)
                
                return True
            except Exception as e:
                logging.error(f"Error in fetch_weather_data: {str(e)}")
                return False

        # Execute the main function
        if fetch_weather_data():
            logging.info("Weather data processing completed successfully")
        else:
            logging.error("Weather data processing failed")

    except Exception as main_ex:
        logging.error(f"Fatal error in function execution: {str(main_ex)}")
    finally:
        try:
            if 'producer' in locals():
                producer.close()
                logging.info("Event Hub producer closed successfully")
        except Exception as close_ex:
            logging.error(f"Error closing producer: {str(close_ex)}")

        logging.info("Function execution completed")