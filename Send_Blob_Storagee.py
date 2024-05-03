from azure.storage.blob import BlobServiceClient
import requests
from datetime import datetime
import logging
import json

def pull_api():
    try:
        response = requests.get('https://api.coinlore.net/api/tickers/')
        response.raise_for_status()
        coinlore_data = response.json()
        coinlore_list = []
        for data in coinlore_data['data']:
            # print(data)
            coinlore_json = {
                'rank': data['rank'],
                'name': data['name'],
                'price_usd': data['price_usd'],
                'percent_change_24h': data['percent_change_24h'],
                'percent_change_1h': data['percent_change_1h'],
                'percent_change_7d': data['percent_change_7d'],
                'market_cap_usd': data['market_cap_usd'],
                'volume24': data['volume24'],
                'volume24a': data['volume24a'],
                'csupply': data['csupply'],
                'tsupply': data['tsupply'],
                'msupply': data['msupply'],
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            coinlore_list.append(coinlore_json)
            json_format = json.dumps(coinlore_list,indent=4)
            data_bytes = json_format.encode('utf-8')
            # add a file_name
            blob_name = f"Coinlore_JsonData_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.json"
            
            connection_string = ""
            container_name = ""
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_cli = blob_service_client.get_container_client(container_name)

            # Upload data
            blob_cli = container_cli.get_blob_client(blob_name)
            blob_cli.upload_blob(data_bytes, overwrite=True)

            print(f"Data Saved To Azure Blob Storage: {data['name']}")
    except Exception as e:
        logging.error(f'An error Occurred while pulling data: {e}')
pull_api()
