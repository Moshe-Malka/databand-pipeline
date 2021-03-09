import os
import json
from datetime import datetime
import requests
import boto3

s3_resource = boto3.resource('s3')

def dump_data_to_s3(content):
    try:
        print(f"Trying to Dump data into s3.")
        dt = datetime.fromtimestamp(content['last_updated'])
        partition = f"fatch_dumps/{dt.year}/{dt.month}/{dt.day}/{dt.hour}"
        fname = f"{partition}/data.json"
        s3_resource.Object(os.getenv('DATA_BUCKET'), fname).put(Body=json.dumps(content))
        print(f"Succeded to Dump data into s3 ==> {fname}")
        return fname
    except Exception as e:
        print(f"[E] Dumping Failed - {e}")
        raise

def fetch(url):
    try:
        print("Started Fetch")
        response = requests.get(url)
        if response.status_code not in range(200, 300): raise
        print("Finished Fetch")
        return response.json()
    except requests.exceptions.RequestException as re:
        print(f"[E] Fetch Failed - {re}")
        raise
    except Exception as e:
        print(f"[E] Fetch Failed - {e}")
        raise

def handler(event, context):
    print(f"Event: {event}")
    try:
        url = os.getenv('URL', None)
        if not url: raise Exception("[E] Could not find URL environment variable!")
        raw_response = fetch(url)
        fname = dump_data_to_s3(raw_response)
        return { 'statusCode': 200, 'message': 'Success', 'fname': fname }
    except:
        raise
    
