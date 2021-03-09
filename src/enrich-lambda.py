import os
from datetime import datetime
import json
import boto3

s3_resource = boto3.resource('s3')

def load_data_from_s3(key):
    try:
        print(f"Trying to Load data into s3 ==> {key}")
        obj = s3_resource.Object(os.getenv('DATA_BUCKET'), key).get()['Body'].read().decode('utf-8')
        json_content = json.loads(obj)
        print(f"Succeded to Load data into s3 ==> {key}")
        return json_content
    except Exception as e:
        print(e)

def dump_data_to_s3(content):
    try:
        print(f"Trying to Dump data into s3.")
        dt = datetime.fromtimestamp(content['last_updated'])
        partition = f"enriched/{dt.year}/{dt.month:02d}/{dt.day:02d}"
        fname = f"{partition}/data.json"
        s3_resource.Object(os.getenv('DATA_BUCKET'), fname).put(Body=json.dumps(content))
        print(f"Succeded to Dump data into s3 ==> {fname}")
        return fname
    except Exception as e:
        print(f"[E] Dumping Failed - {e}")

def enrich_data(data):
    print("Started Enrichment")
    for station in data['data']['stations']:
        if station['num_docks_disabled'] == 0:
            color = 'green'
        elif station['num_docks_disabled'] == 1:
            color = 'yellow'
        elif station['num_docks_disabled'] >= 2:
            color = 'red'
        else: # should never get here, but just in case..
            color = 'None'
        station['station_color'] = color
    print("Finished Enrichment")
    return data

def handler(event, context):
    print(f"Event: {event}")
    fname = event.get('fname', None)
    if fname:
        data = load_data_from_s3(fname)
        enriched = enrich_data(data)
        enriched_path = dump_data_to_s3(enriched)
        return {'enriched_path': enriched_path}
    return
    