import os
import boto3

s3_resource = boto3.resource('s3')

def dump_data_to_s3(content):
    try:
        dt = datetime.fromtimestamp(content['last_updated'])
        partition = f"{dt.year}/{dt.month}/{dt.day}/{dt.hour}"
        fname = f"{partition}/data.json"
        s3_resource.Object(os.getenv('DATA_BUCKET'), fname).put(Body=content)
        return fname
    except Exception as e:
        print(f"[E] Dumping Failed - {e}")

def handler(event, context):
    print(f"Event: {event}")
    enriched_data = event.get('enriched', None)
    if enriched:
        fname = dump_data_to_s3(enriched)
        return {'fname': fname, 'enriched_data': enriched_data}
    return