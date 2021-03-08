import os
from datetime import datetime
import pandas as pd
import boto3

s3_client = boto3.client('s3')

def load_image_to_s3(path):
    try:
        with open(path, "rb") as f:
            key = path.split('/')[-1]
            s3_client.upload_fileobj(f, os.getenv('DATA_BUCKET'), key)
    except Exception as e:
        print(f"[E] Loading of Image Failed - {e}")

def plot(df, path):
    ax = df.plot(x='last_reported_dt', y='is_red_station')
    ax.figure.savefig(path)

def prepre_data(data):
    df = pd.DataFrame(data['data']['stations'])
    df['last_reported_dt'] = df['last_reported'].apply(lambda ts: datetime.fromtimestamp(ts).strftime("%Y-%m-%d"))
    df['is_red_station'] = df['station_color'].apply(lambda color: 1 if color == 'red' else 0)
    return df.groupby(['last_reported_dt'])['is_red_station'].sum()

def handler(event, context):
    print(f"Event: {event}")
    tmp_file_path = "/tmp/monthly_plot.png"
    enriched_data = event.get('enriched_data', None)
    if enriched_data:
        ready_to_plot = prepre_data(enriched_data)
        plot(ready_to_plot, tmp_file_path)
        load_image_to_s3(tmp_file_path)
        return {'status': 'done'}
    return {'status': 'failed'}