import os
from datetime import datetime, timedelta
import json
import pandas as pd
import boto3

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def load_data_from_s3(_key):
    try:
        print(f"Trying to Load data from s3 ==> {_key}")
        obj = s3_resource.Object(os.getenv('DATA_BUCKET'), _key).get()['Body'].read().decode('utf-8')
        json_content = json.loads(obj)
        print(f"Succeded to Load data from s3 ==> {_key}")
        return json_content
    except Exception as e:
        print(e)

def load_image_to_s3(path):
    try:
        print(f"Trying to Load data into s3 ==> {path}")
        with open(path, "rb") as f:
            _key = path.split('/')[-1]
            s3_client.upload_fileobj(f, os.getenv('DATA_BUCKET'), f"plots/{_key}")
            print(f"Succeded to Load data into s3 ==> {_key}")
            return _key
    except Exception as e:
        print(f"[E] Loading of Image Failed - {e}")

def plot(df, path):
    print("Starting Plot")
    ax = df.plot(x='last_reported_dt', y='is_red_station')
    ax.figure.savefig(path)
    print(f"Saved figure to {path}")

def prepre_data(df):
    print("Starting to prepre data")
    df['last_reported_dt'] = df['last_reported'].apply(lambda ts: datetime.fromtimestamp(ts).strftime("%Y-%m-%d"))
    df['is_red_station'] = df['station_color'].apply(lambda color: 1 if color == 'red' else 0)
    final_df = df.groupby(['last_reported_dt'])['is_red_station'].sum()
    print("Done prepering data")
    return final_df

def get_monthly_data():
    thirty_days_back = datetime.today() - timedelta(30)
    today = datetime.today()
    date_ranges = pd.date_range(thirty_days_back, today, freq='d') # date time range 30 days back (including today)
    date_ranges_str = [str(x).split(' ')[0].replace('-', '/') for x in date_ranges]
    print(f"Date Ranges - {date_ranges_str}")
    all_data = []
    for date_range in date_ranges_str:
        _path = f"enriched/{date_range}/data.json"
        print(f"Fetching {_path} ...")
        try:
            all_data += load_data_from_s3(_path)['data']['stations']
        except:
            pass
    return pd.DataFrame(all_data)

def handler(event, context):
    print(f"Event: {event}")
    tmp_file = "/tmp/monthly_plot_"
    suffix = 'png'
    enriched_data = get_monthly_data()
    if not enriched_data.empty:
        dt = datetime.today()
        tmp_file_path = f"{tmp_file}{dt.year}-{dt.month}-{dt.day}.{suffix}"
        ready_to_plot = prepre_data(enriched_data)
        plot(ready_to_plot, tmp_file_path)
        _key = load_image_to_s3(tmp_file_path)
        return {'status': 'Done', 'key': _key}
    return { 'status': 'Failed' }
