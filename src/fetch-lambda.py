import os
import requests

def fetch(url):
    try:
        response = requests.get(url)
        if response.status_code not in range(200, 300): raise
        return response.json()
    except requests.exceptions.RequestException as re:
        print(f"[E] Fetch Failed - {re}")
    except Exception as e:
        print(f"[E] Fetch Failed - {e}")

def handler(event, context):
    print(f"Event: {event}")
    url = os.getenv('URL', None)
    if not url: raise Exception("[E] Could not find URL environment variable!")
    raw_response = fetch(url)
    return {'raw_response': raw_response}
