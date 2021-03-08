import os

def enrich_data(data):
    for station in data['stations']:
        if station['num_docks_disabled'] == 0:
            color = 'green'
        elif station['num_docks_disabled'] == 1:
            color = 'yellow'
        elif station['num_docks_disabled'] >= 2:
            color = 'red'
        else: # should never get here, but just in case..
            color = 'None'
        station['station_color'] = color
    return data.update({'data': { 'stations' : stations } })

def handler(event, context):
    print(f"Event: {event}")
    raw_response = os.getenv('raw_response', None)
    if raw_response:
        enriched = enrich_data(raw_response)
        return {'enriched': enriched}
    return
    