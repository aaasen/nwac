
import requests
import requests_cache

import codecs
import csv
from itertools import chain
from contextlib import closing

requests_cache.install_cache('nwac_cache')


DATA_PORTAL_URL = 'http://www.nwac.us/data-portal/csv/q'
YEARS = [2017, 2016, 2015]
STATIONS_FILE = 'stations.csv'
OUTPUT_FILE = 'nwac.csv'


def fetch_data(datalogger_id, year):
    params = {'datalogger_id': datalogger_id, 'year': year}

    with closing(requests.get(DATA_PORTAL_URL, params, stream=True)) as resp:
        return csv.DictReader(codecs.iterdecode(resp.iter_lines(), 'utf-8'))


def load_stations():
    with open(STATIONS_FILE, mode='r') as stations_file:
        reader = csv.DictReader(stations_file)
        yield from reader


with open(OUTPUT_FILE, 'w') as output_file:
    all_fields = set()
    all_rows = []

    for station in load_stations():
        for year in YEARS:
            print('{} : {}'.format(station['name'], year))
            rows = fetch_data(station['id'], year)
            first = next(rows)
            all_fields.update(first.keys())

            for row in chain([first], rows):
                row['id'] = station['id']
                all_rows.append(row)

    print('\nDownload complete. Writing to {}'.format(OUTPUT_FILE))
    writer = csv.DictWriter(output_file, ['id'] + list(all_fields))
    writer.writeheader()
    for row in all_rows:
        writer.writerow(row)
