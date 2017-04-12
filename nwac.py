
import requests
import requests_cache

import codecs
import csv
from itertools import chain
from contextlib import closing

requests_cache.install_cache('nwac_cache')


DATA_PORTAL_URL = 'http://www.nwac.us/data-portal/csv/q'
YEARS = [2017, 2016, 2015, 2014]
STATIONS_FILE = 'stations.csv'
OUTPUT_FILE = 'nwac.csv'
METRIC_OUTPUT_FILE = 'metric_availability.csv'


def fetch_data(datalogger_id, year):
    params = {'datalogger_id': datalogger_id, 'year': year}

    with closing(requests.get(DATA_PORTAL_URL, params, stream=True)) as resp:
        return csv.DictReader(codecs.iterdecode(resp.iter_lines(), 'utf-8'))


def load_stations():
    with open(STATIONS_FILE, mode='r') as stations_file:
        reader = csv.DictReader(stations_file)
        yield from reader


def download():
    with open(OUTPUT_FILE, 'w') as output_file:
        all_fields = set()
        all_rows = []

        for station in load_stations():
            for year in YEARS:
                print('{} : {}'.format(station['name'], year))
                rows = fetch_data(station['id'], year)
                try:
                    first = next(rows)
                    all_fields.update(first.keys())

                    for row in chain([first], rows):
                        row['id'] = station['id']
                        all_rows.append(row)
                except StopIteration:
                    break

        print('\nDownload complete. Writing to {}'.format(OUTPUT_FILE))
        writer = csv.DictWriter(output_file, ['id'] + list(all_fields))
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)


def metric_availability():
    with open(METRIC_OUTPUT_FILE, 'w') as output_file:
        station_to_metrics = {}

        for station in load_stations():
            metrics = set()

            for year in YEARS:
                rows = fetch_data(station['id'], year)
                try:
                    if station['id'] not in station_to_metrics:
                        station_to_metrics[station['id']] = set()
                    station_to_metrics[station['id']].update(next(rows).keys())
                except StopIteration:
                    break

        all_metrics = list(set.union(*station_to_metrics.values()))
        writer = csv.writer(output_file)
        writer.writerow(['id'] + all_metrics)

        for _id, metrics in station_to_metrics.items():
            writer.writerow([_id] + [1 if metric in metrics else 0
                                     for metric in all_metrics])

download()
