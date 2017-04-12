
# NWAC

Analyzing weather data from the Northwest Avalanche Center.

## Dependencies

Dependencies are listed in `requirements.txt`.
They can be installed with `pip install requirements.txt`.

## Downloading NWAC Data

The `nwac.py` script downloads data from the [NWAC Weather Data Portal](http://www.nwac.us/data-portal/).
It will download data for all weather stations listed in `stations.csv` for
the years 2015, 2016, and 2017. Note that NWAC also publishes data for 2014,
but it is very sparse. Data is written to `nwac.csv`. Each weather station has a
distinct set of metrics so expect some sparsity in the results. Almost all
stations publish temperature data. Each row has an `id` field which refers
to the station id. 

