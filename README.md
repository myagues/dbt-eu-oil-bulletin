# EU Weekly Oil Bulletin

> "To improve the transparency of oil prices and to strengthen the internal market, the European Commission's Oil Bulletin presents weekly consumer prices for petroleum products in EU countries."

The [_Weekly Oil Bulletin_](https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en) is a weekly publication that began in 1994 and contains information about petroleum products prices. Unfortunately, the data publication method is not up to date to modern standards and they do not seem to be up to the task in updating their publication method [[1]](https://stackoverflow.com/questions/36031997/oil-bulletin-weekly-fuel-data-import-to-r).

This repository is my effort to refactor the available data in a more efficient format for analysis.

## Steps to reproduce

This repository uses [dbt](https://www.getdbt.com) with [BigQuery](https://cloud.google.com/bigquery), and you probably need basic familiarity with both tools and Python, but it is just a toy project that involves basic procedures and SQL primitives. If you are familiar with a different flavor of data warehouse, just set up the connection with the dbt profile and change the data upload script.

Refer to the following [tutorial](https://cloud.google.com/resource-manager/docs/creating-managing-projects) to set up a project in Google Cloud.

The project requires Python v3.9+, and my recommendation is to set up a virtual environment. For using Python with BigQuery you can follow the steps in the following [tutorial](https://codelabs.developers.google.com/codelabs/cloud-bigquery-python).

### Create source tables

Change location and names according to your needs and preferences:

```
$ bq --location=EU mk -d --description "Weekly bulletin with European gas prices." eu_oil_bulletin
$ bq mk -t --description "European gas prices for the period 1994 - 2005." eu_oil_bulletin.raw_prices_1994 ./table_schemas/raw_prices_1994.json
$ bq mk -t --description "European gas prices with taxes for the period 2005 onwards." eu_oil_bulletin.raw_prices_2005_with_taxes ./table_schemas/raw_prices_2005.json
$ bq mk -t --description "European gas prices without taxes for the period 2005 onwards." eu_oil_bulletin.raw_prices_2005_wo_taxes ./table_schemas/raw_prices_2005.json
```

### Upload source data

```
$ mkdir data
$ cd data
$ curl -L 'https://energy.ec.europa.eu/document/download/2be0e8e8-6869-4993-a8c1-9880e8242919_en?filename=time_series_years_1994_2005.zip' -o time_series_years_1994_2005.zip
$ unzip time_series_years_1994_2005.zip
$ cd ..
```

Data from 2005 onwards is published in a rolling updated [file](https://ec.europa.eu/energy/observatory/reports/Oil_Bulletin_Prices_History.xlsx) that is directly loaded through Pandas, so no need for downloads:

```
$ export GOOGLE_APPLICATION_CREDENTIALS=/path_to_key.json
$ python sources_data_load.py --dataset=eu_oil_bulletin --table=raw_prices_1994 --data_path=./data
$ python sources_data_load.py --dataset=eu_oil_bulletin --table=raw_prices_2005
```

### Run dbt procedures

For BigQuery and dbt configuration refer to [this](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#overview-of-dbt-bigquery) page.

Install packages with:

```
$ cd dbt-oil-bulletin
$ dbt deps
```

Build views and tables:

```
$ dbt seed
$ dbt run
```
