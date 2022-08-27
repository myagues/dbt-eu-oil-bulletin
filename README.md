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

```bash
$ bq --location=EU mk -d --description "Weekly bulletin with European gas prices." eu_oil_bulletin
$ bq mk -t --description "European gas prices for the period 1994 - 2005." eu_oil_bulletin.raw_prices_1994 ./table_schemas/raw_prices_1994.json
$ bq mk -t --description "European gas prices with taxes for the period 2005 onwards." eu_oil_bulletin.raw_prices_2005_with_taxes ./table_schemas/raw_prices_2005.json
$ bq mk -t --description "European gas prices without taxes for the period 2005 onwards." eu_oil_bulletin.raw_prices_2005_wo_taxes ./table_schemas/raw_prices_2005.json
```

### Upload source data

```bash
$ mkdir data
$ cd data
$ curl -L 'https://energy.ec.europa.eu/document/download/2be0e8e8-6869-4993-a8c1-9880e8242919_en?filename=time_series_years_1994_2005.zip' -o time_series_years_1994_2005.zip
$ unzip time_series_years_1994_2005.zip
$ cd ..
```

Data from 2005 onwards is published in a rolling updated [file](https://ec.europa.eu/energy/observatory/reports/Oil_Bulletin_Prices_History.xlsx) that is directly loaded through Pandas, so no need for downloads:

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=/path_to_key.json
$ python sources_data_load.py --dataset=eu_oil_bulletin --table=raw_prices_1994 --data_path=./data
$ python sources_data_load.py --dataset=eu_oil_bulletin --table=raw_prices_2005
```

### Run dbt procedures

For BigQuery and dbt configuration refer to [this](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#overview-of-dbt-bigquery) page.

Install packages with:

```bash
$ cd dbt-oil-bulletin
$ dbt deps
```

Build views and tables:

```bash
$ dbt seed
$ dbt run --profiles-dir .
```

## Cloud Run jobs

[Michael Whitaker](https://www.michaelwhitaker.com/posts/2022-06-01-cloud-run-jobs)'s blog post shows a convenient way to automate runs with Cloud Run jobs. We can adapt the process to:

1. Build a Docker image and store it in [Artifact Registry](https://cloud.google.com/artifact-registry)
2. Create a [Cloud Run job](https://cloud.google.com/run/docs/create-jobs)
3. Execute on a schedule using [Cloud Scheduler](https://cloud.google.com/scheduler)

### Custom Docker image in Artifact Registry

From a [Cloud Shell](https://cloud.google.com/shell/docs), create and configure an Artifact Registry repository ([official documentation](https://cloud.google.com/artifact-registry/docs/repositories/create-repos#create)):

```bash
$ gcloud artifacts repositories create dbt-images \
    --repository-format=docker \
    --description="dbt related images" \
    --location=$CLOUD_RUN_REGION
```

Then, we build the basic files for creating an image, first the `Dockerfile`:

```bash
$ cat <<EOF > Dockerfile
FROM python:3.10-slim

ENV APP_HOME /app
WORKDIR $APP_HOME

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get -y install --no-install-recommends apt-utils dialog 2>&1 \
    && apt-get -y install git \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /app/entrypoint.sh
ENTRYPOINT [ "./entrypoint.sh" ]
EOF
```

and the script file we use in the entrypoint:

```bash
$ cat <<EOF > entrypoint.sh
#!/bin/bash

set -o pipefail

if [ $# -eq 0 ]; then
    echo "No arguments provided"
    exit 1
fi

git clone --depth 1 https://github.com/myagues/$1
cd $1
pip install --no-cache-dir -r requirements.txt

python sources_data_load.py $2
cd dbt
dbt deps
dbt run --profiles-dir . --target prod
EOF
chmod +x entrypoint.sh
```

Now we register the image with:

```bash
$ gcloud builds submit -t $CLOUD_RUN_REGION-docker.pkg.dev/$PROJECT_ID/dbt-images/dbt-base
```

### Cloud Run job

See [official documentation](https://cloud.google.com/run/docs/create-jobs) for more details:

```bash
$ gcloud beta run jobs create dbt-eu-oil \
    --image $CLOUD_RUN_REGION-docker.pkg.dev/$PROJECT_ID/dbt-images/dbt-base \
    --args=dbt-eu-oil-bulletin,"--dataset=eu_oil_bulletin --table=raw_prices_2005" \
    --max-retries=0 \
    --memory=1Gi \
    --region=$CLOUD_RUN_REGION \
    --set-env-vars=GOOGLE_CLOUD_PROJECT=$PROJECT_ID
```

### Execute the Cloud Run job on a schedule

See [official documentation](https://cloud.google.com/run/docs/execute/jobs-on-schedule) for more details:

```bash
$ gcloud scheduler jobs create http dbt-eu-oil \
  --location $CLOUD_RUN_REGION \
  --schedule="0 0 * * 2" \
  --uri="https://$CLOUD_RUN_REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/dbt-eu-oil:run" \
  --http-method POST \
  --oauth-service-account-email $PROJECT_NUMBER-compute@developer.gserviceaccount.com
```
