# EarthQuake-Batch-Automation-Project Overview - 
This project is a fully automated batch ETL pipeline which uses the [USGS API](https://earthquake.usgs.gov/fdsnws/event/1/) to extract earthquake data in the for of GeoJSON which then transforming and structuring the data so it can then be loaded into GCS, then finally BigQuery for assuming would be analysis. The pipeline runs on a daily schedule using the orcehstration tool prefect, where it automatically pulls data from the most recent day transforming into parquet file format, storing it into a GCS bucket before loading into Bigquery while maintaining idempotency.

# Setup Instructions (environment, Prefect deployment, etc.)
- Create virtual evniorment for Extract.py and pip install -r requirements.txt
- Configure a GCP credentials [block](https://docs.prefect.io/v3/concepts/blocks) within Prefect UI using a service account key
- Within script set destination to prefered GCS bucket, as well BigQuery Table within a project
- Install prefect package "pip install -U prefect"
- Create a deployment "prefect deploy {name of deployment}" https://docs.prefect.io/v3/how-to-guides/deployments/create-deployments
- If running locally (only option with free version of prefect) create a work pool under deployment within prefect UI
- Select Schdeule, or run the flow manually using prefect deployment run "earthquake-pipeline/usgs-daily" --params '{"days_back": 1}' (uses look back window to gather 1 day worth of data)

# Built With 
- Python 
- GoogleSQL 
- Prefect 
- Google Cloud Storage (GCS) 
- Google Big Query 

# Packages/ libraries all in 
- requirements.txt

# 


# Future Work
At this point in time Im considering adding an extra layer such as a loading the data from tables in BigQuery to visualization tools to expand the scope of this project.

