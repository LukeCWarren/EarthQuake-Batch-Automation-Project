# Extract_Merge.py
import requests
import json
import pandas as pd
from datetime import datetime, timedelta, timezone

from prefect import flow, get_run_logger
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from google.cloud import storage, bigquery
import os

#prefect block class managing secret
from prefect_gcp.secret_manager import GcpSecret

gcpsecret_block = GcpSecret.load("gcp-sa-json")


### helper: create GCP clients
def make_gcp_clients():
    """
    Create and return (gcs_client, bq_client).
    If GOOGLE_APPLICATION_CREDENTIALS is set, uses that for auth.
    """
    gcs = storage.Client()
    bq = bigquery.Client()
    return gcs, bq


### helper: load parquet into a single BigQuery partition (overwrites that day) 
def load_parquet_to_curated_partition(gcs_uri: str, date_str: str, bq_client: bigquery.Client):
    """
    Load parquet from GCS into practical-data-engineering.earthquakes.quakes_curated
    for the given date. Uses partition decorator + WRITE_TRUNCATE (idempotent per day).
    """
    partition = date_str.replace("-", "")  # e.g. 2025-09-29 -> 20250929
    dest = f"practical-data-engineering.earthquakes.quakes_curated${partition}"
    job = bq_client.load_table_from_uri(
        gcs_uri,
        dest,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    job.result()
    return dest


@flow(name="earthquake-pipeline")
def earthquake_pipeline(days_back: int = 1):
    logger = get_run_logger()

        # ---- GCP AUTH via Prefect block (your block name: gcp-sa-json) ----
    # This reads your SA JSON from GCP Secret Manager (through the Prefect block)
    gcpsecret_block = GcpSecret.load("gcp-sa-json")  # block you already created
    sa_bytes = gcpsecret_block.read_secret()         # bytes
    creds_dict = json.loads(sa_bytes.decode("utf-8"))

    ### Look-back window
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_back)

    ### API request
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start.isoformat(),
        "endtime": end.isoformat(),
    }
    resp = requests.get(url, params=params, timeout=30)
    logger.info(f"Request URL: {resp.url}")
    resp.raise_for_status()
    data = resp.json()

    ### Normalize
    features = data.get("features", [])
    if not features:
        logger.info("No earthquakes found for the requested window.")
        return 0

    df = pd.json_normalize(features, sep="_")
    cols = [
        "id",
        "properties_time",
        "properties_updated",
        "properties_mag",
        "properties_place",
        "properties_tsunami",
        "properties_sig",
        "properties_type",
        "properties_status",
        "geometry_coordinates",
    ]
    df = df[[c for c in cols if c in df.columns]].copy()

    if "geometry_coordinates" in df.columns:
        df[["longitude", "latitude", "depth_km"]] = pd.DataFrame(
            df["geometry_coordinates"].tolist(), index=df.index
        )
        df.drop(columns=["geometry_coordinates"], inplace=True)

    ### epoch ms -> UTC timestamps
    for c in ["properties_time", "properties_updated"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], unit="ms", utc=True)

    ### de-dupe within batch by event id
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"], keep="last")

    logger.info(f"Rows fetched: {len(df)}")
    if df.empty:
        logger.info("DataFrame is empty")
        return 0

    ### determine which calendar dates are present (UTC)
    df["event_date"] = df["properties_time"].dt.date
    unique_dates = sorted(df["event_date"].unique())
    logger.info(f"Calendar dates in batch: {unique_dates}")

    bucket_name = "luke_project"
    gcs, bq = make_gcp_clients()

    total_loaded = 0
    for d in unique_dates:
        # subset rows for this date
        part_df = df[df["event_date"] == d].drop(columns=["event_date"]).copy()
        date_str = d.isoformat()

        ### Parquet uses TIMESTAMP_MILLIS (avoid TIMESTAMP_NANOS)
        fields = []
        for name in part_df.columns:
            if name in ("properties_time", "properties_updated"):
                fields.append(pa.field(name, pa.timestamp("ms", tz="UTC")))
            elif name in ("longitude", "latitude", "depth_km", "properties_mag"):
                fields.append(pa.field(name, pa.float64()))
            elif name in ("properties_tsunami", "properties_sig"):
                fields.append(pa.field(name, pa.int64()))
            else:
                fields.append(pa.field(name, pa.string()))
        arrow_schema = pa.schema(fields)

        table = pa.Table.from_pandas(part_df, schema=arrow_schema, preserve_index=False)
        buf = BytesIO()
        pq.write_table(table, buf)

        ### daily path per event date
        path = f"earthquakes/ingest_date={date_str}/quakes_{date_str}.parquet"
        gcs.bucket(bucket_name).blob(path).upload_from_string(buf.getvalue())
        gcs_uri = f"gs://{bucket_name}/{path}"
        logger.info(f"Uploaded to {gcs_uri}")

        ### load this date into its own partition (idempotent per day)
        dest = load_parquet_to_curated_partition(gcs_uri, date_str, bq)
        logger.info(f"Loaded GCS parquet into BigQuery partition: {dest}")

        total_loaded += len(part_df)

    ### sanity prints from the full DF
    print(df.head())
    print(len(df))
    print("Dates loaded:", unique_dates)

    return total_loaded


if __name__ == "__main__":
    earthquake_pipeline(days_back=1)

