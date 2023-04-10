from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import requests
import os

@task
def fetch_url_data(url):
    """Fetch data from API, returns a JSON object"""
    r = requests.get(url)
    data = r.json()
    return data

@task
def convert(key) -> pd.DataFrame:
    """Convert JSON data to pandas DataFrame"""
    df = pd.DataFrame(json[key])
    return df

@task
def write_local(df:pd.DataFrame, key:str) -> Path:
    Path(("fpl").mkdir(parents=True, exist_ok=True))
    path = Path(f"data/{key}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoomcampgcsbucket")
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=1000)
    return

@flow
def etl_web_to_gcs() -> None:
    # The main ETL flow
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    json_data = fetch_url_data(url)
    for key in json_data.keys():
        df = convert(key)
        path = write_local(df, key)
        write_gcs(path)

