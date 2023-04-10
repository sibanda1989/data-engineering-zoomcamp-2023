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

def convert(key) -> pd.DataFrame:
    """Convert JSON data to pandas DataFrame"""
    df = pd.DataFrame(json[key])
    return df


@flow
def etl_web_to_gcs() -> None:
    # The main ETL flow
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    json_data = fetch_url_data(url)
    for key in json_data.keys():
        path = convert(key)
