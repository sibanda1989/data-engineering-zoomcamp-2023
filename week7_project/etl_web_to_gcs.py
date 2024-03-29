from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import requests
import os

@task(retries=3)
def fetch_url_data(url):
    """Fetch data from API, returns a JSON object"""
    r = requests.get(url)
    data = r.json()
    return data

@task
def convert(key, json_data) -> pd.DataFrame:
    """Convert JSON data to pandas DataFrame"""
    df = pd.DataFrame(json_data[key])
    return df

@task
def write_local(df:pd.DataFrame, key:str) -> Path:
    """Write dataframe out locally as parquet file"""
    Path("data/fpl").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/fpl/{key}.parquet")
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
    keys_to_skip = ['game_settings', 'total_players'] # these keys do not have a flat JSON structure
    for key in json_data.keys():
        # skip keys in list
        if key in keys_to_skip:
            continue
        df = convert(key, json_data)
        path = write_local(df, key)
        write_gcs(path)
    print("All elements uploaded in GCS!")        

if __name__ == "__main__":
    etl_web_to_gcs()        

