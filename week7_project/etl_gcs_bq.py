from pathlib import Path
from typing import List
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os


@task(retries=3, name="extract")
def extract_from_gcs(file_path: str) -> Path:
    """Download fpl data from GCS"""
    #ammending function to default to downloading all files
    gcs_path = f"data/fpl/"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task(name="transform")
def transform(path: Path) -> pd.DataFrame:
    """Data transform parquet into pandas df"""
    df = pd.read_parquet(path)
    return df


@task(name="load")
def write_bq(df: pd.DataFrame, file: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table=f"fantasy_premier_league_2022_23.{file}",
        project_id="data-eng-zoomcamp-project",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        #chunksize=500_000,
        if_exists="append",
    )

@task(name="extract_file_list")
def get_file_list(directory: str) -> List[str]:
    """Return a list of filenames in a local directory"""
    files = []
    for filename in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, filename)):
            files.append(filename)
    return files            

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    file_path = "data/fpl/"
    directory = extract_from_gcs(file_path)
    files = get_file_list(directory)
    for file in files:
        df = transform(os.path.join(directory,file))
        write_bq(df, file[:-8]) #trim off the .parquet extension
    


if __name__ == "__main__":
    etl_gcs_to_bq()
