from pathlib import Path
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
    gcs_block = GcsBucket.load("zoomcampgcsbucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task(name="transform")
def transform(path: Path) -> pd.DataFrame:
    """Data transform parquet into pandas df"""
    df = pd.read_parquet(path)
    return df


@task(name="load")
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoomcampcred")

    df.to_gbq(
        destination_table="fantasy_premier_league_2022_23.element_stats",
        project_id="data-eng-zoomcamp-project",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        #chunksize=500_000,
        if_exists="append",
    )

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
    files = []
    directory = extract_from_gcs(file_path)
    files = get_file_list(directory)            
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
