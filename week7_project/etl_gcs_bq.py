from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3, name="extract")
def extract_from_gcs(file: str) -> Path:
    """Download fpl data from GCS"""
    gcs_path = f"data/fpl/{file}.parquet"
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


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    file = "element_stats"

    path = extract_from_gcs(file)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
