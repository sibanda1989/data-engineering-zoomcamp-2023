from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import os


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues, different columns for yellow and green taxis"""
    if color == "yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    else:
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoomcampgcsbucket")
    
    # ## For slow upload speed
    gcs_block.blob._DEFAULT_CHUNKSIZE = 2097152 # 1024 * 1024 B * 2 = 2 MB
    gcs_block.blob._MAX_MULTIPART_SIZE = 2097152 # 2 MB

    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=1000)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    colors = ["yellow", "green"]
    
    for idx, color in enumerate(colors): # extract data for yellow and green colors
        for year in range(2019, 2021): #extract years 2019 and 2020 data
            for month in range(1,13): #extract 12 months data for each year
                dataset_file = f"{color}_tripdata_{year}-{month:02}"
                dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

                df = fetch(dataset_url)
                df_clean = clean(df, color)
                path = write_local(df_clean, color, dataset_file)
                write_gcs(path)
                print(color + f" : {year} : " + f"{month} : done")


if __name__ == "__main__":
    etl_web_to_gcs()