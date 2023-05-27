from pathlib import Path

import pandas as pd
import wget

TLC_TRIP_DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def read_trips(data_folder: Path, color: str, year: str, month: str) -> pd.DataFrame:
    if not data_folder.exists():
        data_folder.mkdir(parents=True, exist_ok=True)

    data_path = data_folder / f"{color}_tripdata_{year}-{month:>02}.parquet"
    if not data_path.exists():
        url = f"{TLC_TRIP_DATA_URL}{color}_tripdata_{year}-{month:>02}.parquet"
        wget.download(url, str(data_path))

    return pd.read_parquet(data_path)