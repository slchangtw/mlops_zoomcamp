import pickle
import sys
from typing import Any, Optional

import pandas as pd

with open("model.bin", "rb") as f_in:
    dv, lr = pickle.load(f_in)


def read_data(year: int, month: int) -> pd.DataFrame:
    file_path = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    return pd.read_parquet(file_path)


def prepare_data(df: pd.DataFrame, categorical: list[str]) -> pd.DataFrame:
    df["duration"] = df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    df["duration"] = df["duration"].dt.total_seconds() / 60

    df = df[(df["duration"] >= 1) & (df["duration"] <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")

    return df


def make_predictions(df: pd.DataFrame, categorical: list[str]) -> pd.Series:
    dv, lr = pickle.load(open("model.bin", "rb"))

    dicts = df[categorical].to_dict(orient="records")
    X = dv.transform(dicts)

    return lr.predict(X)


def export_data(
    df: pd.DataFrame, output_path: str, options: Optional[dict[str, Any]]
) -> None:
    df.to_parquet(
        output_path, engine="pyarrow", index=False, storage_options=options
    )


def predict_duration(
    year: int, month: int, categorical: list[str] = ["PULocationID", "DOLocationID"]
) -> None:
    output_file = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"

    df = read_data(year, month)
    df = prepare_data(df, categorical=categorical)
    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype("str")

    df["predicted_duration"] = make_predictions(df, categorical=categorical)

    export_data(df[["ride_id", "predicted_duration"]], output_file)


if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])

    predict_duration(year=year, month=month)
