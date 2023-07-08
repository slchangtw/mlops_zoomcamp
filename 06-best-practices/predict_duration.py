import pickle
import sys

import pandas as pd

with open("model.bin", "rb") as f_in:
    dv, lr = pickle.load(f_in)


def read_data(filename: str) -> pd.DataFrame:
    return pd.read_parquet(filename)

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

    y_pred = lr.predict(X)
    print("predicted mean duration:", y_pred.mean())

    return y_pred


def main(
    year: int, month: int, categorical: list[str] = ["PULocationID", "DOLocationID"]
) -> None:
    input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    output_file = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"

    df = read_data(input_file)
    df = prepare_data(df, categorical=categorical)
    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype("str")

    df["predicted_duration"] = make_predictions(df, categorical=categorical)

    df[["ride_id", "predicted_duration"]].to_parquet(
        output_file, engine="pyarrow", index=False
    )


if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])

    main(year=year, month=month)
