import pickle
from pathlib import Path

import pandas as pd
import wget
from flask import Flask, jsonify, request
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_extraction import DictVectorizer
from waitress import serve

DATA_FOLDER = "data"
FEATURE_COLS = ["PULocationID", "DOLocationID"]

TLC_TRIP_DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def read_trips(data_folder: Path, color: str, year: str, month: str) -> pd.DataFrame:
    if not data_folder.exists():
        data_folder.mkdir(parents=True, exist_ok=True)

    data_path = data_folder / f"{color}_tripdata_{year}-{month:>02}.parquet"
    if not data_path.exists():
        url = f"{TLC_TRIP_DATA_URL}{color}_tripdata_{year}-{month:>02}.parquet"
        wget.download(url, str(data_path))

    return pd.read_parquet(data_path)


def process_trips(trips: pd.DataFrame, trips_params: dict) -> pd.DataFrame:
    trips["duration"] = trips["tpep_dropoff_datetime"] - trips["tpep_pickup_datetime"]
    trips["duration"] = trips["duration"].dt.total_seconds() / 60

    trips = trips[(trips["duration"] >= 1) & (trips["duration"] <= 60)].copy()

    trips[FEATURE_COLS] = trips[FEATURE_COLS].fillna(-1).astype("int").astype("str")
    trips[
        "ride_id"
    ] = f'{trips_params["year"]:>04}/{trips_params["month"]:>02}_' + trips.index.astype(
        "str"
    )

    return trips


def save_predictions(
    trips: pd.DataFrame, data_folder: Path, trips_params: dict
) -> None:
    file_name = (
        f'{trips_params["color"]}_tripdata_{trips_params["year"]}_'
        f'{trips_params["month"]}_preds.parquet'
    )
    trips.to_parquet(
        data_folder / file_name,
        engine="pyarrow",
        compression=None,
        index=False,
    )


def load_model() -> tuple[DictVectorizer, RandomForestRegressor]:
    with open("model.bin", "rb") as f:
        dv, model = pickle.load(f)
    return dv, model


def predict(features: pd.DataFrame) -> pd.Series:
    dv, model = load_model()

    features_dict = features.to_dict(orient="records")
    X = dv.transform(features_dict)
    y_pred = model.predict(X)
    return y_pred


app = Flask("duration-predictor")


@app.route("/predict", methods=["POST"])
def predict_endpoint():
    trips_params = request.get_json()

    trips = read_trips(
        data_folder=Path(DATA_FOLDER),
        color=trips_params["color"],
        year=trips_params["year"],
        month=trips_params["month"],
    )
    trips = process_trips(trips, trips_params)

    trips["pred"] = predict(trips[FEATURE_COLS])

    result = {
        "y_pred_mean": trips["pred"].mean(),
        "y_pred_std": trips["pred"].std(),
    }

    save_predictions(trips[["ride_id", "pred"]], Path(DATA_FOLDER), trips_params)
    return jsonify(result)


if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=9696)
