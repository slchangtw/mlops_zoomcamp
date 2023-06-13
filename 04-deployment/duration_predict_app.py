import pickle

import pandas as pd
from flask import Flask, jsonify, request
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_extraction import DictVectorizer

from src import load_data

DATA_FOLDER = "../data"
FEATURE_COLS = ["PULocationID", "DOLocationID"]


def process_trips(trips: pd.DataFrame) -> pd.DataFrame:
    trips["duration"] = trips["tpep_dropoff_datetime"] - trips["tpep_pickup_datetime"]
    trips["duration"] = trips["duration"].dt.total_seconds() / 60

    trips = trips[(trips["duration"] >= 1) & (trips["duration"] <= 60)].copy()

    trips[FEATURE_COLS] = trips[FEATURE_COLS].fillna(-1).astype("int").astype("str")

    return trips


def load_model() -> tuple[DictVectorizer, RandomForestRegressor]:
    with open("model.bin", "rb") as f:
        dv, model = pickle.load(f)
    return dv, model


def predict(features: dict) -> pd.Series:
    dv, model = load_model()
    X = dv.transform([features])
    y_pred = model.predict(X)[0]
    return y_pred


app = Flask("duration-predictor")


@app.route("/predict", methods=["POST"])
def predict_endpoint():
    trips_parameters = request.get_json()

    trips = load_data(*trips_parameters)
    trips = process_trips(trips)

    y_pred = predict(X[FEATURE_COLS])

    result = {
        "y_pred_mean": y_pred.mean(),
        "y_pred_std": y_pred.std(),
    }

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=9696)
