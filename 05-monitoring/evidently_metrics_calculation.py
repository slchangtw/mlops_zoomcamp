import logging
import pickle
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
from evidently import ColumnMapping
from evidently.metrics import (ColumnDriftMetric, ColumnQuantileMetric,
                               DatasetCorrelationsMetric, DatasetDriftMetric,
                               DatasetMissingValuesMetric)
from evidently.report import Report
from prefect import flow, task

from src import read_trips

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s"
)

SEND_TIMEOUT = 10
DATA_FOLDER = Path("../data")
MODEL_FOLDER = Path("../models")

TARGET = "duration"
CATEGORICAL_COLS = ["PULocationID", "DOLocationID"]
NUMERICAL_COLS = ["passenger_count", "trip_distance", "fare_amount", "total_amount"]


def read_reference_data():
    return pd.read_parquet(DATA_FOLDER / "reference_data.parquet")


def read_model():
    with open(MODEL_FOLDER / "model.pkl", "rb") as f:
        return pickle.load(f)


def crate_column_mapping() -> ColumnMapping:
    return ColumnMapping(
        prediction="prediction",
        numerical_features=NUMERICAL_COLS,
        categorical_features=CATEGORICAL_COLS,
        target=None,
    )


def create_report() -> Report:
    return Report(
        metrics=[
            ColumnDriftMetric(column_name="prediction"),
            ColumnQuantileMetric(column_name="fare_amount", quantile=0.5),
            DatasetDriftMetric(),
            DatasetMissingValuesMetric(),
            DatasetCorrelationsMetric(),
        ]
    )


def create_table() -> None:
    CREATE_TABLE_STATEMENT = """
        DROP TABLE IF EXISTS model_metrics;
        CREATE TABLE model_metrics(
            timestamp timestamp,
            prediction_drift float,
            quantile float,
            num_drifted_columns integer,
            share_missing_values float,
            feature_correlation float
        )
    """
    with psycopg.connect(
        "host=localhost port=5432 dbname=test user=postgres password=postgres",
        autocommit=True,
    ) as conn:
        conn.execute(CREATE_TABLE_STATEMENT)

def get_metrics(
    reference_data: pd.DataFrame, new_data: pd.DataFrame, model: Any
) -> dict:
    new_data["prediction"] = model.predict(
        new_data[NUMERICAL_COLS + CATEGORICAL_COLS].fillna(0)
    )
    column_mapping = crate_column_mapping()
    report = create_report()

    report.run(
        reference_data=reference_data,
        current_data=new_data,
        column_mapping=column_mapping,
    )

    result = report.as_dict()
    prediction_drift = result["metrics"][0]["result"]["drift_score"]
    quantile = result["metrics"][1]["result"]["current"]["value"]
    num_drifted_columns = result["metrics"][2]["result"]["number_of_drifted_columns"]
    share_missing_values = result["metrics"][3]["result"]["current"][
        "share_of_missing_values"
    ]
    feature_correlation = result["metrics"][4]["result"]["current"]["stats"]["kendall"][
        "abs_max_prediction_features_correlation"
    ]

    return {
        "prediction_drift": prediction_drift,
        "quantile": quantile,
        "num_drifted_columns": num_drifted_columns,
        "share_missing_values": share_missing_values,
        "feature_correlation": feature_correlation,
    }


@task(retries=3, retry_delay_seconds=5, name="prepare database")
def prep_db():
    with psycopg.connect(
        "host=localhost port=5432 user=postgres password=postgres", autocommit=True
    ) as conn:
        res = conn.execute("SELECT 1 FROM PG_DATABASE WHERE datname='test'")
        if len(res.fetchall()) == 0:
            conn.execute("CREATE DATABASE test;")

        create_table()

@task(retries=3, retry_delay_seconds=5, name="export metrics to postgresql")
def export_metrics_postgresql(cursor, metrics: dict) -> None:
    cursor.execute(
        """
        INSERT INTO model_metrics(
            timestamp, 
            prediction_drift, 
            quantile, 
            num_drifted_columns, 
            share_missing_values, 
            feature_correlation) 
        VALUES (%s, %s, %s, %s, %s, %s)""",
        (
            metrics["timestamp"],
            metrics["prediction_drift"],
            metrics["quantile"],
            metrics["num_drifted_columns"],
            metrics["share_missing_values"],
            metrics["feature_correlation"],
        ),
    )


@flow
def batch_monitoring_backfill(color="green", year="2023", month="3"):
    prep_db()
    last_send = datetime.now() - timedelta(seconds=10)

    reference_data = read_reference_data()
    model = read_model()
    new_data = read_trips(DATA_FOLDER, color, year, month)

    new_data = new_data[
        (new_data["lpep_pickup_datetime"] >= datetime(int(year), int(month), 1, 0, 0))
        & (
            new_data["lpep_pickup_datetime"]
            <= datetime(int(year), int(month) + 1, 1, 0, 0) - timedelta(days=1)
        )
    ]
    dates = sorted(new_data["lpep_pickup_datetime"].dt.date.unique())

    with psycopg.connect(
        "host=localhost port=5432 dbname=test user=postgres password=postgres",
        autocommit=True,
    ) as conn:
        for date in dates:
            new_data_date = new_data[new_data["lpep_pickup_datetime"].dt.date == date]
            metrics = get_metrics(reference_data, new_data_date, model)
            metrics["timestamp"] = date
            with conn.cursor() as cursor:
                export_metrics_postgresql(cursor, metrics)

            new_send = datetime.now()
            seconds_elapsed = (new_send - last_send).total_seconds()
            if seconds_elapsed < SEND_TIMEOUT:
                time.sleep(SEND_TIMEOUT - seconds_elapsed)
            while last_send < new_send:
                last_send = last_send + timedelta(seconds=10)
            logging.info("data sent")


if __name__ == "__main__":
    batch_monitoring_backfill()
