from datetime import datetime

import pandas as pd
import pytest
from predict_duration import export_data, make_predictions, prepare_data

AWS_ENDPOINT_URL = "http://localstack:4566"
CATEGORICAL_COLS = ["PULocationID", "DOLocationID"]


@pytest.fixture(scope="session", name="trips")
def fixture_trips():
    def dt(hour, minute, second=0):
        return datetime(2022, 1, 1, hour, minute, second)

    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2), dt(1, 10)),
        (1, 2, dt(2, 2), dt(2, 3)),
        (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    columns = [
        "PULocationID",
        "DOLocationID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]

    return pd.DataFrame(data, columns=columns)


def test_prepare_data(trips):
    assert prepare_data(trips, categorical=CATEGORICAL_COLS).shape == (3, 5)


def test_make_predictions(trips):
    trips = prepare_data(trips, categorical=CATEGORICAL_COLS)
    y_pred = make_predictions(trips, categorical=CATEGORICAL_COLS)
    assert pytest.approx(sum(y_pred), 0.01) == 31.51


def test_export_data(trips: pd.DataFrame):
    output_path = "s3://nyc-duration/trips.parquet"
    options = {"client_kwargs": {"endpoint_url": AWS_ENDPOINT_URL}}
    export_data(trips, output_path, options=options)
    