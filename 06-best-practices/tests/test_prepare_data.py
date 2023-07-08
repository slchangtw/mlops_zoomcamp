from datetime import datetime

import pandas as pd
from predict_duration import prepare_data


def test_prepare_data():
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
    categorical_cols = ["PULocationID", "DOLocationID"]

    df = pd.DataFrame(data, columns=columns)

    assert prepare_data(df, categorical_cols).shape == (3, 5)
    
