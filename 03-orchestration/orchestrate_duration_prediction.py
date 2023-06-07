from pathlib import Path

import pandas as pd
from prefect import flow, task

from src import prcoess_trips, read_trips, train_best_xgbregressor

DATA_FOLDER = "../data"

@task(retries=3, retry_delay_seconds=2, name="Read taxi trips data")
def read_trips_task(
    data_folder: str, color: str, year: str, month: str
) -> pd.DataFrame:
    return read_trips(data_folder, color, year, month)


@task()
def process_trips_task(trips: pd.DataFrame) -> pd.DataFrame:
    return prcoess_trips(trips)


@task()
def train_best_xgbregressor_task(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
) -> pd.DataFrame:
    return train_best_xgbregressor(X_train, y_train, X_val, y_val)


@flow()
def main_flow(
    data_folder: str=DATA_FOLDER,
    train_data: tuple[str] = ("green", "2022", "1"),
    val_data: tuple[str] = ("green", "2022", "2"),
):  
    data_folder = Path(data_folder)

    trips_train = read_trips_task(data_folder, *train_data)
    trips_val = read_trips_task(data_folder, *val_data)

    trips_train = process_trips_task(trips_train)
    trips_val = process_trips_task(trips_val)

    target = "duration"
    categorical_cols = ["PU_DO"]
    numerical_cols = ["trip_distance"]
    used_cols = categorical_cols + numerical_cols

    X_train = trips_train[used_cols]
    y_train = trips_train[target]

    X_val = trips_val[used_cols]
    y_val = trips_val[target]

    pipe = train_best_xgbregressor_task(X_train, y_train, X_val, y_val)

    return pipe


if __name__ == "__main__":
    main_flow()
