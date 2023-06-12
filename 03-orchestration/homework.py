from datetime import date
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_aws import S3Bucket
from prefect_email import EmailServerCredentials, email_send_message
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import Pipeline
from xgboost import XGBRegressor

from src import create_pipeline, process_trips, read_trips, save_model

DATA_FOLDER = "data"
MODEL_FOLDER = "models"


@task(retries=3, retry_delay_seconds=2, name="Read taxi trips data")
def read_trips_task(
    data_folder: str, color: str, year: str, month: str
) -> pd.DataFrame:
    return read_trips(data_folder, color, year, month)


@task()
def process_trips_task(trips: pd.DataFrame) -> pd.DataFrame:
    return process_trips(trips)


@task(log_prints=True)
def save_best_model_task(path: str, model_name: str, pipe: Pipeline) -> None:
    path = Path(path)
    save_model(path, model_name, pipe)


@task()
def markdown_task(rmse: float) -> None:
    markdown_report = f"""# RMSE Report

        ## Summary

        Duration Prediction 

        ## RMSE XGBoost Model

        | Region    | RMSE |
        |:----------|-------:|
        | {date.today()} | {rmse:.2f} |
    """

    create_markdown_artifact(key="duration-model-report", markdown=markdown_report)


@flow()
def main_flow_hw(
    data_folder: str = DATA_FOLDER,
    train_data: tuple[str, ...] = ("green", "2023", "1"),
    val_data: tuple[str, ...] = ("green", "2023", "2"),
) -> None:
    data_folder = Path(data_folder)

    s3_bucket_block = S3Bucket.load("aws-s3")
    s3_bucket_block.download_folder_to_path(from_folder="data", to_folder=data_folder)

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

    model_params = {
        "learning_rate": 0.09585355369315604,
        "max_depth": 30,
        "min_child_weight": 1.060597050922164,
        "objective": "reg:linear",
        "reg_alpha": 0.018060244040060163,
        "reg_lambda": 0.011658731377413597,
        "seed": 42,
    }
    xgb_regressor = XGBRegressor(**model_params)
    model = create_pipeline(xgb_regressor)

    model.fit(X_train, y_train)
    rmse = mean_squared_error(y_val, model.predict(X_val), squared=False)

    save_best_model_task(MODEL_FOLDER, "xgbregressor.pkl", model)
    markdown_task(rmse)

    email_server_credentials = EmailServerCredentials.load(
        "email-server-credentials-block"
    )
    email_send_message(
        email_server_credentials=email_server_credentials,
        subject="Flow successfully completed.",
        msg="Flow successfully completed.",
        email_to=email_server_credentials.username,
    )

    return None


if __name__ == "__main__":
    main_flow_hw()
