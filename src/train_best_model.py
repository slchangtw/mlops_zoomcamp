import mlflow
import optuna
import pandas as pd
from optuna.integration.mlflow import MLflowCallback
from sklearn.metrics import mean_squared_error
from xgboost import XGBRegressor

from config import MLFLOW_CONFIG
from create_model import create_pipeline

RANDOM_STATE = 42


def train_best_xgbregressor(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
):
    mlflc = MLflowCallback(
        tracking_uri=MLFLOW_CONFIG["tracking_uri"],
        metric_name="rmse_val",
    )

    @mlflc.track_in_mlflow()
    def objective(trial):
        params = {
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "eta": trial.suggest_float("eta", 0.01, 0.4, log=True),
            "alpha": trial.suggest_float("alpha", 0.01, 5, log=True),
            "lambda": trial.suggest_float("lambda", 0.01, 5, log=True),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
        }
        pipe = create_pipeline(XGBRegressor(**params, random_state=RANDOM_STATE))
        pipe.fit(X_train, y_train)

        rmse = mean_squared_error(y_val, pipe.predict(X_val), squared=False)

        mlflow.log_params(params)
        mlflow.log_metric("rmse_val", rmse)
        return rmse

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=10, gc_after_trial=True, callbacks=[mlflc])

    return create_pipeline(XGBRegressor(**study.best_params))
