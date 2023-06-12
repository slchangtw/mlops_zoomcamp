import numpy as np
import pandas as pd


def process_trips(trips: pd.DataFrame) -> pd.DataFrame:
    trips = trips.copy()
    try:
        pickup_col = [col for col in trips.columns if col.endswith("pickup_datetime")][
            0
        ]
        dropoff_col = [
            col for col in trips.columns if col.endswith("dropoff_datetime")
        ][0]
    except IndexError:
        raise ValueError("Could not find pickup and dropoff columns.")
    trips["duration"] = trips[dropoff_col] - trips[pickup_col]
    trips["duration"] = trips["duration"].apply(lambda td: td.total_seconds() / 60)
    print(f"Standard deviation of duration: {np.std(trips['duration']):.2f}")

    outliers_mask = (trips["duration"] >= 1) & (trips["duration"] <= 60)
    print(
        f"Fraction of the records left after dropping the outliers: "
        f"{sum(outliers_mask) / len(trips)}"
    )
    trips = trips[outliers_mask]

    trips["PULocationID"] = trips["PULocationID"].astype(str)
    trips["DOLocationID"] = trips["DOLocationID"].astype(str)
    trips["PU_DO"] = trips["PULocationID"] + "_" + trips["DOLocationID"]

    return trips
