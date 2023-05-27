import pickle
from pathlib import Path
from typing import Any


def save_model(path: Path, model_name: str, model: Any):
    path.mkdir(parents=True, exist_ok=True)
    with open(path / model_name, 'wb') as f:
        pickle.dump(model, f) 