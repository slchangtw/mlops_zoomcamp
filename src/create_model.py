from sklearn.feature_extraction import DictVectorizer
from sklearn.pipeline import Pipeline


class DictTransformer:
    def transform(self, X, y=None, **fit_params):
        return X.to_dict(orient="records")

    def fit(self, X, y=None, **fit_params):
        return self


def create_pipeline(predictor) -> Pipeline:
    return Pipeline(
        [
            ("dict_transformer", DictTransformer()),
            ("vectorizer", DictVectorizer()),
            ("predictor", predictor),
        ]
    )
