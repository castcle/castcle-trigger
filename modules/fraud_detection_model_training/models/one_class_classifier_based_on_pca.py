import numpy as np
import pandas as pd
from sklearn.decomposition import PCA


class OneClassClassifierBasedOnPCA:
    def __init__(self):
        self.model_name = "one-class classifier based on PCA"
        self.pca = PCA(n_components=1)
        self.upper_bound = None
        self.lower_bound = None

    def fit(self, X: pd.DataFrame):
        self.pca.fit(X)
        generated_X = pd.DataFrame(
            self.pca.inverse_transform(self.pca.transform(X)),
            index=X.index,
            columns=X.columns
        )
        distances = np.sqrt(((X - generated_X) ** 2).sum(axis=1))
        max_distance = distances.max()
        min_distance = distances.min()
        bound_range = (max_distance - min_distance)
        self.upper_bound = max_distance + (bound_range * 0.1)
        self.lower_bound = min_distance - (bound_range * 0.1)

        return self

    def predict(self, X: pd.DataFrame) -> pd.Series:
        generated_X = pd.DataFrame(
            self.pca.inverse_transform(self.pca.transform(X)),
            index=X.index,
            columns=X.columns
        )
        distances = np.sqrt(((X - generated_X) ** 2).sum(axis=1))
        predicted_values = (
                (distances > self.upper_bound) |
                (distances < self.lower_bound)
        ).astype(int)

        return predicted_values
