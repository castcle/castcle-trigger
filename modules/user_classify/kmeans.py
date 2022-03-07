from sklearn.cluster import KMeans
import pandas as pd

class KmeansClusteringModel(KMeans):
    def __init__(self, df: pd.DataFrame, n_clusters: int):
        super().__init__(n_clusters=n_clusters)
        self.df = df
#        self.n_clusters = n_clusters
        self.model = KMeans(n_clusters)

    #! Not work
    def _fit(self):
        model = self.model.fit(self.df)
        return model