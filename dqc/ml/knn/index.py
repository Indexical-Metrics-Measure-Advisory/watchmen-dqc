import numpy as np
import pandas as pd
from pandas import DataFrame
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler


def __clean_data(df: DataFrame, factor_list=[]):
    str_columns = []
    for column in df.columns:
        column_name = column.replace(" ", "")
        # print(column_name)
        # print(df[column].dtype)
        if df[column].dtype == np.float64 or df[column].dtype == np.int64:
            if column_name.endswith("Id") or column_name.endswith("id") or column_name.endswith("row"):
                df = df.drop(column, 1)
        elif column_name.endswith("Date") or column_name.endswith("date") or column_name.endswith(
                "id") or column_name.endswith("time") or column_name.endswith("row"):
            df = df.drop(column, 1)
        else:
            str_columns.append(column)

    print(df)
    print(str_columns)
    unstandardized_data = pd.get_dummies(
        df,
        columns=str_columns,
        # drop_first=True
    )

    cols_to_standardize = [
        column for column in df.columns
        if column not in str_columns
    ]

    print(cols_to_standardize)
    if cols_to_standardize:
        data_to_standardize = unstandardized_data[cols_to_standardize]
    else:
        data_to_standardize = unstandardized_data

    print(data_to_standardize)
    scaler = StandardScaler().fit(data_to_standardize)
    # Standardize the data
    standardized_data = unstandardized_data.copy()
    standardized_columns = scaler.transform(data_to_standardize)
    print("standardized_columns", standardized_columns)
    if cols_to_standardize:
        standardized_data[cols_to_standardize] = standardized_columns
    else:
        standardized_data = standardized_columns
    return standardized_data


# @jit(nopython=True,pandas=True)
def run_knn(df):
    # from sklearn.preprocessing import LabelBinarizer

    kmeans_data_frame = __clean_data(df)
    sil_score_max = -1  # this is the minimum possible score
    # results =
    # std = StandardScaler().fit_transform(kmeans_data_frame)
    # print('Sample of data to use:')
    print(kmeans_data_frame)
    # print('')
    # print(std.head())
    for n_clusters in range(2, 10):
        model = KMeans(n_clusters=n_clusters, init='k-means++', max_iter=50, n_init=2)
        labels = model.fit_predict(kmeans_data_frame)
        sil_score = silhouette_score(kmeans_data_frame, labels)
        print("The average silhouette score for %i clusters is %0.2f" % (n_clusters, sil_score))
        if sil_score > sil_score_max:
            sil_score_max = sil_score
            best_n_clusters = n_clusters

    return best_n_clusters
