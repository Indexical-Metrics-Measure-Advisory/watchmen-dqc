import numpy as np
from pandas import DataFrame
from numba import jit


def __get_first_column_data(df):
    return df.iloc[:, 0]


def init():
    def basic_rule(df: DataFrame, topic_summary):
        topic_summary.factorCount = len(df.columns)
        topic_summary.rowCount = np.int16(__get_first_column_data(df).count()).item()
        return topic_summary
    return basic_rule
