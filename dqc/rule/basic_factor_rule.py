# import great_expectations as ge
# import numpy as np
# from great_expectations.dataset import Dataset
# from pandas import DataFrame
#
# from dqc.model.analysis.factor_summary import FactorSummary
#
#
# def init():
#     # @jit(nopython=True,pandas=True)
#     def basic_factor_rule(df: DataFrame, factor_summary: FactorSummary):
#         df_ge: Dataset = ge.dataset.PandasDataset(df)
#         nonnull_count = np.int16(df_ge.get_column_nonnull_count(factor_summary.factorName)).item()
#         row_count = np.int16(df_ge.get_row_count()).item()
#         factor_summary.numberOfNull = row_count - nonnull_count
#         factor_summary.valueMaximum = np.int16(df_ge.get_column_max(factor_summary.factorName)).item()
#         factor_summary.valueMinimum = np.int16(df_ge.get_column_min(factor_summary.factorName)).item()
#         factor_summary.nullPercent = (row_count - nonnull_count) / row_count
#         factor_summary.distinctValueNumber = np.int16(df_ge.get_column_unique_count(factor_summary.factorName)).item()
#         return factor_summary
#
#     return basic_factor_rule
