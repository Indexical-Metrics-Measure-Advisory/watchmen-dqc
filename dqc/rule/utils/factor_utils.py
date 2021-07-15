import logging
from datetime import datetime, date

import arrow
import pandas as pd

from dqc.model.analysis.monitor_rule import MonitorRule

log = logging.getLogger("app." + __name__)


def __check_date_time(cell):
    try:
        if isinstance(cell, str):
            arrow.get(cell)
            return True
        elif pd.isnull(cell):
            log.warning("date_time_dropoff check data is empty")
            return False
        elif isinstance(cell, date):
            return True
        elif isinstance(cell, datetime):
            return True
        else:
            return False
    except Exception as e:
        log.error(e)


def __try_convert_pandas_type(df_series, pandas_type):
    flag: bool = False
    try:
        df_series.astype(pandas_type)
        flag = True
    except :
        flag = False
        print("convert_df_dtype error {}".format(pandas_type))
    finally:
        return flag


def find_factor(factor_list, factor_id):
    factor_filtered = filter(lambda factor: factor["factorId"] == factor_id,
                             factor_list)

    return factor_filtered


def find_factor_by_name(factor_list, factor_name):
    factor_filtered = list(filter(lambda factor: factor["name"].lower() == factor_name,
                                  factor_list))
    if factor_filtered:
        return factor_filtered[0]
    else:
        return None


def __convert_pandas_type(factor_type):
    if factor_type in ["number", "unsigned"]:
        return "float64"
    elif factor_type in ["sequence", "year", "half-year", "quarter", "month", "half-month", "ten-days", "week-of-year",
                         "week-of-month", "half-week", "day-of-month", "day-of-week", "day-kind", "hour", "hour-kind",
                         "minute", "second", "millisecond", "am-pm"]:
        return "int64"
    elif factor_type in ["datetime", "full-datetime", "date", "data-of-birth"]:
        return "datetime64"
    elif factor_type == "boolean":
        return "bool"
    else:
        return "object"


def check_date_type(df_series, factor_type):
    return __try_convert_pandas_type(df_series, __convert_pandas_type(factor_type))


def check_is_empty(df_series, rule=None, factor=None):
    return df_series.empty


def check_max_in_range(df_series, rule, factor=None):
    df_max = df_series.max()
    return check_value_in_range(df_max, rule)


def check_common_value_in_range(df_series,rule,factor=None):
    common_value = df_series.mode().loc[0]
    return check_value_in_range(common_value,rule)


def check_min_in_range(df_series, rule, factor=None):
    df_min = df_series.min()
    return check_value_in_range(df_min, rule)


def check_median_in_range(df_series, rule):
    df_med = df_series.median()
    return check_value_in_range(df_med,rule)


def check_avg_in_range(df_series,rule):
    df_avg = df_series.avg()
    return check_value_in_range(df_avg,rule)


def check_std_in_range(df_series,rule):
    df_std = df_series.std()
    return check_value_in_range(df_std, rule)


def check_quantile_in_range(df_series,rule):
    df_quantile = df_series.quantile()
    return check_value_in_range(df_quantile,rule)


def check_value_in_range(value, rule):
    range_min = int(rule.params.min)
    range_max = int(rule.params.max)
    return range_min < value < range_max


def check_value_range(df_series, rule: MonitorRule = None, factor=None):
    range_min = int(rule.params.min)
    range_max = int(rule.params.max)
    return df_series.between(range_min, range_max).all()


def check_value_match_type(df_series, factor_type):
    if factor_type == "unsigned":
        return (df_series >= 0).all()
    elif factor_type == "date" or factor_type == "datetime":
        return check_date_type(df_series, factor_type)
    elif factor_type == "minute":
        return df_series.between(0, 59).all()
    elif factor_type == "year":
        return df_series.between(1000, 3000).all()
    elif factor_type == "half-year":
        return (df_series.between(1, 2)).all()
    elif factor_type == "sequence":
        return __try_convert_pandas_type(df_series, __convert_pandas_type(factor_type))
    elif factor_type == "number":
        return __try_convert_pandas_type(df_series, __convert_pandas_type(factor_type))
    elif factor_type == "quarter":
        return (df_series.between(1, 4)).all()
    elif factor_type == "day-of-month":
        return (df_series.between(1, 31)).all()

    # quarter

    # month
    # half - month
    # half-month
    # week-of-year
    # week-of-month

    # half-week

    # day-of-month

    # day-of-week

    # day-kind

    # hour

    ## TODO more type support

    else:
        return None
