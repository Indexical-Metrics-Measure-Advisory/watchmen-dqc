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
    except:
        flag = False
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


def check_is_empty(df_series, rule=None):
    return df_series.empty


def check_value_range(df_series, rule: MonitorRule = None,factor=None):
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
