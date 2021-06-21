import logging
from datetime import datetime, date

import arrow
import pandas as pd

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


# def __check_minute(cell):
#     # print(cell)
#     if cell.between(0,59):
#         return True
#     else:
#         return False


def check_value_match_type(df_series, factor_type):
    if factor_type == "unsigned":
        return (df_series >= 0).all()
    elif factor_type == "date" or factor_type == "datetime":
        return (df_series.apply(__check_date_time)).all()
    elif factor_type == "minute":
        return df_series.between(0,59).all()
    elif factor_type == "year":
        return df_series.between(1000,3000).all()
    elif factor_type == "half-year":
        return (df_series.between(1,2)).all()
    # quarter
    #month
    #half - month
    # half-month
    # week-of-year
    # week-of-month

    # half-week

    # day-of-month

    # day-of-week

    # day-kind

    # hour










    else:
        return None
