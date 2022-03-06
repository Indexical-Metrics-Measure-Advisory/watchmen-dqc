import datetime
from typing import Tuple, Optional, Dict, Union

import arrow
import dask.dataframe as dd
import pandas as pd

from dqc.adhoc.constants import FactorType
from dqc.config.config import settings


def get_from_to_date(statistical_interval: Optional[str],
                     to_date: Optional[datetime.datetime] = None) -> Tuple[arrow.Arrow, arrow.Arrow]:
    if to_date:
        end_date = arrow.get(to_date)
    else:
        end_date = arrow.now()
    return get_date_range_with_end_date(statistical_interval, end_date)


def get_date_range_with_end_date(statistical_interval: Optional[str],
                                 end_date: arrow.Arrow) -> Tuple[arrow.Arrow, arrow.Arrow]:
    # end_date = end_date.shift(days=1)
    if statistical_interval:
        if statistical_interval == "daily":
            # start = end_date.shift(days=-1)
            return end_date.floor('day'), end_date.ceil('day')
        elif statistical_interval == "monthly":
            start = end_date.shift(months=-1)
            return start.floor('day'), end_date.ceil('day')
        elif statistical_interval == "weekly":
            start = end_date.shift(weeks=-1)
            return start.floor('day'), end_date.ceil('day')
    else:
        # start = end_date.shift(days=-1)
        return end_date.floor('day'), end_date.ceil('day')


def build_data_frame(rows, columns, data_type: Dict) -> Union[pd.DataFrame, dd.DataFrame]:
    if settings.DATAFRAME_TYPE == "pandas":
        data_frame = pd.DataFrame(rows, columns=columns)
        return data_frame.astype(data_type)
    elif settings.DATAFRAME_TYPE == "dask":
        data_frame = dd.DataFrame(rows, columns=columns)
        return data_frame.astype(data_type)


def convert_pandas_type(factor_type) -> str:
    if factor_type in [FactorType.NUMBER, FactorType.UNSIGNED, FactorType.SEQUENCE]:
        return "float64"
    elif factor_type in [FactorType.YEAR, FactorType.HALF_YEAR, FactorType.QUARTER, FactorType.MONTH,
                         FactorType.HALF_MONTH, FactorType.TEN_DAYS, FactorType.WEEK_OF_YEAR,
                         FactorType.WEEK_OF_MONTH, FactorType.HALF_WEEK, FactorType.DAY_OF_MONTH,
                         FactorType.DAY_OF_WEEK, FactorType.DAY_KIND, FactorType.HOUR,
                         FactorType.HOUR_KIND, FactorType.MINUTE, FactorType.SECOND, FactorType.MILLISECOND,
                         FactorType.AM_PM]:
        return "float64"
    elif factor_type in [FactorType.DATE, FactorType.DATETIME, FactorType.FULL_DATETIME, FactorType.DATE_OF_BIRTH]:
        return "datetime64"
    elif factor_type == FactorType.BOOLEAN:
        return "bool"
    else:
        return "object"


def df_series_is_str(df_series) -> bool:
    return df_series.apply(type).eq(str).all()
