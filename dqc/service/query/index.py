import pandas as pd

from dqc.presto.presto_client import get_connection
from dqc.rule.utils.factor_utils import find_factor_by_name, __convert_pandas_type


def query_topic_data_by_datetime(topic_name, from_datetime, to_datetime, topic=None):
    ## count data


    ##TODO between datetime
    topic_sql = "select * from {0} where update_time_ between timestamp '{1}' and  timestamp '{2}'".format(
        __build_topic_name(topic_name), from_datetime.format('YYYY-MM-DD'), to_datetime.format('YYYY-MM-DD'))

    print(topic_sql)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(topic_sql)

    rows = cur.fetchall()
    columns = list([desc[0] for desc in cur.description])
    df = pd.DataFrame(rows, columns=columns)

    # print(.dtypes)

    print(df)

    return convert_df_dtype(df, topic)


def convert_df_dtype(df, topic):
    dtype_dict = {}
    # print(df.columns)
    for column in df.columns:
        factor = find_factor_by_name(topic["factors"], column)
        if factor is not None:
            dtype_dict[column] = __convert_pandas_type(factor["type"])

    return df.astype(dtype_dict)


def __build_topic_name(topic_name):
    return "topic_" + topic_name


def query_topic_data_count_by_datetime(topic, from_datetime, to_datetime):
    topic_sql = "select count(*) from {0} ".format(__build_topic_name(topic["name"]))
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(topic_sql)
    row = cur.fetchone()
    return row[0]
