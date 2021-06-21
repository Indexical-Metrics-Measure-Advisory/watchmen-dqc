import pandas as pd

from dqc.presto.presto_client import get_connection


def query_topic_data_by_datetime(topic_name, from_datetime, to_datetime):
    ## count data

    ##TODO between datetime
    topic_sql = "select * from {0} ".format(topic_name)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(topic_sql)

    rows = cur.fetchall()
    columns = list([desc[0] for desc in cur.description])
    df = pd.DataFrame(rows, columns=columns)

    return df



