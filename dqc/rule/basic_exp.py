import great_expectations as ge
from dqc.query.index import query_topic_data_by_datetime
from great_expectations.dataset import Dataset


def health_check_topic_by_basic_expectation(topic_name, from_date, to_date):
    print("from_date", from_date)
    print("to_date", to_date)

    data_frame = query_topic_data_by_datetime(topic_name, from_date, to_date)
    df_ge: Dataset = ge.dataset.PandasDataset(data_frame)
    # print(df_ge)
    reuslt = df_ge.expect_table_row_count_to_equal(4612)



    print(df_ge.validate())

    ## load data
    ## attach basic exp
    ## run result

    pass


def health_check_dataset_by_basic_expectation():
    pass
