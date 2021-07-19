import arrow


def get_date_range(statistical_interval: str):
    end_date = arrow.now()
    return get_date_range_with_end_date(statistical_interval, end_date)


def get_date_range_with_end_date(statistical_interval, end_date):
    # TODO
    # end_date = end_date.shift(days=1)
    if statistical_interval == "daily":
        start = end_date.shift(days=-1)
        return start, end_date
    elif statistical_interval == "monthly":
        start = end_date.shift(months=-1)
        return start, end_date
    elif statistical_interval == "weekly":
        start = end_date.shift(weeks=-1)
        return start, end_date
