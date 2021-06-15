from prophet import Prophet


#
# path = ""
# df = pd.read_csv("../../../data/test.csv")
#
# print(df)
#
# # m = Prophet(daily_seasonality=True, weekly_seasonality=True)
# # m.add_seasonality(name='weekly', period=7, fourier_order=3, prior_scale=0.1)
# # # m.add_seasonality(name='weekly', period=7, fourier_order=3, prior_scale=0.1)
# # m.fit(df)


# fig1 = m.plot(forecast)


def convert_data_to_time_dataframe():
    ## load current year date to pd
    pass


def run_prophet_train(df):
    m = Prophet(daily_seasonality=True, weekly_seasonality=True)
    m.add_country_holidays(country_name='CN')
    m.add_seasonality(name='weekly', period=7, fourier_order=3, prior_scale=0.1)
    m.fit(df)

    pass


def predict_future_data(m):
    future = m.make_future_dataframe(periods=30)
    # m.add_seasonality(name='weekly', period=7, fourier_order=3, prior_scale=0.1)
    # print(future)

    forecast = m.predict(future)
    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])
    pass


def check_prediction_results_and_alarm(current_day):
    ## check current_day with prediction result

    pass
