import pandas as pd


# collect historic data and store it in each csv by month or by date
def collect_data(ticker):
    columns = [
        "open_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "close_time",
        "quote_asset_vol",
        "num_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore_column"
    ]
    months = ["2018-01", "2018-02", "2018-03", "2018-04", "2018-05", "2018-06", "2018-07"
             , "2018-08", "2018-09", "2018-10", "2018-11", "2018-12", "2019-01"
             , "2019-02", "2019-03", "2019-04", "2019-05", "2019-06", "2019-07"
             , "2019-08", "2019-09", "2019-10", "2019-11", "2019-12", "2020-01"
             , "2020-02", "2020-03", "2020-04", "2020-05", "2020-06", "2020-07"
             , "2020-08", "2020-09", "2020-10", "2020-11", "2020-12", "2021-01"
             , "2021-02", "2021-03", "2021-04", "2021-05"]
    days = ["2021-06-01", "2021-06-02", "2021-06-03", "2021-06-04", "2021-06-05",
            "2021-06-06", "2021-06-07", "2021-06-08", "2021-06-09", "2021-06-10",
            "2021-06-11", "2021-06-12", "2021-06-13", "2021-06-14", "2021-06-15",
            "2021-06-16", "2021-06-17", "2021-06-18", "2021-06-19", "2021-06-20",
            "2021-06-21"]

    for i in months:
        url = f"https://data.binance.vision/data/spot/monthly/klines/{ticker}/1m/{ticker}-1m-{i}.zip"
        df = pd.read_csv(url, header=None)
        df.to_csv(f'historic_data/{ticker}_1m/{i}.csv', header=columns)


ticker = "BTCUSDT".upper()

collect_data(ticker)

