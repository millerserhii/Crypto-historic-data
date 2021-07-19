import psycopg2
import pandas as pd
import os


conn = psycopg2.connect(
    host=os.environ['host'],
    database=os.environ['database'],
    user=os.environ['user'],
    password=os.environ['password'])


dates = ["2018-01", "2018-02", "2018-03", "2018-04", "2018-05", "2018-06", "2018-07"
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

for i in days:
    cur = conn.cursor()
    data = pd.read_csv(f'historic_data/BTCUSDT_1m/daily data/{i}.csv')
    df = pd.DataFrame(data, columns=["open_time", "open_price", "high_price", "low_price", "close_price", "volume",
                                      "close_time", "quote_asset_vol", "num_trades", "taker_buy_base",
                                      "taker_buy_quote", "ignore_column"])
    for row in df.itertuples():
        cur.execute(""" SELECT setval('btcusdt_1m_id_seq', max(id)) FROM btcusdt_1m;
                        INSERT INTO btcusdt_1m (open_time, open_price, high_price, low_price, close_price,
                                                    volume, close_time, quote_asset_vol, num_trades, taker_buy_base,
                                                    taker_buy_quote, ignore_column)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                    (row.open_time, row.open_price, row.high_price, row.low_price, row.close_price,
                     row.volume, row.close_time, row.quote_asset_vol, row.num_trades, row.taker_buy_base,
                     row.taker_buy_quote, row.ignore_column))

    conn.commit()
    cur.close()

conn.close()
