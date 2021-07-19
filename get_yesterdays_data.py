from datetime import datetime, timedelta
import pandas as pd
import psycopg2


# get yesterday's data and push it to DB
def get_yesterday_data(ticker):
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    url = f"https://data.binance.vision/data/spot/daily/klines/{ticker}/1m/{ticker}-1m-{yesterday}.zip"
    df = pd.read_csv(url, header=None)
    columns = ["open_time", "open_price", "high_price", "low_price", "close_price", "volume", "close_time",
               "quote_asset_vol", "num_trades", "taker_buy_base", "taker_buy_quote", "ignore_column"]

    df.to_csv(f'historic_data/{ticker}_1m/Daily data/{yesterday}.csv', header=columns)

    data = pd.read_csv(f'historic_data/BTCUSDT_1m/Daily data/{yesterday}.csv')
    df = pd.DataFrame(data, columns=columns)

    conn = psycopg2.connect(
        host="psql.topfol.io",
        database="project2",
        user="backend",
        password="pt$8E0qNFhICnD")

    cur = conn.cursor()
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


get_yesterday_data("BTCUSDT")
