import websocket
import json
import psycopg2
import os


ticker = "BTCUSDT".lower()
socket = f'wss://stream.binance.com:9443/ws/{ticker}@kline_1m'

try:
    conn = psycopg2.connect(
            host=os.environ['host'],
            database=os.environ['database'],
            user=os.environ['user'],
            password=os.environ['password'])

except Exception as e:
    print(e)


def on_message(ws, message):
    open_time = json.loads(message)['k']['t']
    close_time = json.loads(message)['k']['T']
    open_price = json.loads(message)['k']['o']
    high = json.loads(message)['k']['h']
    low = json.loads(message)['k']['l']
    close_price = json.loads(message)['k']['c']
    volume = json.loads(message)['k']['v']
    quote_vol = json.loads(message)['k']['q']
    trades_num = json.loads(message)['k']['n']
    taker_buy_base = json.loads(message)['k']['V']
    taker_buy_quote = json.loads(message)['k']['Q']
    ignore = json.loads(message)['k']['B']

    try:
        cur = conn.cursor()
        cur.execute(""" SELECT setval('btcusdt_1m_id_seq', max(id)) FROM btcusdt_1m;
                        INSERT INTO btcusdt_1m (open_time, open_price, high_price, low_price, close_price,
                                                volume, close_time, quote_asset_vol, num_trades, taker_buy_base,
                                                taker_buy_quote, ignore_column) 
                                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                    (open_time, open_price, high, low, close_price, volume, close_time, quote_vol, trades_num,
                     taker_buy_base, taker_buy_quote, ignore))
        conn.commit()
        cur.close()
    except Exception as e:
        print(e)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")
    conn.close()


ws = websocket.WebSocketApp(socket, on_message=on_message, on_close=on_close)

if __name__ == "__main__":
    ws.run_forever()
