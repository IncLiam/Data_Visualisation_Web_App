from binance.client import Client
import pandas as pd
import sqlite3
import constants
from datetime import datetime, timedelta
import requests

client = Client(constants.API_KEY, constants.SECRET_KEY)


def klines_to_df(klines):
    """ transforms and filters klines from binance into OHLCV pandas dataframe """

    data_f = pd.DataFrame(klines)
    data_f.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'closeTime',
                      'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol', 'ignore']
    # Get rid of columns we do not need
    data_f = data_f.drop(['closeTime', 'quoteAssetVolume', 'numberOfTrades',
                          'takerBuyBaseVol', 'takerBuyQuoteVol', 'ignore'], axis=1)
    # as timestamp is returned in ms, convert this back to proper timestamps.
    data_f["datetime"] = pd.to_datetime(data_f.datetime, unit='ms').dt.strftime("%Y-%m-%d %H:%M:%S")
    data_f.set_index('datetime', inplace=True)
    # convert strings to numbers
    data_f["open"] = pd.to_numeric(data_f["open"])
    data_f["high"] = pd.to_numeric(data_f["high"])
    data_f["low"] = pd.to_numeric(data_f["low"])
    data_f["close"] = pd.to_numeric(data_f["close"])
    data_f["volume"] = pd.to_numeric(data_f["volume"])
    return data_f


# initialise table with past day of data
past_day_df = klines_to_df(client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC"))
conn = sqlite3.connect(f"../app_storage/database.db", check_same_thread=False)
past_day_df.iloc[:-1].to_sql("BTCUSDT", con=conn, index=True, if_exists='replace')
conn.commit()


while True:
    last_row_table = pd.read_sql_query(f"SELECT * FROM BTCUSDT ORDER BY datetime DESC LIMIT 1;", conn)
    now_time = datetime.utcnow()
    if now_time - datetime.strptime(last_row_table["datetime"].iloc[-1], '%Y-%m-%d %H:%M:%S') < timedelta(minutes=2):
        continue

    latest_df = pd.DataFrame()

    try:
        latest_df = klines_to_df(client.get_historical_klines(
            "BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, last_row_table["datetime"].iloc[-1]))
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        print("error", e, "occurred")
        continue

    latest_df.iloc[1:-1].to_sql("BTCUSDT", con=conn, index=True, if_exists='append')
    conn.commit()
