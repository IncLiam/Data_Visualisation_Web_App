from binance.client import Client
import pandas as pd
import sqlite3


client = Client("X9RSMKuh1XgsmegJnQ1ZsvnZB0sJtdQHpWorth6A18RNIgsQ7AAY5wMxLWfYeLxF",
                "YkVycdMY1nwlUD7aHhtUO8mpZIKSEGy3KYMhes9nu7NNRzqzlSFto4qljDLy4MHi")

GBP_balance = client.get_asset_balance("GBP")
print(GBP_balance)


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


past_day_df = klines_to_df(client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC"))

conn = sqlite3.connect(f"../app_storage/bot_database.db", check_same_thread=False)
past_day_df.iloc[:-1].to_sql("BTCUSDT", con=conn, index=True, if_exists='replace')
dataframe = pd.read_sql_query(f"SELECT * FROM BTCUSDT ORDER BY datetime DESC LIMIT 1;", conn)
print(f"actual last row in table is :\n {dataframe}")
conn.commit()

last_minute_table = past_day_df.iloc[-2].name

while True:
    past_5minute_df = klines_to_df(client.get_historical_klines(
        "BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, "5 minutes ago UTC"))
    if last_minute_table != f"{past_5minute_df.iloc[-2].name}":
        past_5minute_df.iloc[[-2]].to_sql("BTCUSDT", con=conn, index=True, if_exists='append')
        conn.commit()
        dataframe = pd.read_sql_query(f"SELECT * FROM BTCUSDT ORDER BY datetime DESC LIMIT 1;", conn)
        print(f"5 mins kline as df is:\n {past_5minute_df}")
        last_minute_table = past_5minute_df.iloc[-2].name
        print(f"updated last row in table with {last_minute_table}")
        print(f"checking, actual last row in table is :\n {dataframe}")
