import json
import pytz
import datetime
import websocket
import pandas as pd
import threading
import requests
from utils import logger
from settings import Config


class Candlesticks(Config):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(self):
        if not self.__initialized:
            super().__init__()
            self.candles = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
            self.__initialized = True

    def __historical__(self):
        print("Retrieving historical data...")

        end_time = datetime.datetime.now()
        end_time = int(end_time.timestamp() * 1000)
        start_time = end_time - (4 * 60 * 60 * 1000)

        url = (
            f'https://fapi.binance.com/fapi/v1/klines?symbol={self.symbol}'
            f'&interval={self.timeframe}&startTime={start_time}&endTime={end_time}'
        )

        data = requests.get(url).json()

        df = pd.DataFrame(data)

        df.columns = ['time',
                      'open',
                      'high',
                      'low',
                      'close',
                      'volume',
                      'close_time',
                      'quote_asset_volume',
                      'number_of_trades',
                      'taker_buy_base_asset_volume',
                      'taker_buy_quote_asset_volume',
                      'ignore']

        df['time'] = pd.to_datetime(df['time'], unit='ms')

        df = df.set_index('time')
        df.index = df.index.tz_localize(pytz.UTC).tz_convert('America/Montevideo')
        df.index = df.index.strftime('%Y-%m-%d %H:%M:%S')

        df = df[['open', 'high', 'low', 'close', 'volume']]

        self.candles = df

    def __on_open__(self, _):
        logger.info('Candlesticks stream opened.')

    def __on_close__(self, _):
        logger.info('Candlesticks stream closed.')

    def __on_message__(self, _, message):
        json_message = json.loads(message)

        candle = json_message['k']
        is_candle_closed = candle['x']

        _open = candle['o']
        _high = candle['h']
        _low = candle['l']
        _close = candle['c']
        _volume = candle['v']

        timestamp_ms = int(candle['t']) / 1000
        current_datetime = datetime.datetime.fromtimestamp(timestamp_ms, tz=pytz.UTC).astimezone(
            pytz.timezone('America/Montevideo')).strftime('%Y-%m-%d %H:%M:%S')

        if self.candles.empty:
            self.candles.loc[current_datetime] = {'open': _open,
                                                  'high': _high,
                                                  'low': _low,
                                                  'close': _close,
                                                  'volume': _volume}
        else:
            if is_candle_closed:
                # if the candle is closed: update the last row
                self.candles.loc[current_datetime] = {'open': _open,
                                                      'high': _high,
                                                      'low': _low,
                                                      'close': _close,
                                                      'volume': _volume}
                # append a empty row to be replaced next time
                self.candles.loc[
                    None
                    ] = {'open': None,
                         'high': None,
                         'low': None,
                         'close': None,
                         'volume': None}
            else:
                # if the candles isn't closed: keep updating the last row
                self.candles.loc[current_datetime] = {'open': _open,
                                                      'high': _high,
                                                      'low': _low,
                                                      'close': _close,
                                                      'volume': _volume}

    def __subscribe__(self):
        self.__historical__()

        ws = websocket.WebSocketApp(self.SOCKET,
                                    on_open=self.__on_open__,
                                    on_close=self.__on_close__,
                                    on_message=self.__on_message__)
        ws.run_forever()

    def stream(self):
        t = threading.Thread(target=self.__subscribe__)
        t.start()

    def fetch(self):
        return self.candles
