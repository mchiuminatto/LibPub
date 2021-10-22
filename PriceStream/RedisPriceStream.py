"""
This library implements price time series streaming using Redis.

Supports different price representation streaming, from tick to OHLC's like
Candle, Range, Renko and TickBars.

Version 1.1.1 -RedisPriceStream.py -  Added price recorder class
"""

import redis
import pandas as pd
from datetime import datetime
import time
from multiprocessing import Process
import logging


CHANNEL_SEPARATOR = ":"
FILE_NAME_SEPARATOR = "_"
FILE_EXTENSION = ".csv"


class PriceRep:
    def __init__(self):
        self.price = dict()

    def parse(self, raw_price):
        pass

    def print_me(self):
        print(self.price)

    def get_price(self) -> dict:
        return self.price


class Tick(PriceRep):

    def __init__(self):
        super().__init__()

    def parse(self, raw_tick):
        #print(raw_tick[0][1], raw_tick[0][1][b"ask"])
        self.price["timestamp"] = int(raw_tick["time"].decode())
        self.price["ask"] = float(raw_tick["ask"].decode())
        self.price["bid"] = float(raw_tick["bid"].decode())
        self.price["time"] = datetime.fromtimestamp(self.price["timestamp"])


class OHLC(PriceRep):
    def __init__(self):
        super().__init__()
        self.open_time = None
        self.open_time_ts = None
        self.close_time = None
        self.close_time_ts = None
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.timestamp = None

    def parse(self, raw_ohlc):
        self.price["timestamp"] = int(raw_ohlc["open_time"])
        self.price["open_time"] = datetime.fromtimestamp(self.price["timestamp"]/1000)
        self.price["close_time_ts"] = int(raw_ohlc["close_time"])
        self.price["close_time"] = datetime.fromtimestamp(self.price["close_time_ts"]/1000)
        self.price["open"] = float(raw_ohlc["open"])
        self.price["high"] = float(raw_ohlc["high"])
        self.price["low"] = float(raw_ohlc["low"])
        self.price["close"] = float(raw_ohlc["close"])


class TickStreamPublisher:

    def __init__(self, host, port, db, bid_key="bid", ask_key="ask", ts_key="date_time_ms"):
        """
        Initializes Tick Publisher

        :param host: redis host
        :param port: redis port at host (6379 default)
        :param db: Number identifying which database to use at redis server
        :param bid_key: bid key name
        :param ask_key: ask key name
        :param ts_key: timestamp key name

        """
        self.client = redis.Redis(host=host, port=port, db=db)
        self.BID_KEY = bid_key
        self.ASK_KEY = ask_key
        self.TS_KEY = ts_key

    def send_tick(self, instrument, bar_size, tick):
        _stream_id = f"{instrument}:{bar_size}"


        self.client.xadd(_stream_id, {"bid": tick[self.BID_KEY], "ask": tick[self.ASK_KEY]}, id=tick[self.TS_KEY])

        return tick[self.TS_KEY]


class OHLCStreamPublisher:
    def __init__(self,
                 host,
                 port,
                 db,
                 open_time_key = "open_time_ms",
                 close_time_key = "close_time_ms",
                 open_key = "open",
                 high_key = "high",
                 low_key = "low",
                 close_key = "close",
                 ):
        """
        Initializes OHLC publisher

        :param host: redis host
        :param port: redis port at host (6379 default)
        :param db: Number identifying which database to use at redis server
        :param open_time_key: open time dictionary key
        :param close_time_key: close time dictionary key
        :param open: open price key
        :param high: high price key
        :param low: low price key
        :param close: close price key


        """
        self.client = redis.Redis(host=host, port=port, db=db)
        self.OPEN_TIME_KEY = open_time_key
        self.CLOSE_TIME_KEY = close_time_key
        self.OPEN_KEY = open_key
        self.HIGH_KEY = high_key
        self.LOW_KEY = low_key
        self.CLOSE_KEY = close_key

    def send_ohlc(self, instrument, bar_size, price_type, ohlc):
        _stream_id = f"{instrument}:{bar_size}:{price_type}"
        self.client.xadd(_stream_id, {"open_time": ohlc[self.OPEN_TIME_KEY],
                                      "close_time": ohlc[self.CLOSE_TIME_KEY],
                                      "open": ohlc[self.OPEN_KEY],
                                      "high": ohlc[self.HIGH_KEY],
                                      "low": ohlc[self.LOW_KEY],
                                      "close": ohlc[self.CLOSE_KEY]},
                                       id=ohlc[self.OPEN_TIME_KEY])


class StreamConsumer:
    """
    Reads data from Price Redis streams.
    """

    def __init__(self, host, port, db, user=None, password=None):
        self.client = redis.Redis(host=host, port=port, db=db, username=user, password=password, decode_responses=True)
        self.last_read_id = "$"


    def read_last_wait(self, stream):
        """
        Wait for the latest update from a Redis Stream.

        The stream could be tick or OHLC

        :param stream: Stream name. Must be an exact name

        :return:
            A price structure: Tick or OHLC depending on the stream type
        """

        _resp = self.client.xread({stream: self.last_read_id}, count=1, block=0)
        _channel = _resp[0][0]
        self.last_read_id = _resp[0][1][0][0]
        _message = _resp[0][1][0][1]

        if "tick" in stream:
            _price_parser = Tick()
        else:
            _price_parser = OHLC()
        _price_parser.parse(_message)

        return _price_parser.get_price()


    def read_last(self, instrument, bar_size):
        _stream_name = f"{instrument}:{bar_size}"
        print("stream :", _stream_name)
        _resp = self.client.xrevrange(_stream_name, max="+", min="-", count=1)
        return _resp

    def read_last_n(self, n, stream):
        """
        Read the top "n" records from the "stream"
        :param n: Number of records to read
        :param stream: Redis stream name
        :return:
            The records read as a stream
        """
        print("stream :", stream)
        _resp = self.client.xrevrange(stream, max="+", min="-", count=n)
        return _resp

    def read_all(self, stream):
        print("stream :", stream)
        _resp = self.client.xrevrange(stream, max="+", min="-")
        return _resp


    def to_pandas(self, messages, cols_to_num=None, col_rename=None) -> pd.DataFrame:
        """
        Transform a raw stream data output to a pandas dataframe with the options of
        explicitly transform columns to number (float) or rename some columns using a mapping
        dictionary.

        :param messages: List of messages dictionaries
        :param cols_to_num: list of columns to convert to number
        :param col_rename: column renaming map with the format:
                            {"<source_col_name_1>":"<new_column_name_1>",
                             "<source_col_name_2>":"<new_column_name_2>",
                              ...}
        :return:
        """
        _df_temp = pd.DataFrame.from_records(messages)
        try:
            _df = pd.json_normalize(_df_temp[1])
        except KeyError as e:
            print("ERROR converting to pandas " + str(e))
            return None

        _df["key"] = _df_temp[0]
        _df.sort_values(by=["key"], inplace=True)
        if cols_to_num is not None:
            _df[cols_to_num] = _df[cols_to_num].astype(float)

        if col_rename is not None:
            _df.rename(columns=col_rename, inplace=True)

        return _df

    def read_between(self, stream_id, start, end):

        _resp = self.client.xrevrange(stream_id, max=end, min=start)
        return _resp


    def consume_stream(self, callback, stream, **params):
        while True:
            _price = self.read_last_wait(stream)
            callback(_price, **params)


class PriceRecorder:

    def __init__(self, host, port, db, output_folder = "./", user=None, password=None):

        self._host = host
        self._port = port
        self._db = db
        self._user = user
        self._password = password
        self._output_folder = output_folder

        self._client = redis.Redis(host=host, port=port, db=db, username=user, password=password, decode_responses=True)
        self._channel_list = None

        self.proc_table = dict()
        self._log_folder = "./log/"



    # region PROPERTIES
    @property
    def channel_list(self):
        return self._channel_list

    # endregion


    def record_price(self, stream):
        # open price dataset if exists
        _base_name = stream.replace(CHANNEL_SEPARATOR, FILE_NAME_SEPARATOR)
        _file_name = _base_name + FILE_EXTENSION
        self.open_log(_base_name)

        logging.info(f"Opening data set {_file_name}")
        df = pd.DataFrame()
        try:
            df = pd.read_csv(f"{self._output_folder}{_file_name}")
        except FileNotFoundError:
            logging.info("File not found, creating a new one named " + _file_name)

        # configure stream consumer instance
        _stc = StreamConsumer(self._host, self._port, self._db, self._user, self._password)

        while True:
            # time.sleep(0.001)
            _price = _stc.read_last_wait(stream)
            logging.info(f"Received {_price}")

            if len(df) == 0:
                df = pd.DataFrame([_price])
            else:
                _df_tmp = pd.DataFrame([_price])
                df = df.append([_df_tmp.iloc[0]])

            df.to_csv(f"{self._output_folder}{_file_name}")

    def scan_channels(self, filter_pattern: str="*", sorted=False) -> int:
        """
        Get the lst of channels for the filter_pattern specified at the connection
        pointed by self._client.

        :param filter_pattern:

            Optional, default="*", all channels
            Pattern to filter the channels.

        :param sorted:

            Optional. default=False. No sorting,
            True will return the list alphabetically sorted

        :return:
            int: Number of channels found
        """

        print(filter_pattern)
        self._channel_list = self._client.keys(filter_pattern)
        if sorted:
            self._channel_list.sort()

        return len(self._channel_list)

    def start_recording(self):

        _channels = self.scan_channels()
        print("Channels ", _channels)

        for _i in range(_channels):
            _channel = self._channel_list[_i]
            print(f"Spawning channel {_channel} recording")
            _p = Process(target=self.record_price, args=(_channel, self._host, self._port, self._db, self._output_folder))
            _p.start()
            _p.join()
            self.proc_table[_channel] = _p.pid
            print(f"Started {_channel} - PID: {self.proc_table[_channel]} ")
            logging.info(f"Started {_channel} - PID: {self.proc_table[_channel]} ")

    def open_log(self, base_name):
        _date = datetime.today().strftime("%Y%m%d")
        _log_file_name = self._log_folder + _date + "_" + base_name + "_.log"
        logging.basicConfig(filename=_log_file_name, format="%(asctime)s %(levelname)s:%(message)s",
                            level=logging.INFO)


