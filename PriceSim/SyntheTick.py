from datetime import datetime
from datetime import timedelta
import random


class SyntheticTick:

    def __init__(self, volatility_mean_pips: float,
                 volatility_std_pips: float,
                 spread_mean_pips: float,
                 spread_std_pips: float):
        self.volatility_mean_pts = volatility_mean_pips / 10000
        self.volatility_std_pts = volatility_std_pips / 10000
        self.spread_mean_pts = spread_mean_pips / 10000
        self.spread_std_pts = spread_std_pips / 10000

        # TODO: Add last calculated tick properties

    def emmit_tick(self, price_i_1: float) -> dict:
        _date_time = datetime.now()
        _date_time_ms = int(datetime.timestamp(_date_time) * 1000)  # date_time in milliseconds since epoch.

        _volatility = random.gauss(self.volatility_mean_pts, self.volatility_std_pts)
        _spread = random.gauss(self.volatility_mean_pts, self.volatility_std_pts)
        _bid = round(random.gauss(price_i_1, _volatility), 5)
        _ask = _bid + _volatility

        _tick = {"date_time_ms": _date_time_ms,
                 "date_time": _date_time,
                 "bid": _bid,
                 "ask": _ask
                 }

        return _tick


class SyntheticOHLC:

    def __init__(self, bar_size,
                 volatility_mean_pips: float,
                 volatility_std_pips: float,
                 spread_mean_pips: float,
                 spread_std_pips: float):

        self.volatility_mean_pts = volatility_mean_pips / 10000
        self.volatility_std_pts = volatility_std_pips / 10000
        self.spread_mean_pts = spread_mean_pips / 10000
        self.spread_std_pts = spread_std_pips / 10000
        self.bar_size = bar_size

    def emmit_ohlc(self, ohlc_i_1: dict) -> dict:
        _open_time = datetime.fromtimestamp(ohlc_i_1["close_time_ms"]/1000)
        _close_time = _open_time + timedelta(seconds=1)

        _open_time_ms = int(datetime.timestamp(_open_time) * 1000)  # open_time in milliseconds since epoch.
        _close_time_ms = int(datetime.timestamp(_close_time) * 1000)  # close_time in milliseconds since epoch.

        _open = ohlc_i_1["close"]
        _close = random.gauss(_open, self.volatility_mean_pts)

        _high = max(_open, _close) + random.gauss(self.volatility_mean_pts, self.volatility_mean_pts)
        _low = min(_open, _close) - random.gauss(self.volatility_mean_pts, self.volatility_mean_pts)

        _ohlc = {"open_time_ms": _open_time_ms,
                 "close_time_ms": _close_time_ms,
                 "open": _open,
                 "high": _high,
                 "low": _low,
                 "close": _close
                 }

        return _ohlc
