from LibTqtk.Connectors import PriceFileConverter
import sys
import numpy as np
import pandas as pd


class Renko(PriceFileConverter.PriceFileConverter):

    def __init__(self, config_folder, config_file, tick_folder, parse):
        super().__init__(config_folder, config_file,  parse)
        self.df_t = None  # tick dataset
        self.last_tick_loaded = None
        self.tick_folder = tick_folder

    def get_max_value(self, date_from, date_to, side):
        _d0 = pd.to_datetime(date_from)
        _d1 = pd.to_datetime(date_to)
        try:
            return self.df_t[_d0:_d1][side].max()
        except KeyError as _key:
            print(f"Key Error, {str(_key)}, side {side}")
            return np.nan
        except Exception as _e:
            print(str(e))

    def get_min_value(self, date_from, date_to, side):
        _d0 = pd.to_datetime(date_from)
        _d1 = pd.to_datetime(date_to)
        try:
            return self.df_t[_d0:_d1][side].min()
        except KeyError as _key:
            print(f"Key Error, {str(_key)}, side {side}")
        except Exception as _e:
            print(str(e))

    def add_wicks(self, df, side):
        df["top_wick"] = df.apply(lambda row: self.get_max_value(date_from=row["date_time"],
                                                             date_to=row["end_time"], side=side), axis=1)
        df["bot_wick"] = df.apply(lambda row: self.get_min_value(date_from=row["date_time"],
                                                            date_to=row["end_time"], side=side), axis=1)
        return df

    def parse_file_name(self, file_name):
        """
        Parse a Dukascopy Renko File name into
        internal format

        :return:
        """

        _comps = str(file_name).split("_")
        _fn_info = dict()
        _fn_info["represent"] = "RENKO"
        _fn_info["instrument"] = _comps[0]
        _fn_info["bar_size"] = _comps[2]
        _fn_info["price_type"] = _comps[5]
        assert _fn_info["price_type"].lower() in ["bid", "ask"], \
            f"wrong price type {_fn_info['price_type']}, must be Bid or Ask"

        if _fn_info["bar_size"] == "ONE":
            _fn_info["bar_size"] = "1"
        elif _fn_info["bar_size"] == "TWO":
            _fn_info["bar_size"] = "2"
        elif _fn_info["bar_size"] == "FIVE":
            _fn_info["bar_size"] = "5"

        self.file_name_info = _fn_info

        return _fn_info

    def post_process(self, df):
        # open tick data set if it is not already open
        #df = self.add_wicks(df)

        if self.configuration["calc_wicks"]:
            if (self.last_tick_loaded is None) or (self.file_name_info["instrument"] != self.last_tick_loaded):
                print("Loading", self.file_name_info["instrument"])
                _file_name = f"{self.tick_folder}TICK_{self.file_name_info['instrument']}_0_BOTH_{self.configuration['time_zone']}.csv"
                self.df_t = pd.read_csv(_file_name)
                self.df_t["date_time"] = pd.to_datetime(self.df_t["date_time"])
                self.df_t.set_index("date_time", inplace=True)
                self.last_tick_loaded = self.file_name_info["instrument"]
                print("Tick data loaded Ok for", self.file_name_info["instrument"])

            df = self.add_wicks(df, self.file_name_info["price_type"].lower())

        return df


if __name__ == "__main__":
    try:
        _config_folder = sys.argv[1]
        _config_file = sys.argv[2]
        _tick_folder = sys.argv[3]
    except Exception as e:
        raise Exception(str(e))

    print("Processing for", _config_file)

    _rk = Renko(_config_folder, _config_file, _tick_folder, "")
    _rk.run()
