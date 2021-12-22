# V 1.000

from enum import Enum
import pandas as pd
import numpy as np
import sys

from LibTqtk.TradingLib.Currency.Currency import Currency
from LibTqtk.Utils.SBConfig import SBConfig
from LibTqtk.Utils.FileNames import FileNames
from LibTqtk.Connectors.Dukascopy.DukasCopyFileNameParser import DukasCopyFileNameParser

import timeit


class PriceType(Enum):
    BID = 1
    ASK = 2


class TickToNinjatrader:

    def __init__(self, config_folder, config_file):
        self.config = SBConfig.read_config_file(config_folder, config_file)

    def transform(self, instrument: str, df: pd.DataFrame, chunk_id: int =-1, precision: str = "second"):
        """
        Apply transformation from a Dukascopy data set to a Ninjatrader tick file.

        :param df: Input data set in raw dukascopy format (tick file)
        :param apply_to_price: which price to apply transformation to: Bid or Ask
        :param precision: second or sub-second
        :return:

        """

        # apply dates transformations
        self.transform_dates(df, precision=precision)


        _curr = Currency()
        _std_sym = _curr.get_clean_to_std(instrument)
        _digits = _curr.get_digits(_std_sym)
        _cols_base = ["date_time"]
        df["date_time"] = df["date"] + " " + df["time"]

        _cols = _cols_base + ["Bid", "BidVolume"]
        if chunk_id == -1:
            # processing full data set, no need to identify a chunk
            _file_name = f"{instrument}.Bid.txt"
        else:
            # processing a chunk tat needs to be identified in the file name
            _file_name = f"{instrument}.Bid_{chunk_id}.txt"

        df["BidVolume"] =(df["BidVolume"]*100).astype(int)
        df["Bid"] = np.round(df["Bid"], _digits+1)
        print("Saving file " + _file_name)
        df[_cols].to_csv(f"{self.config['FOLDERS']['output_folder']}{_file_name}", header=False, index=False, sep=";")

        _cols = _cols_base + ["Ask", "AskVolume"]
        if chunk_id == -1:
            # processing full data set, no need to identify a chunk
            _file_name = f"{instrument}.Ask.txt"
        else:
            # processing a chunk tat needs to be identified in the file name
            _file_name = f"{instrument}.Ask_{chunk_id}.txt"

        df["AskVolume"] = (df["AskVolume"]*100).astype(int)
        df["Ask"] = np.round(df["Ask"], _digits + 1)
        print("Saving file " + _file_name)
        df[_cols].to_csv(f"{self.config['FOLDERS']['output_folder']}{_file_name}", header=False, index=False, sep=";")

    def produce(self):
        _files_to_process = FileNames.collect_file_names(self.config["FOLDERS"]["input_folder"], ".csv")

        for _f in _files_to_process:
            print("Processing file " + _f)

            _instrument = _f[:6]

            if self.config["SIZE_HANDLING"]["split"]:
                _df_chunks = pd.read_csv(f"{self.config['FOLDERS']['input_folder']}{_f}", chunksize=self.config["SIZE_HANDLING"]["chunk_size"])
                _T1 = timeit.default_timer()
                for _i,_df in enumerate(_df_chunks):
                    print("Processing chunk " + str(_i) + " of the file")
                    _t1 = timeit.default_timer()
                    self.transform(_instrument, _df, chunk_id= _i, precision="sub-second")
                    _t2 = timeit.default_timer()
                    print("Chunk processing time " + str(np.round(_t2 - _t1,2)) + " seconds")
                _T2 = timeit.default_timer()
                print("Total dataset processing time is " + str(np.round(_T2 - _T1,2)) + " seconds")

            else:
                _df = pd.read_csv(f"{self.config['FOLDERS']['input_folder']}{_f}")
                self.transform(_instrument, _df, precision="sub-second")



    # *************************************
    # HELPER FUNCTIONS
    # *************************************

    def transform_dates(self, df, precision):
        print("transforming dates")
        #_time_col = [c for c in list(df.columns) if "Time" in c][0]
        _time_col = "Time (AZT)"
        df[_time_col] = pd.to_datetime(df[_time_col])

        df["date"] = df[_time_col].dt.strftime('%Y%m%d')
        if precision == "second":
            df["time"] = df[_time_col].dt.strftime("%H%M%S")
        else:
            df["millisecs"] = df[_time_col].dt.strftime("%f") + "0"
            df["time"] = df[_time_col].dt.strftime("%H%M%S %f") + "0"


if __name__ == "__main__":
    _config_folder = sys.argv[1]
    _config_file = sys.argv[2]

    _tktn = TickToNinjatrader(_config_folder, _config_file)
    _tktn.produce()