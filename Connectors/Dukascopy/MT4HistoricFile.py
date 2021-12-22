"""
Dukascopy to MT4 Data converter module

Author: Marcello Chiuminatto
Version 0.9

"""

import os
import pandas as pd
from LibTqtk.TradingLib.Currency import Currency as Cr
from LibTqtk.Utils import  FileNames

class MT4HistoricFile:

    def __init__(self, input_folder, output_folder):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.fn_parser = FileNames

    def collect_datasets(self, pattern):
        """
        Collects the input_currency data set file names
        :param pattern: Search pattern
        :return  _input_files: Input file list
        """
        _dir_list = next(os.walk(self.input_folder))[2]
        _input_files = [s for s in _dir_list if pattern in s]
        self.data_sets = _input_files
        return _input_files

    def convert(self, file_name):
        """
        Converts the data format from Dukascopy to
        MT4
        :param file_name:
        :return:  True if conversion was Ok, False otherwise
        """
        print(f"processing {file_name}")
        try:
            _df = pd.read_csv(f"{self.input_folder}{file_name}")
        except FileNotFoundError:
            return False

        # DATE AND TIME COLUMN CONVERSION
        _time_col = [c for c in _df.cols if "Time" in c][0]
        _df["DateTime"] = pd.to_datetime(_df[_time_col], yearfirst=True)
        _df["Date"] = _df["DateTime"].dt.date
        _df["Time"] = _df["DateTime"].dt.time
        _df.drop(columns=["DateTime"], inplace=True)

        # GET DATA SET INFORMATION FROM TIME FRAME
        _name_info = None
        try:
            _name_info = self.fn_parser.parse_internal_file_name(file_name)
        except Exception as e:
            raise Exception(str(e))

        # ROUND TO CURRENCY DIGITS
        _currency = _name_info["instrument"]
        _cr = Cr.Currency()
        _cr_std = _cr.get_clean_to_std(_currency)
        _digits = _cr.get_digits(_cr_std)

        _df["Open"] = _df["Open"].round(decimals=_digits+1)
        _df["High"] = _df["High"].round(decimals=_digits+1)
        _df["Low"] = _df["Low"].round(decimals=_digits+1)
        _df["Close"] = _df["Close"].round(decimals=_digits+1)

        #  SAVE TRANSFORMED DATA SET
        print(_name_info)
        print(_name_info["price_type"])

        _out_file_name =f"{_name_info['instrument']}_{_name_info['bar_size']}_{_name_info['price_type']}.csv"

        print("Saving output_feature file", _out_file_name, "Columns ", _df.cols)
        _df[["Date", "Time","Open", "High", "Low", "Close"]].to_csv(f"{self.output_folder}{_out_file_name}", header=False, index=False)

        return True

    def process_folder(self, pattern=""):
        _file_list = self.collect_datasets(pattern)
        for f in _file_list:
            self.convert(f)
