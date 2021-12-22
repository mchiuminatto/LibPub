"""
As part of the Dukascopy connector, this module
provides the functionality to parse historic file names
,extract information from them and massively rename to
transfor file name to a common framework format

Author: Marcello Chiuminatto
Company: The Quantick
"""

# TODO: Add one minute parsing
# TODO: Extend  to other price representations (tick, Renko...)

import os
from shutil import copy2

from LibTqtk.TradingLib.Currency import Currency as Cr
class DukasCopyFileNameParser:

    def __init__(self, input_folder="", output_folder=""):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.parsers = dict()
        self.parsers["candle"] = self.parse_name
        self.parsers["renko"] = self.parse_renko

    def collect_data_sets(self, pattern):
        """
        Collects the input_currency data set file names

        :param pattern: Search pattern
        :return  _input_files: Input file list
        """

        _dir_list = next(os.walk(self.input_folder))[2]
        _input_files = [s for s in _dir_list if pattern in s]
        self.data_sets = _input_files
        return _input_files

    def names_to_common(self, input_files):
        """
            Convert file names from dukascopy format
            to internal format and moves the files to a new
            destination


        :param input_files:
        :return:
        """

        for f in input_files:
            _p_name = self.parse_name(f)
            _src_fn = f"{self.input_folder}{f}"
            _dest_fn = f"{self.output_folder}{_p_name['curr_clean']}_{_p_name['time_frame']}_{_p_name['price_type']}.csv"
            copy2(_src_fn, _dest_fn)

        print("all files moved")

    @staticmethod
    def parse_name( file_name):
        """
        Maps Dukascopy file name elements to
        individual information components:

        * currency: Instrument in slashed format
        * curr_clean: in clean format
        * time_frame: bar  size
        * price_type: bid or ask

        :param file_name:
        :return: _name_info, information elements found on name
        """

        # PARSE CURRENCY
        _name_info = dict()
        _fn_comps = file_name.split("_")
        _curr_clean = _fn_comps[0]
        _tf_raw = _fn_comps[1]
        _price_type  = _fn_comps[2].lower()

        _cr = Cr.Currency()
        try:
            _name_info["currency"] = _cr.get_clean_to_std(_curr_clean)
            _name_info["curr_clean"] = _curr_clean
        except KeyError as e:
            raise Exception(f"Currency code {e}  was not found")
        _tf_comps = _tf_raw.split(" ")

        if len(_tf_comps) == 1:
            if _tf_comps[0].lower() == "monthly":
                _time_frame = "M"
            elif  _tf_comps[0].lower() == "weekly":
                _time_frame = "W"
            elif _tf_comps[0].lower() == "daily":
                _time_frame = "D"
            elif _tf_comps[0].lower() == "hourly":
                _time_frame = "H1"
            else:
                raise Exception(f"Time Frame {_tf_comps[0]} is not valid")
        elif len(_tf_comps) == 2:
            if _tf_comps[1].lower() == "hours":
                _time_frame = f"H{_tf_comps[0]}"
            elif _tf_comps[1].lower() == "mins":
                _time_frame = f"m{_tf_comps[0]}"
            elif _tf_comps[1].lower() == "min":
                _time_frame = "m1"
            else:
                raise Exception(f"Time frame {_tf_raw} not valid")

        _name_info["time_frame"] = _time_frame
        _name_info ["price_type"]= _price_type

        return _name_info

    def parse_renko(self, file_name):
        pass

    def parse(self, file_name, representation):
        print(file_name, representation)
        pass

