import logging

from LibTqtk.Connectors import PriceFileConverter
import sys
from dateutil import parser as Prs


class Tick(PriceFileConverter.PriceFileConverter):

    def __init__(self, config_folder, config_file, parse):
        super().__init__(config_folder, config_file, parse)

    def parse_file_name(self, file_name):
        """
        Parse a Dukascopy Renko File name into
        internal format

        :return:
        """
        _comps = file_name.replace(".csv","").split("_")
        logging.info(f"Parsed file name elements {_comps}")

        assert _comps[1].lower() == "ticks", "Apparently wrong file type"
        _fn_info = dict()
        _fn_info["represent"] = "TICK"
        _fn_info["instrument"] = _comps[0]
        _fn_info["bar_size"] = "0"
        _fn_info["price_type"] = "BOTH"
        _fn_info["date_from"] = Prs.parse(_comps[2])
        _fn_info["date_to"] = Prs.parse(_comps[3])

        self.file_name_info = _fn_info

        return _fn_info


if __name__ == "__main__":
    try:
        _config_folder = sys.argv[1]  # config folder
        _config_file = sys.argv[2]  # config file name
        try:
            _split =  bool(sys.argv[6])  # 1: split, 0 no split
        except IndexError:
            _split = False

        _chunksize = None
        if _split:
            try:
                _chunksize = int(sys.argv[7])
            except IndexError:
                _chunksize = 100000

    except Exception as e:
        raise Exception(str(e))

    print("Processing for", _config_file)

    _rk = Tick(_config_folder, _config_file,  "")
    _rk.run()
