from LibTqtk.Connectors import PriceFileConverter
import sys


class TickBar(PriceFileConverter.PriceFileConverter):

    def __init__(self, config_folder, config_file, parse):
        super().__init__(config_folder, config_file, parse)

    def parse_file_name(self, file_name):
        """
        Parse a Dukascopy Range File name into
        internal format

        :return:
        """

        _comps = str(file_name).split("_")
        _fn_info = dict()
        _fn_info["represent"] = "TICKBAR"
        _fn_info["instrument"] = _comps[0]
        _fn_info["bar_size"] = _comps[2]
        _fn_info["price_type"] = _comps[3]
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

if __name__ == "__main__":
    try:
        _config_folder = sys.argv[1]
        _config_file = sys.argv[2]


    except Exception as e:
        raise Exception(str(e))

    print("Processing for", _config_file)

    _rk = TickBar(_config_folder, _config_file,  "")
    _rk.run()
