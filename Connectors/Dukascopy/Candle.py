from Connectors import PriceFileConverter
import sys
from Currency import Currency as Cr


class Candle(PriceFileConverter.PriceFileConverter):

    def __init__(self, config_folder, config_file,  parse):
        super().__init__(config_folder, config_file,  parse)

    def parse_file_name(self, file_name):
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
        _price_type = _fn_comps[2]

        _cr = Cr.Currency()
        _name_info["represent"] = "CANDLE"
        try:
            _name_info["instrument"] = _fn_comps[0]
            #_name_info["curr_clean"] = _curr_clean
        except KeyError as e:
            raise Exception(f"Currency code {e}  was not found")
        _tf_comps = _tf_raw.split(" ")

        _time_frame = None
        if len(_tf_comps) == 1:
            if _tf_comps[0].lower() == "monthly":
                _time_frame = "M"
            elif _tf_comps[0].lower() == "weekly":
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

        if _time_frame is None:
            raise Exception(f"Time frame not mapped for file {file_name}")

        _name_info["bar_size"] = _time_frame
        _name_info["price_type"] = _price_type

        self.file_name_info = _name_info

        return _name_info

    def post_process(self, df):
        df["end_time"] = df["date_time"].shift(-1)
        df.dropna(inplace=True)

        return df


if __name__ == "__main__":
    try:
        _config_folder = sys.argv[1]
        _config_file = sys.argv[2]

        print(">>>>>>>", _config_folder, _config_file)


    except Exception as e:
        raise Exception(str(e))

    print("Processing for", _config_file)

    _cdl = Candle(_config_folder, _config_file, "")
    _cdl.run()

