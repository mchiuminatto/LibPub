import pandas as pd
from LibTqtk.Connectors import PriceFileConverter
from LibTqtk.Connectors.Dukascopy import DukasCopyFileNameParser as Dfnp


class  DukascopyToInternal(PriceFileConverter.PriceFileConverter):

    def __init__(self, config_folder, config_file, input_folder, output_folder, parse):
        super().__init__(config_folder, config_file, input_folder,output_folder, parse)

    def transform(self, file_name, df):

        _name_parser = Dfnp.DukasCopyFileNameParser()
        _name_info = _name_parser.parse_name(file_name)
        print(_name_info)

        # CREATE NEW COLUMNS
        df["instrument"] = _name_info["curr_clean"]
        df["bar_size"] = _name_info["time_frame"]
        df["price_type"] = _name_info["price_type"]

        # NORMALIZE DATE/TIME COLUMN
        _time_col = [c for c in list(df.cols) if "Time" in c]
        if len(_time_col) == 0:
            raise Exception(f"No date time column was found, please check Dukascopy file format")
        df.rename(columns={_time_col[0]: "date_time"}, inplace=True)
        df["date_time"] = pd.to_datetime(df["date_time"])

        # RENAME REMAINING COLUMNS
        df.rename(columns = self.configuration["mapper"], inplace=True)

        return df

    def candle_trf_full(self, file_name):
        """
        Transform a dukascopy's candle price data set into
        an internal format price data set.

        Open the data set in the input_currency folder
        Transform it
         Save it in the output_feature folder

        :param file_name: data set's name in the  input_currency folder
        :return:
        """



