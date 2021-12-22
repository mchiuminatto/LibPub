import pandas as pd
class  CommonFormat:

    def __init__(self, input_folder, output_folder):
        self.input_folder = input_folder
        self.output_folder = output_folder


    def open_data_set(self, instrument, bar_size, price_type, repres=""):
        """

        Opens a data set in Dukascopy format

        :param repres:
        :param bar_size:
        :param price_type:
        :return:
        """
        # OPEN DATA SET
        _file_name = f"{self.input_folder}{instrument}_{bar_size}_{price_type}.csv"
        _df = pd.read_csv(_file_name)

        # TRANSFORMATIONS
        # search Dukascopy date time column
        # print(_df.columns)
        _time_col = [c for c in list(_df.cols) if "Time" in c]
        if len(_time_col) == 0:
            raise Exception(f"No date time column was found, please check Dukascopy file format")

        _df.rename(columns = {_time_col[0]:"date_time",
                              "Open":"open",
                              "High":"high",
                              "Low":"low",
                              "Close":"close",
                              "Volume": "volume",
                              "Volume ":"volume"}, inplace=True)

        _df["date_time"] = pd.to_datetime(_df["date_time"])
        _df.set_index(["date_time"], inplace=True)
       # print("Time column found ", _df.columns)

        #_df.to_csv(f"{self.output_folder}test_to_common.csv")
        return _df

    def save_data_set(self, df, instrument, bar_size, price_type, repres=""):
        df.reset_index(inplace=True)
        df.rename(columns={"date_time":"Time",
                            "open": "Open",
                            "high": "High",
                            "low": "Low",
                            "close": "Close",
                            "volume": "Volume "},
                  inplace=True)
        _file_name = f"{self.output_folder}{instrument}_{bar_size}_{price_type}_BLT.csv"
        try:
            df[["Time", "Open","High", "Low", "Close", "Volume "]].to_csv(_file_name)
        except KeyError as k:
           if "Volume" in str(k):
               df[["Time", "Open", "High", "Low", "Close"]].to_csv(_file_name)
        except Exception as e:
            raise Exception(f"Error saving data set {str(e)}")

