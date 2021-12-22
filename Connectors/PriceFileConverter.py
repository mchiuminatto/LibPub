import dask

from LibTqtk.Utils import SBConfig, FileNames
import pandas as pd
import logging
from datetime import datetime as Dt
import dask.dataframe as dd

# version 2020.09.24  - added time zone validation
# version 2021.12.16 - files now are handled with Dask
#                    - output format now is parquet

class PriceFileConverter:

    def __init__(self, config_folder, config_file, parse):
        """
        Initializes the class

        :param config_folder: Configuration file location
        :param config_file: Configuration file name
        :param input_folder:  Folder from which take data
        :param output_folder:  Folder to which save transformed data
        :param parse: Which price abstraction to parse: candle, renko
        """

        self.config_folder = config_folder
        self.config_file = config_file

        self.parsing = parse
        self.file_name_info = None

        # TODO add folder validations

        # open configuration file
        self.__config = SBConfig.SBConfig()

        try:
            self.configuration = self.__config.read_config_file(self.config_folder, self.config_file)
        except FileNotFoundError as e:
            raise Exception(str(e))

        self.input_folder = self.configuration["FOLDERS"]["input"]
        self.output_folder = self.configuration["FOLDERS"]["output"]
        self.log_folder = self.configuration["FOLDERS"]["log"]

        _date = Dt.today().strftime("%Y%m%d_%H%M")
        _log_file_name = self.log_folder + _date + "_" + "DC_TO_INT.log"
        logging.basicConfig(filename=_log_file_name, format="%(asctime)s %(levelname)s:%(message)s",
                            level=logging.INFO)

    def open(self, file_name: str, blocksize=None):
        if blocksize is not None:
            return dd.read_csv(f"{self.input_folder}{file_name}", blocksize=blocksize)
        else:
            return dd.read_csv(f"{self.input_folder}{file_name}")


    def parse_file_name(self, file_name):
        pass

    def transform(self, file_name, df):
        _name_info = self.parse_file_name(file_name)
        #print(_name_info)

        # CREATE NEW COLUMNS
        df["instrument"] = _name_info["instrument"]
        df["bar_size"] = _name_info["bar_size"]
        df["price_type"] = _name_info["price_type"]

        # NORMALIZE DATE/TIME COLUMN
        _time_col = [c for c in list(df.columns) if "Time" in c]
        if len(_time_col) == 0:
            raise Exception(f"No date time column was found, please check Dukascopy file format")

        # validate time zone
        _tz_string = f"({self.configuration['time_zone']})"
        if _tz_string not in _time_col[0]:
            raise Exception(f"Time zone {_tz_string } does not match time zone in " + _time_col[0] )

        df = df.rename(columns={_time_col[0]: "date_time"}).compute()
        df["date_time"] = pd.to_datetime(df["date_time"])

        # RENAME REMAINING COLUMNS
        df.rename(columns=self.configuration["mapper"], inplace=True)

        return df

    def save(self, df: pd.DataFrame, chunk: int = None):

        # GET NAME COMPONENTS
        _tz_string = self.configuration["time_zone"]
        _elements = list(df.iloc[0][["instrument", "bar_size", "price_type"]])

        _file_name = f"{self.file_name_info['represent']}_{_elements[0]}_{_elements[1]}_{_elements[2]}_{_tz_string}"
        _file_name += ".parquet"

        logging.info(f"Saving file : {_file_name}")
        _file_dest = f"{self.output_folder}{_file_name}"
        df.to_parquet(_file_dest, index=False)

    def integrity_checks(self, df):
        # missing data check

        _mask = df.isna()
        _dly_N0 = len(df)
        _dly_N1 = len(df[_mask])

        _N0, _N1 = dask.compute(_dly_N0, _dly_N1)
        _test_1 = _N0 == _N1

        logging.info(f'Integrity check performed,  {_N0}, {_N1}, {_test_1}')

        return _test_1

    def post_process(self, df):
        """
        Processing after transformation has been applied
        :param df:
        :return:
        """
        return df

    def run(self):
        # get input_currency file list
        logging.info(f"Processing files in folder {self.input_folder}")
        _file_list = FileNames.FileNames.collect_file_names(self.input_folder, "csv")

        # process each file
        for f in _file_list:
            logging.info(f"Processing file {f}")
            df = self.run_single(f,save=True, split=self.configuration["split"], blocksize=self.configuration["chunk_size"])

    def run_single(self, file_name, save=True, split=False, blocksize=None ):
        """
        Run the process for a single file and optionally saves it.

        :param file_name: file to be processed
        :param save: True if saves the file, False otherwise

        :return:
            df: transformed data set
        """

        df = self.open(file_name)
        assert self.integrity_checks(df), f"Integrity problem found on file {file_name}"
        df = self.transform(file_name, df)
        df = self.post_process(df)
        if save:
            self.save(df)
        return df




if __name__ == "__main__":
    pass




