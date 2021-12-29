"""
Library to package data and metadata required for
processing in a pipeline.


# version 2121.12.29
# - added save metadata and data set in a single operation

"""

import json
import pandas as pd
from LibPub.Encoders.JSONDefaultHandler import JSONDefaultHandler

class DataSet:

    # READ_FORMAT_MAP = {'csv':pd.read_csv,
    #               'parquet': pd.read_parquet}
    #
    # WRITE_FORMAT_MAP = {'csv':pd.DataFrame.to_csv,
    #               'parquet': pd.DataFrame.to_parquet}

    def __init__(self):
        self.__metadata = dict()
        self.__data = None

        self.READ_FORMAT_MAP = {'csv': pd.read_csv,
                           'parquet': pd.read_parquet}

    # region Properties
    @property
    def metadata(self, key=None):
        if key is None:
            return self.__metadata
        else:
            return self.__metadata[key]

    @property
    def data(self):
        return self.__data

    # endregion


    def open_dataset(self, file_name, format='csv', **params):

        if format not in self.READ_FORMAT_MAP.keys():
            raise Exception(f'Invalid format. Allowed formats: {", ".join(list(self.READ_FORMAT_MAP.keys()))}')

        try:
            with open(file_name + '.json') as fp:
                _envelope = json.load(fp)
                self.__metadata = _envelope['metadata']
        except FileNotFoundError:
            raise FileNotFoundError(f'Meta data file {file_name} not found')

        try:
            self.__data = self.READ_FORMAT_MAP[format](f'{file_name}.{format}', **params)
        except FileNotFoundError:
            raise FileNotFoundError(f'Data set {file_name}.{format} not found')



    def set_metadata(self, key, value):
        """
        Add or updates values for dataset metadata

        :param key: key to add under metadata key
        :param value: value to associate the key with. Can be any valid
                python type: int, float, str, list, dict, etc.
        :return:
        """
        self.__metadata[key] = value

    def copy_metadata(self, metadata):
        self.__metadata = metadata

    # def set_data(self, df):
    #     self.__data = df

    def save_dataset(self, file_name, format='csv', **params):

        if format not in self.READ_FORMAT_MAP.keys():
            raise Exception(f'Invalid format. Allowed formats: {", ".join(list(self.READ_FORMAT_MAP.keys()))}')

        try:
            with open(file_name + '.json', "w") as fp:
                _to_save_metadata = dict()
                _to_save_metadata['metadata'] = self.__metadata
                json.dump(_to_save_metadata, fp, default=JSONDefaultHandler.myconverter)
        except Exception as e:
            raise Exception(f'Was not possible to write metadata file{file_name}, {str(e)} ')

        try:
            if format == 'csv':
                self.__data.to_csv(f'{file_name}.{format}', **params)
            elif format == 'parquet':
                self.__data.to_parquet(f'{file_name}.{format}', **params)
        except Exception as e:
            raise Exception(f'Was not possible to write dataset file{file_name}, {str(e)} ')
