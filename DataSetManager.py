"""
Library to package data and metadata required for
processing in a pipeline.

# version 2021.12.23


"""

import json
import pandas as pd
from LibPub.Encoders.JSONDefaultHandler import JSONDefaultHandler

class DataSet:
    def __init__(self):
        self.__metadata = dict()
        self.__data = None

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


    def open_dataset(self, file_name):
        with open(file_name) as fp:
            _envelope = json.load(fp)

        # self.__data = pd.read_json(_envelope["data"], orient="split")
        self.__metadata = _envelope["metadata"]

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

    def save_dataset(self, file_name):
        _envelope = dict()
        # _envelope["data"] = self.__data.to_json(orient="split")
        _envelope["metadata"] = self.__metadata
        with open(f"{file_name}", "w") as fp:
            json.dump(_envelope, fp, default=JSONDefaultHandler.myconverter)

