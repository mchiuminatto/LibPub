from Test.TestBase import TestBase
from LibPub.DataSetManager import DataSet

class Test(TestBase):

    def test_case_1(self):
        """
        Test basic path: read existing file in parquet format
        """
        _ds = DataSet()
        _ds.open_dataset('./data/MICROFEAT@RENKO_EURJPY_10_Bid_UTC', format='parquet')
        _test_1 = _ds.metadata['mf_columns'][0] == 'tick_mean'
        _test_2 = len(_ds.data) == 2584

        return _test_1 and _test_2

    def test_case_2(self):
        """
        Test alternative path: read existing file in csv format

        """
        _ds = DataSet()
        _ds.open_dataset('./data/MICROFEAT@RENKO_EURJPY_10_Bid_UTC', format='csv')

        _test_1 = _ds.metadata['mf_columns'][0] == 'tick_mean'
        _test_2 = len(_ds.data) == 2584

        return _test_1 and _test_2


    def test_case_3(self):
        """
        Test alternative path: read non existing file

        """
        _ds = DataSet()
        try:
            _ds.open_dataset('./datax/MICROFEAT@RENKO_EURJPY_10_Bid_UTC', format='csv')
        except  FileNotFoundError as e:
            print(str(e))
            return True

        return False

    def test_case_4(self):
        """
        Test alternative path: read non-existent format for existing file

        """
        _ds = DataSet()
        try:
            _ds.open_dataset('./data/MICROFEAT@RENKO_EURJPY_10_Bid_UTC', format='xls')
        except  Exception as e:
            print(str(e))
            return True

        return False


    def test_case_5(self):
        """
        Test alternative path: read existing file in csv format and pass options to pandas:

        """
        _ds = DataSet()
        _ds.open_dataset('./data/MICROFEAT@RENKO_EURJPY_10_Bid_UTC', format='csv', index_col='date_time')

        _test_1 = _ds.metadata['mf_columns'][0] == 'tick_mean'
        _test_2 = len(_ds.data) == 2584

        return (_ds.data.index.name == 'date_time')


    def test_case_6(self):
        """
        Test alternative path: read and write parquet format file
        """
        _fn = 'MICROFEAT@RENKO_EURJPY_10_Bid_UTC'
        _ds = DataSet()
        _ds.open_dataset(f'./data/{_fn}', format='parquet')

        _ds.set_metadata('test_case', 6)
        _ds.data['test_case'] = 6
        _fn_new = _fn.replace('MICROFEAT', 'TEST')

        _ds.save_dataset(f'./data/{_fn_new}',format='parquet')

        _ds_new = DataSet()
        _ds_new.open_dataset(f'./data/{_fn_new}', format='parquet')
        _test_1 = _ds_new.metadata['test_case']==6
        _test_2 = _ds.data['test_case'].mean() == 6

        return _test_1 and _test_2

    def test_case_7(self):
        """
        Test alternative path: read and write csv format file
        """
        _fn = 'MICROFEAT@RENKO_EURJPY_10_Bid_UTC'
        _ds = DataSet()
        _ds.open_dataset(f'./data/{_fn}', format='csv')

        _ds.set_metadata('test_case', 6)
        _ds.data['test_case'] = 6
        _fn_new = _fn.replace('MICROFEAT', 'TEST')

        _ds.save_dataset(f'./data/{_fn_new}',format='csv')

        _ds_new = DataSet()
        _ds_new.open_dataset(f'./data/{_fn_new}', format='csv')
        _test_1 = _ds_new.metadata['test_case']==6
        _test_2 = _ds.data['test_case'].mean() == 6

        return _test_1 and _test_2

if __name__ == '__main__':
    _test = Test()
    _test.run([7])