import numpy as np
import pandas as pd
from datetime import datetime
from dateutil import parser

class RandomWalk:

    @staticmethod
    def gen_random_walk(size, std, last_date=None, value_0=0, freq="H", output_col_name="X"):

        if last_date == None:
            _last_date = datetime.today()
        else:
            _last_date = parser.parse(last_date)

        _p_t = np.zeros(size)
        _wn = np.random.normal(loc=0, scale=std, size=size)
        _p_t[0] = value_0
        _wn[0] = 0

        _date_index = pd.date_range(end=_last_date, periods=size, freq=freq)

        _df_rw = pd.DataFrame(np.stack([_p_t, _wn], axis=1), columns=[output_col_name, "wn"], index=_date_index)

        for i in range(1, size):
            _df_rw.iloc[i][output_col_name] = _df_rw.iloc[i - 1][output_col_name] + _df_rw.iloc[i].wn

        return _df_rw
