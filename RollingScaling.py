class RollingScaling:

    def __init__(self):
        pass

    @staticmethod
    def min_max_scaler(x, window):
        _r = x.rolling(window=window)
        _min = _r.min()
        _max = _r.max()
        _mms = (x - _min) / (_max - _min)
        return _mms

    @staticmethod
    def z_score_scaler(x, window):
        _r = x.rolling(window=window)
        _m = _r.mean()
        _s = _r.std(ddof=1)
        _z = (x - _m) / _s
        return _z

