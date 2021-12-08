import numpy as np
from scipy.stats import t
from scipy.stats import norm

from LibPub.Stats.Stats import Stats


class HypothesisTesting(Stats):

    def __init__(self, precision=3):
        super().__init__(precision)

    def z_test(self, sig_level, test_type=2):

        if test_type == 2:  # two tailed test
            _alpha = sig_level / 2
        elif test_type == 1:  # one tailed test
            _alpha = sig_level
        else:
            raise Exception(f'Invalid test type, must be 1 or 2 but {test_type} was provided')

        return np.round(norm.ppf(1 - _alpha), self.precision)

    def t_test(self, dof, sig_level, test_type=2):

        _val_search = sig_level / test_type

        return t.ppf(_val_search, df=dof).round(self.precision)

    def score_test(self, data, mu_0=None, std=None, sig_level=0.05, test_type=2):

        _n = len(data)  # sample size
        _x_mean = data.mean()  # sample mean
        _s = data.std(ddof=1)  # sample standard deviation
        _SE = None
        _cv_type = ''
        if std is not None:
            _SE = self.standard_error(std,_n)
            # _SE = std / np.sqrt(_n)  # population standard error when variance is known
        else:
            _SE = self.standard_error(_s, _n)
            # _SE = _s / np.sqrt(_n)

        print('x mean', _x_mean)

        _Z = (_x_mean - mu_0) / _SE  # Standardize the variable to compare it with  standardized score

        if std is not None:
            _cv = self.z_test(sig_level, test_type=2)  # critical value
            _cv_type = 'Z-score'
        else:
            _cv = self.t_test(_n - 1, sig_level, test_type=test_type)
            _cv_type = 'T-score'

        _reject = np.abs(_Z) > np.abs(_cv)
        _results = dict()
        _results = {'reject_H_0': _reject,
                    'standard_error': _SE,
                   _cv_type: _Z,
                    'critical_value': _cv,
                    'sample_mean': _x_mean,
                    'sample_size': _n,
                    'sample_std': _s}
        return _results

    def p_value_test(self, x_mean, mu_0, std, n, distribution, test_type, sig_level):
        _score = self.score(x_mean, mu_0, std, n)
        _p_value = self.p_value(np.abs(_score), distribution, test_type,dof=n-1)

        _results = dict()
        _results['score'] = _score
        _results['p-value'] = _p_value
        _results['reject_H_0'] = _p_value < sig_level
        return _results



