from scipy.stats import t
from scipy.stats import norm
import numpy as np


class Stats:

    def __init__(self, precision=3):
        self.precision = precision
        self.results = None

    def score(self, mean, mu_0, std, n):
        return np.round((mean - mu_0)/self.standard_error(std, n), self.precision)

    def standard_error(self, std, n):
        return np.round(std/np.sqrt(n), self.precision)

    @staticmethod
    def p_value(critical_value, distribution, test_type, **params):

        """
        Calculates p-value using the critical value for the following distributions:
        * normal
        * t-student

        :param critical_value: The value used to look for the probability distribution function value
        :param distribution: Distribution name
        :param test_type: 1: one sided test, 2: two sided test
        :param additional parameters for each particular distribution:
               * dof: degrees of freedom for t-student

        :return:
        p-value

        """

        assert test_type in [1, 2], 'test type can be 1 (one-sided) or two (two-sided)'
        assert distribution in ['normal', 't-student'], f'Supported distributions are: normal, ' \
                                                        f't-student. Provided value: {distribution}'

        _dst_class = None
        _cdf = 0
        if distribution == 'normal':
            _dst_class = norm
            _cdf = _dst_class.cdf(critical_value)
        elif distribution == 't-student':
            _dst_class = t
            _cdf = _dst_class.cdf(critical_value, df=params['dof'] )

        _p_value = (1 - _cdf) * test_type

        return _p_value
