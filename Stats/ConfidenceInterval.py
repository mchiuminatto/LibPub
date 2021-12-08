import numpy as np
from scipy.stats import norm
from scipy.stats import t

from LibPub.Stats.Stats import Stats


class ConfidenceInterval(Stats):

    def __init__(self, precision=5):
        super().__init__(precision)

    @staticmethod
    def reliability_factor_norm(alpha):
        print('Norm')
        _val_search = 1 - alpha / 2
        return norm.ppf(_val_search).round(3)

    @staticmethod
    def reliability_factor_t(n, alpha):
        print('t-student')
        _val_search = 1 - alpha / 2
        _dof = n - 1
        return t.ppf(_val_search, df=_dof).round(3)

    def confidence_interval(self, data, confidence_level=None, alpha=None, pop_var=None):
        """
        Calculates the confidence interval for the the mean parameter.
        :param data: The subject data
        :param confidence_level: 1 - alpha
        :param alpha: probability that estimated population parameter is outside the confidence interval
        :param pop_var: population variance in case it is known
        :return:
        """

        # TODO: Generalize ofr any population parameter

        _cl = 0  # confidence level
        _alpha= 0  # significance level

        if alpha is not None:
            _alpha = alpha
            _cl = 1 - _alpha
        elif confidence_level is not None:
            _cl = confidence_level
            _alpha = 1 - _cl
        else:
            raise Exception('at least and at most one of alpha or confidence level needs to be specified')

        _N = len(data)
        _sample_mean = data.mean().round(self.precision)  # sample mean
        _sample_std = data.std(ddof=1).round(self.precision)  # sample standard deviation

        _RF = 0
        _SE = 0
        if pop_var is not None:
            # population variance is known
            _RF = self.reliability_factor_norm(_alpha)  # reliability factor
            _SE = np.sqrt(pop_var / _N)  # standard error
        else:
            # population variance is not known
            _RF = self.reliability_factor_t(_N, _alpha)
            _SE = _sample_std/np.sqrt(_N)  # standard error

        _left_lim = np.round(_sample_mean - _RF * _SE, self.precision)
        _right_lim = np.round(_sample_mean + _RF * _SE, self.precision)

        self.results = {'left_limit': _left_lim,
                        'right_limit': _right_lim,
                        'standard_error': np.round(_SE,self.precision),
                        'reliability_factor': _RF,
                        'sample_mean': _sample_mean,
                        'sample_std':_sample_std}