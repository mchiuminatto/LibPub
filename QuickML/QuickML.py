import numpy as np

# PROPRIETARY
import pandas as pd

from LibPub.DataSetManager import DataSet
from LibTqtk.ML.DataPreparation.General.DataSetSplit import DataSetSplit

# SKLEARN
from sklearn.metrics import mean_squared_error
from sklearn.metrics import accuracy_score, recall_score, precision_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import plot_confusion_matrix
from sklearn.preprocessing import StandardScaler

# PLOTTING
import matplotlib.pyplot as plt
import seaborn as sns; sns.set_theme()


class FullProcess:

    def process_regressor(self, model,
                           df: pd.DataFrame,
                           features: list,
                           label: str,
                           pred_col: str,
                           verbose: bool = False,
                           test_recs: float = 0.25,
                           split_mode: str = 'pctg',
                           metric_round_dig: int = 2,
                          scale = False,
                           **params):
        """
        Run a full cycle of split-train-predict for any classifier model that respects sklearn interface.


        :param model: Classifier class
        :param ds: Data set with data and metadata
        :param verbose: True for printing actions to console
        :param conf_matrix: True to plot confusion matrix
        :param train_recs: Training records proportion: 1 - test_rec if 0:
        :param test_recs: Testing records proportion: 1 - train_recs if 0:
        :param split_mode(not implemented): * If 'pctg': test_recs and train_recs are considered as percentage.
                            * If 'records': test_recs and train recs are considered as number of records, taking the
                            last test_recs for prediction and the last train_recs before the last test_recs for training
        :param metric_round_dig: number of digits to rounds metrics to
        :param params: ML model hyper-parameters

        :return:
        """

        # VALIDATE AND PROCESS PARAMETERS

        assert (test_recs > 0) and (test_recs < 1), 'test_recs need to be in the range )0,1('

        # PREPARE DATA
        _label_col = label
        _df_train, _df_pred = DataSetSplit.sep_predict_percentage(df, test_recs)
        _X_t = _df_train[features]

        if scale:
            _scaler = StandardScaler()
            _scaler.fit(_X_t)
            _X_t = _scaler.transform(_X_t)

        _y_t = _df_train[label]

        # TRAIN
        _clf = model(**params)
        _clf.fit(_X_t, _y_t)

        # calculate train metrics
        _train_pred_col = "y_hat_train"
        _df_train[_train_pred_col] = _clf.predict(_X_t)

        _rmse_train = mean_squared_error(_df_train[label], _df_train[_train_pred_col], squared=False).round(metric_round_dig)
        _smape_train = SMAPE.SMAPE(_df_train[label],_df_train[_train_pred_col])
        #_prec_train = precision_score(_df_train[label], _df_train[_train_pred_col]).round(metric_round_dig)
        #_rec_train = recall_score(_df_train[label], _df_train[_train_pred_col]).round(metric_round_dig)

        if verbose:
            print('')
            print('Train accuracy ', _rmse_train)

        # PREDICT
        _X_p = _df_pred[features]
        if scale:
            _scaler = StandardScaler()
            _scaler.fit(_X_p)
            _X_p = _scaler.transform(_X_p)

        _y_true = _df_pred[label]

        _df_pred[pred_col] = _clf.predict(_X_p)

        _rmse_pred = mean_squared_error(_df_pred[label], _df_pred[pred_col], squared=False).round(metric_round_dig)
        _smape_pred = SMAPE.SMAPE(_df_pred[label], _df_pred[pred_col])
        #_prec = precision_score(_df_pred[label], _df_pred[pred_col]).round(3)
        #_rec = recall_score(_df_pred[label], _df_pred[pred_col]).round(3)

        # if verbose:
        #     print('')
        #     print('Prediction accuracy ', _acc)
        #     print('Prediction precision ', _prec)
        #     print('Prediction recall ', _rec)

        # if conf_matrix:
        #     _cfm = confusion_matrix(np.array(_df_pred[label]), np.array(_df_pred[pred_col]))
        #     _cfm = (_cfm / len(_df_pred)).round(3)
        #     ax = sns.heatmap(_cfm, annot=True, fmt="0.2f")
        #     plt.show()

        # if conf_matrix:
        #
        #     plot_confusion_matrix(_clf, _X_p, _y_true, normalize='pred', cmap='plasma')
        #     plt.grid = False
        #     plt.show()
        _metrics = dict()
        _metrics['RMSE_pred'] = _rmse_pred
        _metrics['RMSE_train'] = _rmse_train
        _metrics['SMAPE_train'] = _smape_train
        _metrics['SMAPE_pred'] = _smape_pred

        return _df_pred, _metrics

    def process_classifier(self, model,
                           df: pd.DataFrame,
                           features: list,
                           label: str,
                           pred_col: str,
                           verbose: bool = False,
                           conf_matrix: bool = False,
                           test_recs: float = 0.25,
                           split_mode: str = 'pctg',
                           metric_round_dig: int = 2,
                           **params):
        """
        Run a full cycle of split-train-predict for any classifier model that respects sklearn interface.


        :param model: Regressor class
        :param ds: Data set with data and metadata
        :param verbose: True for printing actions to console
        :param conf_matrix: True to plot confusion matrix
        :param train_recs: Training records proportion: 1 - test_rec if 0:
        :param test_recs: Testing records proportion: 1 - train_recs if 0:
        :param split_mode(not implemented): * If 'pctg': test_recs and train_recs are considered as percentage.
                            * If 'records': test_recs and train recs are considered as number of records, taking the
                            last test_recs for prediction and the last train_recs before the last test_recs for training
        :param metric_round_dig: number of digits to rounds metrics to
        :param params: ML model hyper-parameters

        :return:
        """

        # VALIDATE AND PROCESS PARAMETERS

        assert (test_recs > 0) and (test_recs < 1), 'test_recs need to be in the range )0,1('

        # PREPARE DATA
        _label_col = label
        _df_train, _df_pred = DataSetSplit.sep_predict_percentage(df, test_recs)
        _X_t = _df_train[features]
        _y_t = _df_train[label]

        # TRAIN
        _clf = model(**params)
        _clf.fit(_X_t, _y_t)

        # calculate train metrics
        _train_pred_col = "y_hat_train"
        _df_train[_train_pred_col] = _clf.predict(_X_t)

        _acc_train = accuracy_score(_df_train[label], _df_train[_train_pred_col]).round(metric_round_dig)
        _prec_train = precision_score(_df_train[label], _df_train[_train_pred_col]).round(metric_round_dig)
        _rec_train = recall_score(_df_train[label], _df_train[_train_pred_col]).round(metric_round_dig)

        if verbose:
            print('')
            print('Train accuracy ', _acc_train)
            print('Train precision ', _prec_train)
            print('Train recall ', _rec_train)

        # PREDICT

        _X_p = _df_pred[features]
        _y_true = _df_pred[label]

        _df_pred[pred_col] = _clf.predict(_X_p)

        _acc = accuracy_score(_df_pred[label], _df_pred[pred_col]).round(3)
        _prec = precision_score(_df_pred[label], _df_pred[pred_col]).round(3)
        _rec = recall_score(_df_pred[label], _df_pred[pred_col]).round(3)

        # if verbose:
        #     print('')
        #     print('Prediction accuracy ', _acc)
        #     print('Prediction precision ', _prec)
        #     print('Prediction recall ', _rec)

        # if conf_matrix:
        #     _cfm = confusion_matrix(np.array(_df_pred[label]), np.array(_df_pred[pred_col]))
        #     _cfm = (_cfm / len(_df_pred)).round(3)
        #     ax = sns.heatmap(_cfm, annot=True, fmt="0.2f")
        #     plt.show()

        if conf_matrix:

            plot_confusion_matrix(_clf, _X_p, _y_true, normalize='pred', cmap='plasma')
            plt.grid = False
            plt.show()


        _metrics = dict()
        _metrics['acc_train'] = _acc_train
        _metrics['prec_train'] = _prec_train
        _metrics['rec_train'] = _rec_train
        _metrics['acc_pred'] = _acc
        _metrics['prec_pred'] = _prec
        _metrics['rec_pred'] = _rec

        return _df_pred, _metrics




class SMAPE:

    @staticmethod
    def SMAPE(y_true, y_pred):
        return np.mean((np.abs(y_pred - y_true) * 200 / (np.abs(y_pred) + np.abs(y_true))))

