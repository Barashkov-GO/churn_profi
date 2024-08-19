import numpy as np
from sklearn.metrics import mean_squared_error


def r2_score(y_true, y_pred):
    sse = np.sum((y_true - y_pred) ** 2)
    sst = np.sum((y_true - np.mean(y_true)) ** 2)
    r_squared = 1 - (sse / sst)
    return r_squared


def root_mean_squared_error(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))


def smape(y_true, y_pred):
    return 2 * np.mean(np.abs(y_pred - y_true) / (np.abs(y_pred) + np.abs(y_true)))


def wape(y_true, y_pred):
    return np.sum(np.abs(y_pred - y_true)) / np.sum(y_true)
