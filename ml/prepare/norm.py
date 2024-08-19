import numpy as np
import pandas as pd
import numpy as np
from joblib import Parallel, delayed
from tqdm.notebook import tqdm
import multiprocessing
from ml.parallel import apply_par_col


def z_column(data, col):
    mean, std = data[col].mean(), data[col].std()
    return (data[col] - mean) / std


def robust_column(data, col):
    q75, q25 = np.percentile(data[col], [75, 25])
    iqr = q75 - q25
    if iqr != 0:
        return (data[col] - data[col].median()) / iqr
    else:
        return data[col]


def log_column(data, col):
    old_data = data[data[col] <= 0][col]
    return np.log(data[col])


def min_max_column(data, col):
    min_val, max_val = data[col].min(), data[col].max()
    return (
        (data[col] - min_val) / (max_val - min_val)
        if max_val - min_val != 0
        else data[col]
    )


def min_max(data, columns):
    return apply_par_col(data, columns, min_max_column)


def z_norm(data, columns):
    return apply_par_col(data, columns, z_column)


def robust(data, columns):
    return apply_par_col(data, columns, robust_column)


def log(data, columns):
    return apply_par_col(data, columns, log_column)
