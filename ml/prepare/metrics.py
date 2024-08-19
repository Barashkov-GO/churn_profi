import numpy as np


def skewness(data_big, column):
    data = data_big[column]
    n = len(data)
    mean = np.mean(data)
    variance = np.var(data)
    skewness = np.sum((data - mean) ** 3) / (n * variance ** (3 / 2))
    return skewness


def excess(data_big, column):
    data = data_big[column]
    n = len(data)
    mean = np.mean(data)
    variance = np.var(data)
    return np.sum((data - mean) ** 4) / (n * variance ** 2) - 3
