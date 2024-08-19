import numpy as np
import math
import pandas as pd
from sklearn.neighbors import RadiusNeighborsClassifier, KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_log_error

from sklearn.neighbors import KNeighborsRegressor
from sklearn.neighbors import RadiusNeighborsRegressor
from sklearn.neighbors import BallTree
from sklearn.neighbors import KDTree

from ml.plot.plot_points import plot_datas, plot_data, plot_datas_many_dim
from ml.plot.plot_metrics import plot_regression


def test_classification_knn(X, y, min_radius=1.5, max_radius=10.0, iter=0.5):
    def polynomial_kernel(distances, gamma=1, r=1, d=2):
        # Полиномиальное ядро
        return (gamma * distances + r) ** d

    def rbf_kernel(distances, gamma=1):
        # Гауссовское (ра|диально-базисная функция) ядро
        return np.exp(-gamma * distances**2)

    def sigmoid_kernel(distances, gamma=1, r=0):
        # Сигмоидное ядро
        return np.tanh(gamma * distances + r)

    def rectangular_kernel(distances):
        return (distances <= 1).astype(
            float
        )  # Возвращает 1, если расстояние <= 1, иначе 0

    def triangular_kernel(distances):
        return (1 - distances) * (distances <= 1).astype(
            float
        )  # Возвращает (1 - расстояние) если расстояние <= 1, иначе 0

    def epanechnikov_kernel(distances):
        return (
            (3 / 4) * (1 - distances**2) * (distances <= 1).astype(float)
        )  # Возвращает (3/4) * (1 - расстояние^2) если расстояние <= 1, иначе 0

    def bisquare_kernel(distances):
        return (
            (15 / 16) * (1 - distances**2) ** 2 * (distances <= 1).astype(float)
        )  # Возвращает (15/16) * (1 - расстояние^2)^2 если расстояние <= 1, иначе 0

    def gaussian_kernel(distances):
        # Параметр ширины гауссовского ядра (можно настроить)
        sigma = 0.7
        return np.exp(-0.5 * (distances / sigma) ** 2) / np.sqrt(2 * math.pi)

    kernels = [
        polynomial_kernel,
        rbf_kernel,
        sigmoid_kernel,
        rectangular_kernel,
        triangular_kernel,
        epanechnikov_kernel,
        bisquare_kernel,
        gaussian_kernel,
    ]

    ans = dict()
    metrics = [accuracy_score, precision_score, recall_score, f1_score, roc_auc_score]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=143
    )

    knn = KNeighborsClassifier(n_jobs=-1, n_neighbors=5)
    knn.fit(X_train, y_train)
    y_pred = knn.predict(X_test)
    #
    for_plot = []

    for_plot.append([X_test, y_pred, "knn 5"])
    # plot_data(X_test, y_pred)
    ans["knn"] = dict()
    for metric in metrics:
        ans["knn"][metric.__name__] = metric(y_test, y_pred)
    del knn

    def distance_weight(distances):
        return 1 / (1 + distances)

    knn = KNeighborsClassifier(n_jobs=-1, n_neighbors=5, weights=distance_weight)
    knn.fit(X_train, y_train)
    y_pred = knn.predict(X_test)

    for_plot.append([X_test, y_pred, "Weighted knn"])
    # plot_data(X_test, y_pred)
    ans["weighted_knn"] = dict()
    for metric in metrics:
        ans["weighted_knn"][metric.__name__] = metric(y_test, y_pred)
    del knn

    for kernel in kernels:
        knn = KNeighborsClassifier(n_jobs=-1, n_neighbors=5, weights=kernel)
        knn.fit(X_train, y_train)
        y_pred = knn.predict(X_test)

        for_plot.append([X_test, y_pred, f"{kernel.__name__} knn"])
        # plot_data(X_test, y_pred)
        ans[f"{kernel.__name__}_knn"] = dict()
        for metric in metrics:
            ans[f"{kernel.__name__}_knn"][metric.__name__] = metric(y_test, y_pred)
        del knn

    for radius in np.arange(min_radius, max_radius + iter, iter):
        knn = RadiusNeighborsClassifier(radius, outlier_label=0)
        knn.fit(X_train, y_train)
        y_pred = knn.predict(X_test)
        # plot_data(X_test, y_pred)

        for_plot.append([X_test, y_pred, f"{radius} knn"])
        ans[f"radius_{radius}_knn"] = dict()
        for metric in metrics:
            ans[f"radius_{radius}_knn"][metric.__name__] = metric(y_test, y_pred)

    results_grid = pd.DataFrame(data=ans).transpose()
    plot_datas_many_dim(for_plot)
    return results_grid


def test_regression_knn(
    X,
    y,
    min_radius=10.0,
    max_radius=20.0,
    iter=1.0,
    min_neighbors=3,
    max_neighbors=10,
    iter_neighbors=3,
):
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

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=143
    )
    ans = dict()
    metrics = [
        mean_squared_error,
        root_mean_squared_error,
        mean_absolute_error,
        mean_absolute_percentage_error,
        smape,
        wape,
        r2_score,
    ]

    for_plot = dict()
    knn_regressor = KNeighborsRegressor()

    knn_with_ball_tree = KNeighborsRegressor(algorithm="ball_tree")
    knn_with_kd_tree = KNeighborsRegressor(algorithm="kd_tree")
    brute_force_knn = KNeighborsRegressor(algorithm="brute")

    for algo in ["auto", "ball_tree", "kd_tree"]:
        for c_neighbors in range(min_neighbors, max_neighbors, iter_neighbors):
            model = KNeighborsRegressor(n_neighbors=c_neighbors, algorithm=algo)
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            ans[f"{model.__str__()}"] = dict()
            for metric in metrics:
                ans[f"{model.__str__()}"][metric.__name__] = metric(y_test, y_pred)

    # for model in [knn_regressor, knn_with_ball_tree, knn_with_kd_tree, brute_force_knn]:
    #     model.fit(X_train, y_train)
    #     y_pred = model.predict(X_test)
    #     ans[f'{model.__str__()}'] = dict()
    #     for metric in metrics:
    #         ans[f'{model.__str__()}'][metric.__name__] = metric(y_test, y_pred)

    for rad in np.arange(min_radius, max_radius + iter, iter):
        model = RadiusNeighborsRegressor(radius=rad)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        ans[f"{model.__str__()}"] = dict()
        for metric in metrics:
            ans[f"{model.__str__()}"][metric.__name__] = metric(y_test, y_pred)

    results_grid = pd.DataFrame(data=ans).transpose()

    plot_regression(results_grid)
    return results_grid
