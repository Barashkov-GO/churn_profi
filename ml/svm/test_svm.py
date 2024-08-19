import numpy as np
import pandas as pd
# from tqdm.notebook import tqdm

from sklearn.svm import SVC, SVR
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score

from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_log_error
from sklearn.svm import SVR, NuSVR

from ml.plot.plot_points import plot_datas, plot_data, plot_datas_many_dim

from ml.plot.plot_metrics import plot_regression


def test_classification_svm(X, y):
    kernels = ['linear', 'poly', 'rbf', 'sigmoid']
    cs = np.arange(0.5, 1.5, 0.25)
    degrees_2 = [i for i in range(1, 3)]
    for_plots = []
    ans = dict()
    metrics = [accuracy_score, precision_score, recall_score, f1_score, roc_auc_score]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)

    for kernel in kernels:
        for c in cs:
            if kernel == 'poly':
                for deg in degrees_2:
                    svm = SVC(kernel=kernel, C=c, degree=deg)
                    svm.fit(X_train, y_train)
                    y_pred = svm.predict(X_test)
                    for_plots.append([X_test, y_pred, f'{kernel}-c{c}-d{deg} svm'])
                    ans[f'{kernel}_svm_{c}_{deg}'] = dict()
                    for metric in metrics:
                        ans[f'{kernel}_svm_{c}_{deg}'][metric.__name__] = metric(y_test, y_pred)
            else:
                svm = SVC(kernel=kernel, C=c)
                svm.fit(X_train, y_train)
                y_pred = svm.predict(X_test)
                for_plots.append([X_test, y_pred, f'{kernel}-{c} svm'])
                ans[f'{kernel}_svm_{c}'] = dict()
                for metric in metrics:
                    ans[f'{kernel}_svm_{c}'][metric.__name__] = metric(y_test, y_pred)
            print(f'Finished: kernel {kernel}, c {c}')

    plot_datas_many_dim(for_plots)
    return pd.DataFrame(ans).transpose()


def test_regression_svm(X, y, c_min, c_max, c_iter):
    kernels = ['linear', 'poly', 'rbf', 'sigmoid']

    cs = np.arange(c_min, c_max, c_iter)
    degrees_2 = [i for i in range(2, 4)]


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

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=143)
    ans = dict()
    metrics = [mean_squared_error, root_mean_squared_error, mean_absolute_error, mean_absolute_percentage_error, smape,
               wape, r2_score]

    for kernel in kernels:
        for c in cs:
            if kernel == 'poly':
                for deg in degrees_2:
                    svm = SVR(kernel=kernel, C=c, degree=deg)
                    svm.fit(X_train, y_train)
                    y_pred = svm.predict(X_test)
                    ans[f'{svm.__str__()}'] = dict()
                    for metric in metrics:
                        ans[f'{svm.__str__()}'][metric.__name__] = metric(y_test, y_pred)
            else:
                svm = SVR(kernel=kernel, C=c)
                svm.fit(X_train, y_train)
                y_pred = svm.predict(X_test)
                ans[f'{svm.__str__()}'] = dict()
                for metric in metrics:
                    ans[f'{svm.__str__()}'][metric.__name__] = metric(y_test, y_pred)
    results_grid = pd.DataFrame(data=ans).transpose()

    plot_regression(results_grid)
    return results_grid
