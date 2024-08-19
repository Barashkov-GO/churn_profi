import pandas as pd
from catboost import CatBoostRegressor
from lightgbm import LGBMRegressor
from sklearn.ensemble import AdaBoostRegressor
from sklearn.ensemble import BaggingRegressor
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.model_selection import train_test_split
# from tqdm.notebook import tqdm
from sklearn.tree import DecisionTreeRegressor, ExtraTreeRegressor
from xgboost import XGBRegressor

from ml.metrics import *
from ml.plot.plot_metrics import plot_regression


def get_result(df, metric='root_mean_squared_error'):
    df = df.sort_values(by=metric, ascending=True)
    return df


def test_regression(models, X_train, y_train, X_test, y_test, extended_name=False, spec_name=None):
    metrics = [mean_squared_error, root_mean_squared_error, mean_absolute_error, mean_absolute_percentage_error, smape,
               wape, r2_score]
    ans = dict()
    for model in models:
        model.fit(X_train, y_train)
        model_name = model.__str__()
        if extended_name:
            name = model.__str__().split('(')[0]
            params = dict([(k, model.get_params()[k]) for k in model.get_params().keys() if
                           k in ['n_estimators', 'learning_rate', 'max_depth', 'subsample', 'iterations', 'depth'] or name!='XGBRegressor'])
            if spec_name is not None:
                name = spec_name
            model_name = f'{name}_{params}'
        y_pred = model.predict(X_test)
        if model_name not in ans:
            ans[model_name] = dict()
        for metric in metrics:
            ans[model_name][metric.__name__] = metric(y_test, y_pred)
    return ans


def test_regression_trees(X, y, max_depths=None):
    if max_depths is None:
        max_depths = range(1, 10)
    models = [DecisionTreeRegressor, ExtraTreeRegressor, RandomForestRegressor, ExtraTreesRegressor,
              GradientBoostingRegressor, AdaBoostRegressor, BaggingRegressor]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=143)
    models_2 = []
    for model in models:
        if model == DecisionTreeRegressor:
            for deep in max_depths:
                models_2.append(model(max_depth=deep))
        else:
            models_2.append(model())
    ans = test_regression(models_2, X_train, y_train, X_test, y_test)
    results_grid = pd.DataFrame(data=ans).transpose()
    plot_regression(results_grid)
    return get_result(results_grid)


def test_regression_bagging(X, y, bootstrap_features=None, n_estimators=None, deeps=None):
    if deeps is None:
        deeps = [5, 10, 15]
    if n_estimators is None:
        n_estimators = [10, 20, 30]
    if bootstrap_features is None:
        bootstrap_features = [True, False]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=143)
    models = []
    for bf in bootstrap_features:
        for ne in n_estimators:
            for deep in deeps:
                base_regressor = DecisionTreeRegressor(max_depth=deep)
                bagging_regressor = BaggingRegressor(base_regressor, n_estimators=ne, bootstrap_features=bf)
                models.append(bagging_regressor)
    ans = test_regression(models, X_train, y_train, X_test, y_test)
    results_grid = pd.DataFrame(data=ans).transpose()
    plot_regression(results_grid)
    return get_result(results_grid)


def test_regression_random_forest(X, y, criterions=None, n_estimators=None, deeps=None):
    if deeps is None:
        deeps = [5, 10, 15]
    if n_estimators is None:
        n_estimators = [100, 200, 300]
    if criterions is None:
        criterions = ['gini', 'entropy', 'log_loss']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=143)
    models = []
    for c in criterions:
        for ne in n_estimators:
            for deep in deeps:
                models.append(RandomForestRegressor(criterion=c, max_depth=deep, n_estimators=ne))
    ans = test_regression(models, X_train, y_train, X_test, y_test)
    results_grid = pd.DataFrame(data=ans).transpose()
    plot_regression(results_grid)
    return get_result(results_grid)


def test_regression_ada_boost(X, y, n_estimators=None, learning_rates=None):
    if n_estimators is None:
        n_estimators = [5, 20, 45]
    if learning_rates is None:
        learning_rates = [0.01, 0.05, 0.1]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=143)
    models = []
    for lr in learning_rates:
        for ne in n_estimators:
            models.append(AdaBoostRegressor(n_estimators=ne, learning_rate=lr))
    ans = test_regression(models, X_train, y_train, X_test, y_test)
    res = pd.DataFrame(ans).transpose()
    plot_regression(res)
    return get_result(res)


def test_regression_gradient_boost(X, y, n_estimators=None, learning_rates=None, losses=None, criterions=None):
    if n_estimators is None:
        n_estimators = [5, 20, 45]
    if learning_rates is None:
        learning_rates = [0.01, 0.05, 0.1]
    if losses is None:
        losses = ['quantile', 'squared_error', 'absolute_error', 'huber']
    if criterions is None:
        criterions = ['friedman_mse']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=143)
    models = []
    for l in losses:
        for lr in learning_rates:
            for ne in n_estimators:
                for c in criterions:
                    models.append(GradientBoostingRegressor(loss=l, criterion=c, n_estimators=ne, learning_rate=lr))
    ans = test_regression(models, X_train, y_train, X_test, y_test)
    res = pd.DataFrame(ans).transpose()
    plot_regression(res)
    return get_result(res)


def test_regression_xg_boost(X, y, n_estimators=None, learning_rates=None, max_depths=None, sub_samples=None):
    if n_estimators is None:
        n_estimators = [5, 20, 45]
    if learning_rates is None:
        learning_rates = [0.01, 0.1]
    if max_depths is None:
        max_depths = [10, 20, 50]
    if sub_samples is None:
        sub_samples = [0.1, 0.5]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=143)
    models = []
    for m in max_depths:
        for lr in learning_rates:
            for ne in n_estimators:
                for ss in sub_samples:
                    models.append(XGBRegressor(n_estimators=ne, learning_rate=lr, max_depth=m, subsample=ss))

    ans = test_regression(models, X_train, y_train, X_test, y_test, extended_name=True)
    res = pd.DataFrame(ans).transpose()
    plot_regression(res)
    return get_result(res)


def test_regression_cat_boost(X, y, iterations=None, learning_rates=None, depths=None):
    if iterations is None:
        iterations = [5, 20, 45]
    if learning_rates is None:
        learning_rates = [0.01, 0.05, 0.1]
    if depths is None:
        depths = [3, 5, 8]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=143)
    models = []
    for d in depths:
        for lr in learning_rates:
            for i in iterations:
                models.append(CatBoostRegressor(iterations=i, learning_rate=lr, depth=d, logging_level='Silent'))
    ans = test_regression(models, X_train, y_train, X_test, y_test, extended_name=True, spec_name='CatBoost')
    res = pd.DataFrame(ans).transpose()
    plot_regression(res)
    return get_result(res)


def test_regression_light_gbm(X, y, n_estimators=None, learning_rates=None, max_depths=None, sub_samples=None):
    if n_estimators is None:
        n_estimators = [5, 20, 45]
    if learning_rates is None:
        learning_rates = [0.01, 0.1]
    if max_depths is None:
        max_depths = [10, 20, 50]
    if sub_samples is None:
        sub_samples = [0.1, 0.5]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=143)
    models = []
    for m in max_depths:
        for lr in learning_rates:
            for ne in n_estimators:
                for ss in sub_samples:
                    models.append(
                        LGBMRegressor(n_estimators=ne, learning_rate=lr, max_depth=m, subsample=ss, verbose=-1))
    ans = test_regression(models, X_train, y_train, X_test, y_test)
    res = pd.DataFrame(ans).transpose()
    plot_regression(res)
    return get_result(res)
