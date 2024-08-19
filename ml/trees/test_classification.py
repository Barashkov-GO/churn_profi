import pandas as pd
from catboost import CatBoostClassifier
from lightgbm import LGBMClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import BaggingClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

# from tqdm.notebook import tqdm
from sklearn.tree import DecisionTreeClassifier, ExtraTreeClassifier
from xgboost import XGBClassifier

from ml.plot.plot_points import plot_datas_many_dim


def get_result(df, metric="roc_auc_score"):
    df = df.sort_values(by=metric, ascending=False)
    best = df.iloc[0, 0]
    return df


def test_classification(
    models, X_train, y_train, X_test, y_test, extended_name=False, spec_name=None
):
    ans = dict()
    for_plots = []
    metrics = [accuracy_score, precision_score, recall_score, f1_score, roc_auc_score]
    for model in models:
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        model_name = model.__str__()
        if extended_name:
            name = model.__str__().split("(")[0]
            params = dict(
                [
                    (k, model.get_params()[k])
                    for k in model.get_params().keys()
                    if k
                    in [
                        "n_estimators",
                        "learning_rate",
                        "max_depth",
                        "subsample",
                        "iterations",
                        "depth",
                    ]
                    or name != "XGBClassifier"
                ]
            )
            if spec_name is not None:
                name = spec_name
            model_name = f"{name}_{params}"
        for_plots.append([X_test, y_pred, f"{model_name}"])
        if model_name not in ans:
            ans[model_name] = dict()
        for metric in metrics:
            ans[model_name][metric.__name__] = metric(y_test, y_pred)
    return ans, for_plots


def test_classification_trees(X, y):
    models = [
        DecisionTreeClassifier(),
        ExtraTreeClassifier(),
        RandomForestClassifier(),
        ExtraTreesClassifier(),
        GradientBoostingClassifier(),
        AdaBoostClassifier(),
        BaggingClassifier(),
    ]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=143
    )
    ans, for_plots = test_classification(models, X_train, y_train, X_test, y_test)

    plot_datas_many_dim(for_plots)
    res = pd.DataFrame(ans).transpose()
    return get_result(res)


def test_classification_bagging(
    X, y, bootstrap_features=None, n_estimators=None, deeps=None
):
    if deeps is None:
        deeps = [5, 10, 15]
    if n_estimators is None:
        n_estimators = [10, 20, 30]
    if bootstrap_features is None:
        bootstrap_features = [True, False]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=143
    )
    models = []
    for bf in bootstrap_features:
        for ne in n_estimators:
            for deep in deeps:
                base_classifier = DecisionTreeClassifier(max_depth=deep)
                bagging_classifier = BaggingClassifier(
                    base_classifier, n_estimators=ne, bootstrap_features=bf
                )
                models.append(bagging_classifier)
    ans, for_plots = test_classification(models, X_train, y_train, X_test, y_test)
    plot_datas_many_dim(for_plots, count=4)
    res = pd.DataFrame(ans).transpose()
    return get_result(res)


def test_classification_random_forest(
    X, y, criterions=None, n_estimators=None, deeps=None
):
    if deeps is None:
        deeps = [5, 10, 15]
    if n_estimators is None:
        n_estimators = [100, 200, 300]
    if criterions is None:
        criterions = ["gini", "entropy", "log_loss"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=143
    )
    models = []
    for c in criterions:
        for ne in n_estimators:
            for deep in deeps:
                models.append(
                    RandomForestClassifier(criterion=c, max_depth=deep, n_estimators=ne)
                )
    ans, for_plots = test_classification(models, X_train, y_train, X_test, y_test)
    plot_datas_many_dim(for_plots, count=4)
    res = pd.DataFrame(ans).transpose()
    return get_result(res)


def test_classification_ada_boost(
    X, y, n_estimators=None, learning_rates=None, algorithms=None
):
    if n_estimators is None:
        n_estimators = [5, 20, 45]
    if learning_rates is None:
        learning_rates = [0.01, 0.05, 0.1]
    if algorithms is None:
        algorithms = ["SAMME.R"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=143
    )
    models = []
    for lr in learning_rates:
        for ne in n_estimators:
            for algo in algorithms:
                models.append(
                    AdaBoostClassifier(
                        n_estimators=ne, learning_rate=lr, algorithm=algo
                    )
                )
    ans, for_plots = test_classification(models, X_train, y_train, X_test, y_test)
    plot_datas_many_dim(for_plots, count=4)
    res = pd.DataFrame(ans).transpose()
    return get_result(res)


def test_classification_gradient_boost(
    X, y, n_estimators=None, learning_rates=None, losses=None, criterions=None
):
    if n_estimators is None:
        n_estimators = [5, 20, 45]
    if learning_rates is None:
        learning_rates = [0.01, 0.05, 0.1]
    if losses is None:
        losses = ["log_loss", "exponential"]
    if criterions is None:
        criterions = ["friedman_mse"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=143
    )
    models = []
    for l in losses:
        for lr in learning_rates:
            for ne in n_estimators:
                for c in criterions:
                    models.append(
                        GradientBoostingClassifier(
                            loss=l, criterion=c, n_estimators=ne, learning_rate=lr
                        )
                    )
    ans, for_plots = test_classification(models, X_train, y_train, X_test, y_test)
    plot_datas_many_dim(for_plots, count=4)
    res = pd.DataFrame(ans).transpose()
    return get_result(res)


def test_classification_xg_boost(
    X, y, n_estimators=None, learning_rates=None, max_depths=None, sub_samples=None
):
    if n_estimators is None:
        n_estimators = [5, 20, 45]
    if learning_rates is None:
        learning_rates = [0.01, 0.1]
    if max_depths is None:
        max_depths = [10, 20, 50]
    if sub_samples is None:
        sub_samples = [0.1, 0.5]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=143
    )
    models = []
    for m in max_depths:
        for lr in learning_rates:
            for ne in n_estimators:
                for ss in sub_samples:
                    models.append(
                        XGBClassifier(
                            n_estimators=ne, learning_rate=lr, max_depth=m, subsample=ss
                        )
                    )

    ans, for_plots = test_classification(
        models, X_train, y_train, X_test, y_test, extended_name=True
    )
    plot_datas_many_dim(for_plots, count=4)
    res = pd.DataFrame(ans).transpose()
    return get_result(res)


def test_classification_cat_boost(
    X, y, iterations=None, learning_rates=None, depths=None
):
    if iterations is None:
        iterations = [5, 20, 45]
    if learning_rates is None:
        learning_rates = [0.01, 0.05, 0.1]
    if depths is None:
        depths = [3, 5, 8]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=143
    )
    models = []
    for d in depths:
        for lr in learning_rates:
            for i in iterations:
                models.append(
                    CatBoostClassifier(
                        iterations=i, learning_rate=lr, depth=d, logging_level="Silent"
                    )
                )
    ans, for_plots = test_classification(
        models,
        X_train,
        y_train,
        X_test,
        y_test,
        extended_name=True,
        spec_name="CatBoost",
    )
    plot_datas_many_dim(for_plots, count=4)
    res = pd.DataFrame(ans).transpose()
    return get_result(res)


def test_classification_light_gbm(
    X, y, n_estimators=None, learning_rates=None, max_depths=None, sub_samples=None
):
    if n_estimators is None:
        n_estimators = [5, 20, 45]
    if learning_rates is None:
        learning_rates = [0.01, 0.1]
    if max_depths is None:
        max_depths = [10, 20, 50]
    if sub_samples is None:
        sub_samples = [0.1, 0.5]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=143
    )
    models = []
    for m in max_depths:
        for lr in learning_rates:
            for ne in n_estimators:
                for ss in sub_samples:
                    models.append(
                        LGBMClassifier(
                            n_estimators=ne,
                            learning_rate=lr,
                            max_depth=m,
                            subsample=ss,
                            verbose=-1,
                        )
                    )
    ans, for_plots = test_classification(models, X_train, y_train, X_test, y_test)
    plot_datas_many_dim(for_plots, count=5)
    res = pd.DataFrame(ans).transpose()
    return get_result(res)
