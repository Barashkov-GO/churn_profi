def grid_search_cv_with_thresholds(model, grid, thresholds, X, y, scoring_func='roc_auc', cv=3):
    from sklearn.metrics import confusion_matrix
    from sklearn.model_selection import train_test_split, GridSearchCV
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
    from pretty_confusion_matrix import pp_matrix
    import pandas as pd

    metrics = [accuracy_score, precision_score, recall_score, f1_score, roc_auc_score]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    grid_search = GridSearchCV(estimator=model, param_grid=grid, cv=cv, scoring=scoring_func, n_jobs=-1, refit=True, verbose=2, error_score='raise')

    grid_search.fit(X_train, y_train)

    best = grid_search.best_estimator_
    ans_all = dict()
    probs = grid_search.predict_proba(X_test)[:, 1]
    for threshold in thresholds:
        ans = dict()
        y_pred = (probs >= threshold).astype(int)
        cm = confusion_matrix(y_test, y_pred)
        df_cm = pd.DataFrame(cm, index=range(0, 2), columns=range(0, 2))
        print(f'Threshold {threshold}')
        pp_matrix(df_cm, cmap='plasma', cbar=True, figsize=(5, 7), show_null_values=True, pred_val_axis='x')
        print()
        tn, fp, fn, tp = [i / sum(cm.ravel()) for i in cm.ravel()]
        ans['tn'] = tn
        ans['fp'] = fp
        ans['fn'] = fn
        ans['tp'] = tp

        for metric in metrics:
            ans[metric.__name__] = metric(y_test, y_pred)

        ans_all[threshold] = ans
    return ans_all, best, grid_search