
def plot_param_scores_dependence(cv_result):
    import matplotlib.pyplot as plt
    params, scores, scores_std = cv_result.__dict__['cv_results_']['params'], cv_result.__dict__['cv_results_']['mean_test_score'], cv_result.__dict__['cv_results_']['std_test_score']
    param_names = list(params[0].keys())

    fig, axs = plt.subplots(len(param_names), 1, figsize=(10, 7))
    if len(param_names) == 1:
        axs = [axs]
    else:
        axs = axs.flat
    for i, ax in enumerate(axs):
        param_name = param_names[i]
        params_ = [i[param_name] for i in params]
        ax.scatter(params_, scores, color='b')
        ax.fill_between(params_, scores - scores_std, scores + scores_std, alpha=0.3, color='r')
        ax.set_xlabel(f'Params ({param_name})')
        ax.set_ylabel('Score')
    fig.tight_layout()
    

def plot_threshold_scores_dependence(y_true, y_pred_proba, left=0.05, right=1.0):
    import matplotlib.pyplot as plt
    from sklearn.metrics import confusion_matrix
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
    import numpy as np

    thresholds = np.arange(left, right, 0.01)
    metrics = [accuracy_score, precision_score, recall_score, f1_score, roc_auc_score]

    def get_data():
        ans_all = dict([(f'{i.__name__}', []) for i in metrics] + [(i, []) for i in ['tp', 'fp', 'tn', 'fn']])

        for threshold in thresholds:
            y_pred = (y_pred_proba >= threshold).astype(int)
            cm = confusion_matrix(y_true, y_pred)
            tn, fp, fn, tp = [i / sum(cm.ravel()) for i in cm.ravel()]
            ans_all['tn'].append(tn)
            ans_all['fp'].append(fp)
            ans_all['fn'].append(fn)
            ans_all['tp'].append(tp)

            for metric in metrics:
                ans_all[metric.__name__].append(metric(y_true, y_pred))
        return ans_all

    ans = get_data()

    fig, axs = plt.subplots(2, 1, figsize=(15, 10))
    for metric in metrics:
        axs[0].plot(thresholds, ans[metric.__name__], label=f'{metric.__name__}')

    axs[0].set_xlabel('Thresholds')
    axs[0].set_ylabel('Metric values')
    axs[0].set_xticks(np.arange(left, right, 0.05))
    axs[0].set_yticks(np.arange(0, 1, 0.05))
    axs[0].grid(visible=True)
    axs[0].legend()

    for t in ['tp', 'fp', 'tn', 'fn']:
        axs[1].plot(thresholds, ans[t], label=f'{t} normalized')
    
    axs[1].set_xlabel('Thresholds')
    axs[1].set_ylabel('Normalized counts')
    axs[1].set_xticks(np.arange(left, right, 0.05))
    axs[1].set_yticks(np.arange(0, 1, 0.05))
    axs[1].grid(visible=True)
    axs[1].legend()

    fig.tight_layout()