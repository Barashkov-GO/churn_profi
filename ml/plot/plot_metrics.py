import matplotlib.pyplot as plt
import seaborn as sns

def plot_regression(df):
    sns.set(style="whitegrid")

    names = df.index.values
    metrics = df.columns.values
    x = len(metrics)
    fig, axs = plt.subplots(nrows=x // 2 + 1, ncols=2, figsize=(25, 25))
    for i, metric in enumerate(metrics):
        # axs[i // 2][i % 2].bar(names, df[metric], label=f'{metric}')
        #
        # axs[i // 2][i % 2].set_xticklabels(names, rotation=90)
        # axs[i // 2][i % 2].legend()
        bar_plot = sns.barplot(x=metric, y=names, data=df, color='skyblue', ax=axs[i // 2][i % 2])
        axs[i // 2][i % 2].set_title(f'{metric}')

    plt.title('Результаты методов машинного обучения по метрикам')
    plt.xlabel('Значение метрик')
    plt.tight_layout()
    plt.show()


def plot_regression_2(df):
    names = df.index.values
    metrics = df.columns.values
    x = len(metrics)
    fig, axs = plt.subplots(nrows=x // 2 + 1, ncols=2, figsize=(25, 25))
    for i, metric in enumerate(metrics):
        axs[i // 2][i % 2].bar(names, df[metric], label=f'{metric}')

        axs[i // 2][i % 2].set_xticklabels(names, rotation=90)
        axs[i // 2][i % 2].legend()

    plt.show()
