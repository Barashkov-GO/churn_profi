import matplotlib.pyplot as plt
from sklearn.manifold import TSNE


def plot_data(data, labels):
    plt.scatter(data[:, 0], data[:, 1], marker='.', c=labels, cmap=plt.cm.Spectral)
    plt.show()


def plot_3d(d, l):
    fig = plt.figure()

    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(d[:, 0], d[:, 1], d[:, 2], c=l, cmap=plt.cm.Spectral, marker='.', alpha=0.4)


def plot_datas(datas):
    x = len(datas)
    if len(datas[0][0][0]) > 2:
        fig = plt.figure(figsize=(25, 25))
        for i, [data, labels, title] in enumerate(datas):
            ax = fig.add_subplot((x // 5 + 1 if x % 5 != 0 else x // 5), 5, i + 1, projection='3d')
            ax.scatter(data[:, 0], data[:, 1], data[:, 2], marker='.', c=labels, cmap=plt.cm.Spectral)
            ax.set_title(title)
        plt.tight_layout()
        plt.show()
        return

    fig, axs = plt.subplots(nrows=(x // 5 + 1 if x % 5 != 0 else x // 5), ncols=5, figsize=(25, 25))
    for i, [data, labels, title] in enumerate(datas):
        axs[i // 5][i % 5].scatter(data[:, 0], data[:, 1], marker='.', c=labels, cmap=plt.cm.Spectral)
        axs[i // 5][i % 5].set_title(title)
    plt.tight_layout()
    plt.show()


def plot_data_many_dim(data, labels):
    tsne = TSNE(n_components=2, random_state=143)
    data = tsne.fit_transform(data)
    plot_data(data, labels)


def plot_datas_many_dim(datas, count=5):
    tsne = TSNE(n_components=2, random_state=143)

    x = len(datas)
    figsize = (25, 25)
    if (x // count + 1 if x % count != 0 else x // count) == 2:
        figsize = (25, 15)
    fig, axs = plt.subplots(nrows=(x // count + 1 if x % count != 0 else x // count), ncols=count, figsize=figsize)
    for i, [data, labels, title] in enumerate(datas):
        data = tsne.fit_transform(data)
        axs[i // count][i % count].scatter(data[:, 0], data[:, 1], marker='.', c=labels, cmap=plt.cm.Spectral)
        axs[i // count][i % count].set_title(title)
    plt.tight_layout()
    plt.title('t-SNE visualisation')
    plt.show()
