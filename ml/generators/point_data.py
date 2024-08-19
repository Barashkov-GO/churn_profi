import numpy as np


def generate_u_data(num_samples=500, noise_level=0.25, x=4, y=3):
    num_classes = 2
    num_samples_per_class = num_samples // num_classes
    class_centers = np.array([[x, y], [x - 1, y - 1]])
    data = []
    labels = []
    for class_id in range(num_classes):
        t = np.linspace(0, np.pi, num_samples_per_class)
        if class_id == 0:
            x = -2.0 * np.sin(t) + class_centers[class_id, 0]
            y = 1.0 * np.cos(t) + class_centers[class_id, 1]
        else:
            x = 2.0 * np.sin(t) + class_centers[class_id, 0]
            y = -1.0 * np.cos(t) + class_centers[class_id, 1]

        x += np.random.normal(0, noise_level, num_samples_per_class)
        y += np.random.normal(0, noise_level, num_samples_per_class)

        data.extend(np.column_stack((x, y)))
        labels.extend([class_id] * num_samples_per_class)

    data = np.array(data)
    labels = np.array(labels)
    permutation = np.random.permutation(num_samples)
    data = data[permutation]
    labels = labels[permutation]

    return data, labels


def generate_circle_data(num_samples, inner_radius, outer_radius, noise_level, center):
    def get_datas(num_samples, inner_radius, outer_radius, noise_level, center):
        data = []
        labels = []

        for _ in range(num_samples):
            x = np.random.uniform(center[0] - outer_radius, center[0] + outer_radius)
            y = np.random.uniform(center[1] - outer_radius, center[1] + outer_radius)

            distance_to_center = np.sqrt((x - center[0]) ** 2 + (y - center[1]) ** 2)
            is_inside_circle = distance_to_center < inner_radius

            x += np.random.normal(0, noise_level)
            y += np.random.normal(0, noise_level)

            data.append([x, y])
            labels.append(0 if is_inside_circle else 1)
        return np.array(data), np.array(labels)

    data, labels = get_datas(num_samples, inner_radius, outer_radius, noise_level, center)
    data_2, labels_2 = get_datas(num_samples - 100, inner_radius + 1.0, outer_radius + 0.5, noise_level,
                                 (center[0] + 5.5, center[1]))
    labels_2 = np.array([abs(1 - i) for i in labels_2])
    data_all = np.vstack((data, data_2))
    labels_all = np.concatenate((labels, labels_2))

    return np.array(data_all), np.array(labels_all)


def generate_xor_data(num_samples=500, noise_level=0.25, left=-5, right=5, upper=5, lower=-5):
    data = []
    labels = []

    for _ in range(num_samples):
        x = np.random.uniform(left, right)
        y = np.random.uniform(lower, upper)

        is_class_1 = (x < 0 and y < 0) or (x >= 0 and y >= 0)  # Проверяем условие для класса 1

        x += np.random.normal(0, noise_level)
        y += np.random.normal(0, noise_level)

        data.append([x, y])
        labels.append(int(is_class_1))

    return np.array(data), np.array(labels)


def generate_outlier_data(num_samples=500, noise_level=0.5):
    data = []
    labels = []

    for _ in range(num_samples):
        cl = np.random.rand() > 0.5
        if cl:
            x = np.random.uniform(2.5, 10)
        else:
            x = np.random.uniform(-10, -2.5)

        is_outlier = np.random.rand() < 0.2

        if cl and is_outlier:
            x = np.random.uniform(-1, 1)
            y = np.random.uniform(8, 10)
        elif is_outlier:
            x = np.random.uniform(-1, 1)
            y = np.random.uniform(-10, -8)
        else:
            y = np.random.uniform(-5, 5)

        x += np.random.normal(0, noise_level)
        y += np.random.normal(0, noise_level)

        data.append([x, y])
        labels.append(int(cl))

    return np.array(data), np.array(labels)


def generate_svm_canon(num_samples=500, center=(0, 0), outer_radius=7, inner_radius=3, noise_level: float = 0.15):
    data = []
    labels = []

    for _ in range(num_samples):
        is_inside_circle = np.random.rand() > 0.5
        is_in = False
        while not is_in:
            if is_inside_circle:
                x = np.random.uniform(center[0] - inner_radius, center[0] + inner_radius)
                y = np.random.uniform(center[1] - inner_radius, center[1] + inner_radius)
            else:
                x = np.random.uniform(center[0] - outer_radius, center[0] + outer_radius)
                y = np.random.uniform(center[1] - outer_radius, center[1] + outer_radius)

            distance_to_center = np.sqrt((x - center[0]) ** 2 + (y - center[1]) ** 2)
            is_in = (is_inside_circle and (distance_to_center < inner_radius)) or (not is_inside_circle and (
                    outer_radius > distance_to_center > inner_radius))

        x += np.random.normal(0, noise_level)
        y += np.random.normal(0, noise_level)

        data.append([x, y])
        labels.append(0 if is_inside_circle else 1)
    return np.array(data), np.array(labels)
