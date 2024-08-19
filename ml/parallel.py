import numpy as np
import pandas as pd
import numpy as np
from joblib import Parallel, delayed
from tqdm.notebook import tqdm
import multiprocessing
import warnings

warnings.filterwarnings("ignore")


def apply_par_col(data, columns, func, **kwargs):
    func_name = func.__str__().split(" ")[1]
    results = Parallel(n_jobs=multiprocessing.cpu_count())(
        delayed(func)(data, col, **kwargs)
        for col in tqdm(
            columns, desc=f"Processing func: {func_name}", total=len(columns)
        )
    )
    result = pd.concat(results, axis=1)
    return pd.concat([data.drop(columns, axis=1), result], axis=1)


def apply_par_row(data, columns, func, **kwargs):
    """ For hard and slow functions
    """
    func_name = func.__str__().split(" ")[1]

    results = Parallel(n_jobs=multiprocessing.cpu_count())(
        delayed(func)(row, **kwargs)
        for _, row in tqdm(
            data[columns].iterrows(),
            desc=f"Processing func: {func_name}",
            total=data.shape[0],
        )
    )
    result_df = pd.DataFrame(results, index=data.index)
    return result_df


def apply_par_row_batches(data, columns, func, batch_size=1_000, **kwargs):
    """ For small and fast functions
    """
    func_name = func.__str__().split(" ")[1]
    num_batches = int(np.ceil(data.shape[0] / batch_size))

    def process_batch(batch):
        return batch.apply(func, axis=1, **kwargs)

    results = Parallel(n_jobs=multiprocessing.cpu_count())(
        delayed(process_batch)(
            data.iloc[i * batch_size : (i + 1) * batch_size][columns]
        )
        for i in tqdm(
            range(num_batches), desc=f"Processing func: {func_name}", total=num_batches
        )
    )

    result_df = pd.concat(results, axis=0)
    return result_df


def apply_par_group(dfGrouped, func, **kwargs):
    func_name = func.__str__().split(" ")[1]

    result_list = Parallel(n_jobs=multiprocessing.cpu_count())(
        delayed(func)(group, **kwargs)
        for _, group in tqdm(
            dfGrouped, desc=f"Processing func: {func_name}", total=len(dfGrouped)
        )
    )
    result = pd.DataFrame(result_list, index=[name for name, _ in dfGrouped])
    return result
