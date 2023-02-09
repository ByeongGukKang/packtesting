import numpy as np

from . import core_ts_numba_inner

def ts_rank(pba, start, end, data, *args):
    """Inner function to calulate rank.

    Args:
        data (2d-np.array): array, length is lookback period
        lookback (int): lookback period
        *args (tuple): delivers function settings
    
    Returns:
        2d-np.array
    """
    data = data[0]
    lookback = args[0][0]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        result[j] = core_ts_numba_inner.ts_rank_inner(data[j], lookback, number_of_days)
        # tqdm update
        pba.update.remote(1)

    return result

def ts_zscore(pba, start, end, data, *args):
    """Inner function to calulate zscore.

    Args:
        data (2d-np.array): array, length is lookback period
        lookback (int): lookback period
        *args (tuple): delivers function settings
    
    Returns:
        2d-np.array
    """
    data = data[0]
    lookback = args[0][0]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        result[j] = core_ts_numba_inner.ts_zscore_inner(data[j], lookback, number_of_days)
        # tqdm update
        pba.update.remote(1)

    return result

def ts_winsorize(pba, start, end, data, *args):
    """Inner function to calculate winsorization.

    Args:
        data (2d-np.array): array, length is lookback period
        lookback (int): lookback period
        *args (): delivers function settings
    
    Returns:
        2d-np.array
    """
    data = data[0]
    lookback = args[0][0]
    sigma = args[0][1]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        result[j] = core_ts_numba_inner.ts_winsorize_inner(data[j], lookback, number_of_days, sigma)
        # tqdm update
        pba.update.remote(1)

    return result

def ts_truncate(pba, start, end, data, *args):
    """Inner function to truncate.

    Args:
        data (2d-np.array): input data
        lookback (int): lookback period
        *args (): delivers function settings
    
    Returns:
        2d-np.array
    """
    data = data[0]
    lookback = args[0][0]
    maxPercent = args[0][1]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        result[j] = core_ts_numba_inner.ts_winsorize_inner(data[j], lookback, number_of_days, maxPercent)
        # tqdm update
        pba.update.remote(1)

    return result

def ts_corr_pearson(pba, start, end, data, *args):
    """Inner function to calculate Pearson's correlation.

    Args:
        y_mat (2d-np.array): data
        x_vec (1d-np.array): data
        *args (): delivers function settings
    
    Returns:
        2d-np.array
    """
    y_mat = data[0]
    x_vec = data[1]
    lookback = args[0][0]

    result = np.zeros_like(y_mat)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        result[j] = core_ts_numba_inner.ts_corr_pearson_inner(y_mat[j], x_vec, lookback, number_of_days)
        # tqdm update
        pba.update.remote(1)

    return result

def ts_corr_spearman(pba, start, end, data, *args):
    """Inner function to calculate Spearman's rank correlation.

    Args:
        y_mat (2d-np.array): data
        x_vec (1d-np.array): data
        *args (): delivers function settings
    
    Returns:
        2d-np.array
    """
    y_mat = data[0]
    x_vec = data[1]
    lookback = args[0][0]

    result = np.zeros_like(y_mat)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        result[j] = core_ts_numba_inner.ts_corr_spearman_inner(y_mat[j], x_vec, lookback, number_of_days)
        # tqdm update
        pba.update.remote(1)

    return result

# def ts_corr_kendall(y_mat, x_vec, lookback, **kargs):
#     """Inner function to calculate Kendall's rank correlation.

#     Args:
#         y_mat (2d-np.array): data
#         x_vec (1d-np.array): data
#         lookback (int): lookback period
#         **kargs (): delivers function settings
    
#     Returns:
#         2d-np.array
#     """
#     result = np.zeros_like(y_mat) * np.nan
#     number_of_stocks = result.shape[0]
#     number_of_days = result.shape[1] + 1

#     for j in tqdm(range(number_of_stocks), disable=kargs["_off_tqdm"], desc="ts_truncate"):
#         arr = y_mat[j]
#         for i in range(lookback, number_of_days):
#             # select window
#             _y_arr = arr[i-lookback:i]
#             _x_arr = x_vec[i-lookback:i]
#             # prepare nan mask
#             y_mask = _y_arr * 0
#             x_mask = _x_arr * 0
#             # calculate ranks
#             y_ranks = _y_arr.argsort(kind='mergesort').argsort(kind='mergesort')
#             y_ranks = np.where(y_ranks>=(y_ranks.shape[0]-np.isnan(_y_arr).sum()), np.nan, y_ranks)
#             x_ranks = _x_arr.argsort(kind='mergesort').argsort(kind='mergesort')
#             x_ranks = np.where(x_ranks>=(x_ranks.shape[0]-np.isnan(_x_arr).sum()), np.nan, x_ranks)
#             # nan masking
#             y_ranks = y_mask * y_ranks
#             x_ranks = x_mask * x_ranks
#             # calculate correlation
#             result[j,i-1] = np.corrcoef(y_ranks, x_ranks)[0,1]

#     return result

### Applies don't support use_numba = True.
def ts_apply(pba, start, end, data, *args):
    """Apply user-defined function.

    Args:
        data (2d-np.array): input data
        *args (): arguments for user-defined function

    Returns:
        2d-np.array
    """
    data = data[0]
    lookback = args[0][0]
    func = args[0][1]
    inner_args = args[0][2]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        arr = data[j]
        for i in range(lookback, number_of_days):
            _arr = arr[i-lookback:i].copy()
            if np.sum(np.isnan(_arr)) == lookback:
                continue
            else:
                result[j,i-1] = func(_arr, *inner_args)
        # tqdm update
        pba.update.remote(1)

    return result

def ts_dual_apply(pba, start, end, data, *args):
    """Apply user-defined function with two data arguments.
    
    Args:
        y_mat (2d-np.array): input data1
        x_mat (2d-np.array): input data2
        lookback (int): lookback period
        func (function): function to apply
        *args (): arguments for user-defined function

    Returns:
        2d-np.array
    """
    y_mat = data[0]
    x_mat = data[1]

    lookback = args[0][0]
    func = args[0][1]
    inner_args = args[0][2]

    result = np.zeros_like(y_mat)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        y_arr = y_mat[j,:]
        x_arr = x_mat[j,:]
        for i in range(lookback, number_of_days):
            _y_arr = y_arr[i-lookback:i].copy()
            _x_arr = x_arr[i-lookback:i].copy()
            if np.sum(np.isnan(_y_arr)) == lookback:
                continue
            elif np.sum(np.isnan(_x_arr)) == lookback:
                continue
            else:
                result[j,i-1] = func(_y_arr, _x_arr, *inner_args)
        # tqdm update
        pba.update.remote(1)

    return result

def ts_multi_apply(pba, start, end, data, *args):
    """Apply user-defined function with multiple data arguments.
    
    Args:
        lookback (int): lookback period
        *args (): delivers function settings
    
    Returns:
        2d-np.array
    """
    lookback = args[0][0]
    func = args[0][1]
    inner_args = list(args[0][2])

    result = np.zeros_like(data[0])
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        for i in range(lookback, number_of_days):
            data_args_arr = []
            for each_data in data:
                data_args_arr.append(each_data[j,:][i-lookback:i])

            result_args = tuple(data_args_arr + inner_args)
            result[j,i-1] = func(*result_args)
        # tqdm update
        pba.update.remote(1)

    return result