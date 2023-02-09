import numpy as np
from numba import jit_module

def ts_rank_inner(arr, lookback, number_of_days):
    """Inner function for ts_rank numba iteration.

    Args:
        j (int): order of original data, used to recover the original sequence
        arr (1d-np.array): array, length is lookback period
        lookback (int): lookback period
        number_of_days (int): total data period
    
    Returns:
        1d-np.array
    """
    inner_result = np.zeros(arr.shape[0]) * np.nan
    for i in range(lookback, number_of_days):
        _arr = arr[i-lookback:i].copy()
        if np.sum(np.isnan(_arr)) == lookback:
            continue
        else:
            ranks = _arr.argsort(kind='mergesort').argsort(kind='mergesort')
            ranks = np.where(ranks>=(ranks.shape[0]-np.isnan(_arr).sum()), np.nan, ranks)
            inner_result[i-1] = (ranks/np.nanmax(ranks))[-1]

    return inner_result

def ts_zscore_inner(arr, lookback, number_of_days):
    """Inner function for ts_zscore numba iteration.

    Args:
        j (int): order of original data, used to recover the original sequence
        arr (1d-np.array): array, length is lookback period
        lookback (int): lookback period
        number_of_days (int): total data period
    
    Returns:
        1d-np.array
    """
    inner_result = np.zeros(arr.shape[0]) * np.nan
    for i in range(lookback, number_of_days):
        _arr = arr[i-lookback:i].copy()
        if np.sum(np.isnan(_arr)) == lookback:
            continue
        else:
            inner_result[i-1] = np.divide(_arr - np.nanmean(_arr), np.nanstd(_arr))[-1]

    return inner_result

def ts_winsorize_inner(arr, lookback, number_of_days, sigma):
    """Inner function for ts_winsorize numba iteration.

    Args:
        j (int): order of original data, used to recover the original sequence
        arr (1d-np.array): array, length is lookback period
        lookback (int): lookback period
        number_of_days (int): total data period
        sigma (int/float): winsorizing hurdle
    
    Returns:
        1d-np.array
    """
    inner_result = np.zeros(arr.shape[0]) * np.nan
    for i in range(lookback, number_of_days):
        _arr = arr[i-lookback:i].copy()
        if np.sum(np.isnan(_arr)) == lookback:
            continue
        else:
            high_adjust = np.where(_arr>np.nanmean(_arr)+sigma*np.nanstd(_arr), np.nanmean(_arr)+sigma*np.nanstd(_arr), 0)
            low_adjust = np.where(_arr<np.nanmean(_arr)-sigma*np.nanstd(_arr), np.nanmean(_arr)-sigma*np.nanstd(_arr), 0)
            tmp = high_adjust + low_adjust
            inner_result[i-1] = (np.where(tmp==0, _arr, tmp))[-1]

    return inner_result

def ts_truncate_inner(arr, lookback, number_of_days, maxPercent):
    """Inner function for ts_truncate numba iteration.

    Args:
        j (int): order of original data, used to recover the original sequence
        arr (1d-np.array): array, length is lookback period
        lookback (int): lookback period
        number_of_days (int): total data period
        maxPercent (int/float): truncate hurdle, truncate if data is larger than the hurdle
    
    Returns:
        1d-np.array
    """
    inner_result = np.zeros(arr.shape[0]) * np.nan
    for i in range(lookback, number_of_days):
        _arr = arr[i-lookback:i].copy()
        if np.sum(np.isnan(_arr)) == lookback:
            continue
        else:
            available_max = np.nansum(_arr) * maxPercent
            inner_result[i-1] = (np.where(_arr>available_max, available_max, _arr))[-1]

    return inner_result

def ts_corr_pearson_inner(y_arr, x_arr, lookback, number_of_days):
    """Inner function for ts_corr_pearson numba iteration.

    Args:
        j (int): order of original data, used to recover the original sequence
        y_arr (1d-np.array): array, length is lookback period
        x_arr (1d-np.array): array, length is lookback period
        lookback (int): lookback period
        number_of_days (int): total data period
    
    Returns:
        1d-np.array
    """
    inner_result = np.zeros(y_arr.shape[0]) * np.nan
    for i in range(lookback, number_of_days):
        _y_arr = y_arr[i-lookback:i].copy()
        _x_arr = x_arr[i-lookback:i].copy()
        if np.sum(np.isnan(_y_arr+_x_arr)) != 0:
            continue
        else:
            inner_result[i-1] = np.corrcoef(_y_arr, _x_arr)[0,1]
            
    return inner_result

def ts_corr_spearman_inner(y_arr, x_arr, lookback, number_of_days):
    """Inner function for ts_corr_spearman numba iteration.

    Args:
        j (int): order of original data, used to recover the original sequence
        y_arr (1d-np.array): array, length is lookback period
        x_arr (1d-np.array): array, length is lookback period
        lookback (int): lookback period
        number_of_days (int): total data period
    
    Returns:
        1d-np.array
    """
    inner_result = np.zeros(y_arr.shape[0]) * np.nan
    for i in range(lookback, number_of_days):
        # select window
        _y_arr = y_arr[i-lookback:i].copy()
        _x_arr = x_arr[i-lookback:i].copy()
        if np.sum(np.isnan(_y_arr)+np.isnan(_x_arr)) != 0:
            continue
        else:
            # calculate ranks
            y_ranks = _y_arr.argsort(kind='mergesort').argsort(kind='mergesort')
            y_ranks = np.where(y_ranks>=(y_ranks.shape[0]-np.isnan(_y_arr).sum()), np.nan, y_ranks)
            x_ranks = _x_arr.argsort(kind='mergesort').argsort(kind='mergesort')
            x_ranks = np.where(x_ranks>=(x_ranks.shape[0]-np.isnan(_x_arr).sum()), np.nan, x_ranks)
            # calculate correlation
            inner_result[i-1] = np.corrcoef(y_arr[i-lookback:i], x_arr[i-lookback:i])[0,1]

    return inner_result

jit_module(nopython=True, cache=True)