import numpy as np
from numba import jit_module

def cs_rank_inner(arr):
    """Rank.

    Args:
        progress_proxy (numba-progress obj): used for tqdm progressbar
        data (2d-np.array): input data.

    Returns:
        2d-np.array.
    """
    ranks = arr.argsort(kind='mergesort').argsort(kind='mergesort')
    ranks = np.where(ranks>=(ranks.shape[0]-np.isnan(arr).sum()), np.nan, ranks)
    result = ranks/np.nanmax(ranks)
    
    return result

def cs_percentile_inner(arr, percent):
    """Percentile.

    Args:
        progress_proxy (numba-progress obj): used for tqdm progressbar
        data (2d-np.array): input data
        percent (float): criterion percent, eg) 0.2 for 20%

    Returns:
        2d-np.array
    """
    result = np.nanpercentile(arr, q=percent)

    return result

def cs_zscore_inner(arr):
    """Zscore.
    
    Args:
        progress_proxy (numba-progress obj): used for tqdm progressbar
        data (2d-np.array): input data.
    
    Returns:
        2d-np.array.
    """
    result = (arr - np.nanmean(arr))/np.nanstd(arr)

    return result

def cs_winsorize_inner(arr, sigma):
    """Winsorize.

    Args:
        progress_proxy (numba-progress obj): used for tqdm progressbar
        data (2d-np.array): input data
        sigma (int/float): hurdle sigma rate, data which higher/lower than mean +- sigma * standard_deiviation are winsorized

    Returns:
        2d-np.array with winsorized data
    """
    high_adjust = np.where(arr>np.nanmean(arr)+sigma*np.nanstd(arr), np.nanmean(arr)+sigma*np.nanstd(arr), 0).copy()
    low_adjust = np.where(arr<np.nanmean(arr)-sigma*np.nanstd(arr), np.nanmean(arr)-sigma*np.nanstd(arr), 0).copy()
    tmp = high_adjust + low_adjust
    result = np.where(tmp==0, arr, tmp)

    return result

def cs_truncate_inner(arr, maxPercent):
    """Truncate.

    Args:
        progress_proxy (numba-progress obj): used for tqdm progressbar
        data (2d-np.array): input data
        maxPercent (int/float): truncate hurdle, truncate if data is larger than the hurdle
    
    Returns:
        2d-np.array
    """
    available_max = np.nansum(arr) * maxPercent
    result = np.where(arr>available_max, available_max, arr)

    return result

def cs_softmax_inner(arr):
    """Softmax.
    
    Args:
        progress_proxy (numba-progress obj): used for tqdm progressbar
        data (2d-np.array): input data.
    
    Returns:
        2d-np.array

    Note:
        softmax function f(x) = np.exp(x-np.nanmax(x))/np.nansum(np.exp(x-np.nanmax(x)))
    """
    result = np.exp(arr-np.nanmax(arr))/np.nansum(np.exp(arr-np.nanmax(arr)))

    return result

def cs_top_inner(arr, n, satify, otherwise):
    """Top.

    Args:
        progress_proxy (numba-progress obj): used for tqdm progressbar
        data (2d-np.array): input data

    Returns:
        2d-np.array
    """
    num_nan = np.sum(np.isnan(arr))
    tmp = arr.argsort(kind="mergesort")
    if n < num_nan:
        tmp = tmp[:-num_nan]
    tmp = arr[tmp]
    nth = tmp[-n]
    result = np.where(arr>=nth, satify, otherwise)

    return result

jit_module(nopython=True, cache=True)