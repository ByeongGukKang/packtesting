import numpy as np

from . import core_cs_numba_inner

def cs_rank(pba, start, end, data, *args):
    """Rank.

    Args:
        arr (2d-np.array): input data
        *args (): delivers function settings

    Returns:
        2d-np.array.
    """
    data = data[0]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan

    for i in range(start, end):
        result[i,:] = core_cs_numba_inner.cs_rank_inner(data[i,:])
        # tqdm update
        pba.update.remote(1)    

    return result

def cs_percentile(pba, start, end, data, *args):
    """Percentile.

    Args:
        data (2d-np.array): input data
        *args (): delivers function settings

    Returns:
        2d-np.array
    """
    data = data[0]
    percent = args[0][0]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan

    for i in range(start, end):
        result[i,:] = core_cs_numba_inner.cs_percentile_inner(data[i,:], percent)
        # tqdm update
        pba.update.remote(1) 

    return result

def cs_zscore(pba, start, end, data, *args):
    """Zscore.
    
    Args:
        data (2d-np.array): input data
        *args (): delivers function settings
    
    Returns:
        2d-np.array.
    """
    data = data[0]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan

    for i in range(start, end):
        result[i,:] = core_cs_numba_inner.cs_zscore_inner(data[i,:])
        # tqdm update
        pba.update.remote(1) 

    return result

def cs_winsorize(pba, start, end, data, *args):
    """Winsorize.

    Args:
        data (2d-np.array): input data
        *args (): delivers function settings

    Returns:
        2d-np.array with winsorized data.
    """
    data = data[0]
    sigma = args[0][0]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan

    for i in range(start, end):
        result[i,:] = core_cs_numba_inner.cs_winsorize_inner(data[i,:], sigma)
        # tqdm update
        pba.update.remote(1) 

    return result

def cs_truncate(pba, start, end, data, *args):
    """Truncate.

    Args:
        data (2d-np.array): input data
        maxPercent (int/float): truncate hurdle, truncate if data is larger than the hurdle
        **kargs (): delivers function settings
    
    Returns:
        2d-np.array.
    """
    data = data[0]
    maxPercent = args[0][0]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan

    for i in range(start, end):
        result[i,:] = core_cs_numba_inner.cs_truncate_inner(data[i,:], maxPercent)
        # tqdm update
        pba.update.remote(1) 

    return result

def cs_softmax(pba, start, end, data, *args):
    """Softmax.
    
    Args:
        data (2d-np.array): input data
        *args (): delivers function settings
    
    Returns:
        2d-np.array

    Note:
        softmax function f(x) = np.exp(x-np.nanmax(x))/np.nansum(np.exp(x-np.nanmax(x)))
    """
    data = data[0]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan

    for i in range(start, end):
        result[i,:] = core_cs_numba_inner.cs_softmax_inner(data[i,:])
        # tqdm update
        pba.update.remote(1)  

    return result

def cs_top(pba, start, end, data, *args):
    """Top.

    Args:
        data (2d-np.array): input data
        **kargs (): delivers function settings

    Returns:
        2d-np.array
    """
    data = data[0]
    n = args[0][0]
    satify = args[0][1]
    otherwise = args[0][2]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan

    for i in range(start, end):
        result[i,:] = core_cs_numba_inner.cs_top_inner(data[i,:], n, satify, otherwise)
        # tqdm update
        pba.update.remote(1)  

    return result