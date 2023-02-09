import numpy as np

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
        arr = data[i,:]
        ranks = arr.argsort(kind='mergesort').argsort(kind='mergesort')
        ranks = np.where(ranks>=(ranks.shape[0]-np.isnan(arr).sum()), np.nan, ranks)
        result[i,:] = ranks/np.nanmax(ranks)
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
        arr = data[i,:]
        result[i,:] = np.nanpercentile(arr, q=percent)
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
        arr = data[i,:]
        result[i,:] = (arr - np.nanmean(arr))/np.nanstd(arr)
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
        arr = data[i,:]
        high_adjust = np.where(arr>np.nanmean(arr)+sigma*np.nanstd(arr), np.nanmean(arr)+sigma*np.nanstd(arr), 0).copy()
        low_adjust = np.where(arr<np.nanmean(arr)-sigma*np.nanstd(arr), np.nanmean(arr)-sigma*np.nanstd(arr), 0).copy()
        tmp = high_adjust + low_adjust
        result[i,:] = np.where(tmp==0, arr, tmp)
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
        arr = data[i,:]
        available_max = np.nansum(arr) * maxPercent
        result[i,:] = np.where(arr>available_max, available_max, arr)
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
        arr = data[i,:]
        result[i,:] = np.exp(arr-np.nanmax(arr))/np.nansum(np.exp(arr-np.nanmax(arr)))
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
        arr = data[i]
        num_nan = np.sum(np.isnan(arr))
        if num_nan == len(arr):
            pass
        else:
            tmp = arr.argsort(kind="mergesort")
            if n < num_nan:
                tmp = tmp[:-num_nan]
            tmp = arr[tmp]
            nth = tmp[-n]
            result[i] = np.where(arr>=nth, satify, otherwise)
        # tqdm update
        pba.update.remote(1) 

    return result

def cs_apply(pba, start, end, data, *args):
    """Apply user-defined function.

    Args:
        data (2d-np.array): input data
        func (function): function to apply
        *args (): arguments for user-defined function
        **kargs (): deliver some function settings

    Returns:
        2d-np.array
    """
    data = data[0]
    func = args[0][0]
    inner_args = args[0][1]
    print(args)

    result = np.zeros_like(data)
    result[start:end,:] = np.nan

    for i in range(start, end):
        arr = data[i,:]
        result[i,:] = func(arr, *inner_args)
        # tqdm update
        pba.update.remote(1) 

    return result

def cs_dual_apply(pba, start, end, data, *args):
    """Apply user-defined function with two data arguments.
    
    Args:
        y_mat (2d-np.array): input data1
        x_mat (2d-np.array): input data2
        func (function): function to apply
        *args (): arguments for user-defined function

    Returns:
        2d-np.array
    """
    y_mat = data[0]
    x_mat = data[1]

    func = args[0][0]
    inner_args = args[0][1]

    result = np.zeros_like(y_mat)
    result[start:end,:] = np.nan

    for i in range(start, end):
        y_arr = y_mat[i,:]
        x_arr = x_mat[i,:]
        result[i,:] = func(y_arr, x_arr, *inner_args)
        # tqdm update
        pba.update.remote(1) 

    return result

def cs_multi_apply(pba, start, end, data, *args):
    """Apply user-defined function with two data arguments.
    
    Args:
        y_mat (2d-np.array): input data1
        x_mat (2d-np.array): input data2
        func (function): function to apply
        *args (): arguments for user-defined function

    Returns:
        2d-np.array
    """
    func = args[0][0]
    inner_args = list(args[0][1])

    result = np.zeros_like(data[0])
    result[start:end,:] = np.nan

    for i in range(start, end):
        data_args_arr = []
        for each_data in data:
            data_args_arr.append(each_data[i,:])
        result_args = tuple(data_args_arr + inner_args)
        result[i,:] = func(*result_args)
        # tqdm update
        pba.update.remote(1) 

    return result