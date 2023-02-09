
import pandas as pd

from . raymaster import RayMaster, RayManager
from . import core_ts

off_numba = True

### Ray Initialization ###
ray = RayManager()
ray._initialize(isWhere='ts')

### Global Functions ###
"""
나중에 utils로 따로 빼야할 필요 있음
"""
def __type_check(data):
    """Data type checker.

    Arg:
        data (np.array/pd.DataFrame): whatever user put
    
    Return:
        2-D np.array

    Note:
        Check the data and return as np.array to calculate things
    """
    if "ndarray" in str(type(data)):
        return data
    elif "DataFrame" in str(type(data)):
        return data.values
    else:
        raise ValueError("arg 'data' must be '2d-np.array' or 'pd.DataFrame'")

### Product Functions ###
def rank(data, lookback):
    """Rank.

    Args:
        data (pd.DataFrame): input data
        lookback (int): lookback period

    Returns:
        pd.DataFrame with values 0 ~ 1, size is same as input

    Note:
        Calculate the ranking of the data and divide with the largest rank
    """
    _data = __type_check(data).T

    if off_numba == False:
        worker = RayMaster("ts_rank", ray.batch, [_data], 0, core_ts_numba.ts_rank, lookback)
    elif off_numba == True:
        worker = RayMaster("ts_rank", ray.batch, [_data], 0, core_ts.ts_rank, lookback)
    result = worker.run()
    
    return pd.DataFrame(result.T, index=data.index, columns=data.columns)

def zscore(data, lookback):
    """Zscore.
    
    Args:
        data (pd.DataFrame): input data
        lookback (int): lookback period
    
    Returns:
        pd.DataFrame with zscores
    """
    _data = __type_check(data).T

    if off_numba == False:
        worker = RayMaster("ts_zscore", ray.batch, [_data], 0, core_ts_numba.ts_zscore, lookback)
    elif off_numba == True:
        worker = RayMaster("ts_zscore", ray.batch, [_data], 0, core_ts.ts_zscore, lookback)
    result = worker.run()
    
    return pd.DataFrame(result.T, index=data.index, columns=data.columns)

def winsorize(data, lookback, sigma=4):
    """Winsorize.

    Args:
        data (pd.DataFrame): input data
        lookback (int): lookback period
        sigma (int/float): hurdle sigma rate, data which higher/lower than mean +- sigma * standard_deiviation are winsorized

    Returns:
        pd.DataFrame with winsorized data
    """
    _data = __type_check(data).T

    if off_numba == False:
        worker = RayMaster("ts_winsorize", ray.batch, [_data], 0, core_ts_numba.ts_zscore, lookback, sigma)
    elif off_numba == True:
        worker = RayMaster("ts_winsorize", ray.batch, [_data], 0, core_ts.ts_zscore, lookback, sigma)
    result = worker.run()
    
    return pd.DataFrame(result.T, index=data.index, columns=data.columns)

def truncate(data, lookback, maxPercent):
    """Truncate.

    Args:
        data (pd.DataFrame): input data
        lookback (int): lookback period
        maxPercent (int/float): truncate hurdle, truncate if data is larger than the hurdle
    
    Returns:
        pd.DataFrame with truncated data
    """
    _data = __type_check(data).T

    if off_numba == False:
        worker = RayMaster("ts_truncate", ray.batch, [_data], 0, core_ts_numba.ts_truncate, lookback, maxPercent)
    elif off_numba == True:
        worker = RayMaster("ts_truncate", ray.batch, [_data], 0, core_ts.ts_truncate, lookback, maxPercent)
    result = worker.run()
    
    return pd.DataFrame(result.T, index=data.index, columns=data.columns)

def corr_pearson(y, x, lookback):
    """Pearson's correlation.

    Args:
        y (pd.DataFrame): input dataframe with rows and columns
        x (pd.DataFrame): input data with rows and only one column
        lookback (int): lookback period
    
    Returns:
        pd.DataFrame
    """    
    _y = __type_check(y).T
    _x = __type_check(x).T[0]

    if off_numba == False:
        worker = RayMaster("ts_corr_pearson", ray.batch, [_y, _x], 0, core_ts_numba.ts_corr_pearson, lookback)
    elif off_numba == True:
        worker = RayMaster("ts_corr_pearson", ray.batch, [_y, _x], 0, core_ts.ts_corr_pearson, lookback)
    result = worker.run()
    
    return pd.DataFrame(result.T, index=y.index, columns=y.columns)

def corr_spearman(y, x, lookback):
    """Spearman's rank correlation.

    Args:
        y (pd.DataFrame): input dataframe with rows and columns
        x (pd.DataFrame): input data with rows and only one column
        lookback (int): lookback period
    
    Returns:
        pd.DataFrame
    """    
    _y = __type_check(y).T
    _x = __type_check(x).T[0]

    if off_numba == False:
        worker = RayMaster("ts_corr_spearman", ray.batch, [_y, _x], 0, core_ts_numba.ts_corr_spearman, lookback)
    elif off_numba == True:
        worker = RayMaster("ts_corr_spearman", ray.batch, [_y, _x], 0, core_ts.ts_corr_spearman, lookback)
    result = worker.run()
    
    return pd.DataFrame(result.T, index=y.index, columns=y.columns)

def corr_kendall(y, x, lookback):
    """Kendall's rank correlation.

    Args:
        y (pd.DataFrame): input dataframe with rows and columns
        x (pd.DataFrame): input data with rows and only one column
        lookback (int): lookback period
    
    Returns:
        pd.DataFrame
    """ 
    raise Exception("Currently Developing...")
    _y = __type_check(y).T
    _x = __type_check(x).T[0]

    if off_numba == False:
        worker = RayMasterDual("ts_corr_kendall", ray.batch, _y, _x, 0, core_ts_ray_numba.ts_corr_kendall, lookback)
    elif off_numba == True:
        worker = RayMasterDual("ts_corr_kendall", ray.batch, _y, _x, 0, core_ts_ray.ts_corr_kendall, lookback)
    result = worker.run()
    
    return pd.DataFrame(result.T, index=y.index, columns=y.columns)

def apply(data, lookback, func, *args):
    """Apply user-defined function with one data argument.

    Args:
        data (pd.DataFrame): input data
        lookback (int): lookback period
        func (function): function to apply
        *args : arguments used in the function

    Returns:
        pd.DataFrame

    Note:
        Functions must have a 1-D np.array as an input
        Numpy array methods are availalbe, eg) lambda x: x/x.sum()

    Example:

    """
    _data = __type_check(data).T

    worker = RayMaster("ts_apply", ray.batch, [_data], 0, core_ts.ts_apply, lookback, func, args)
    result = worker.run()

    return pd.DataFrame(result.T, index=data.index, columns=data.columns)

def dual_apply(y, x, lookback, func, *args):
    """Apply user-defined function with two data arguments.
    
    Args:
        y (pd.DataFrame): data to adjust.
        x (pd.DataFrame): data used as an criteria to adjust
        lookback (int): lookback period
        func (function): function to apply
        *args : arguments used in the function

    Returns:
        pd.DataFrame
    
    """
    _y = __type_check(y).T
    _x = __type_check(x).T

    worker = RayMaster("ts_dual_apply", ray.batch, [_y, _x], 0, core_ts.ts_dual_apply, lookback, func, args)
    result = worker.run()

    return pd.DataFrame(result.T, index=y.index, columns=y.columns)

def multi_apply(data, lookback, func, *args):
    """Apply user-defined  function with multiple data arguments.
    
    Args:
        data (list[pd.DataFrame]): list of dataframe
        lookback (int): lookback period
        func (function): function to apply
        *args : arguments used in the function

    Returns:
        pd.DataFrame
    """
    result_idx, result_col = data[0].index, data[0].columns
    if type(data) != list:
        raise ValueError("data must be list of pd.DataFrame or np.array")
    data = [__type_check(each_data).T for each_data in data]
         
    worker = RayMaster("ts_multi_apply", ray.batch, data, 0, core_ts.ts_multi_apply, lookback, func, args)
    result = worker.run()

    return pd.DataFrame(result.T, index=result_idx, columns=result_col)


# def regression(y, x, lookback, returns="slope"):
#     """Correlation between two data.

#     Args:
#         y (pd.DataFrame): data for dependent variable.
#         x (pd.DataFrame): data for independent varialbe.
#         lookback (int): lookback period.
#         returns (str, one in ['slope','constant']): decide which to return.
    
#     Returns:
#         pd.DataFrame

#     Note:
#         Function implements the OLS regression with y = a + bx.
#         You can choose which to return 'a' (costant) or 'b' (slope) 
#     """
#     _y = y.values.copy()
#     _x = x.values.copy()