
import pandas as pd

from . raymaster import RayMaster, RayManager
from . import core_cs
from . import core_cs_numba

off_numba = False

### Ray Initialization ###
ray = RayManager()
ray._initialize(isWhere='cs')

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
def rank(data):
    """Rank.

    Args:
        data (pd.DataFrame): input data

    Returns:
        pd.DataFrame with values 0 ~ 1, size is same as input

    Note:
        Calculate the ranking of the data and divide with the largest rank
    """
    _data = __type_check(data)

    if off_numba == False:
        worker = RayMaster("cs_rank", ray.batch, [_data], 0, core_cs_numba.cs_rank)
    elif off_numba == True:
        worker = RayMaster("cs_rank", ray.batch, [_data], 0, core_cs.cs_rank)
    result = worker.run()
    
    return pd.DataFrame(result, index=data.index, columns=data.columns)

def percentile(data, percent):
    """Percentile.

    Args:
        data (pd.DataFrame): input data
        percent (float): criterion percent, eg) 0.2 for 20%

    Returns:
        pd.DataFrame, same rownumber, one column
    """
    _data = __type_check(data)

    if off_numba == False:
        worker = RayMaster("cs_percentile", ray.batch, [_data], 0, core_cs_numba.cs_percentile, percent)
    elif off_numba == True:
        worker = RayMaster("cs_percentile", ray.batch, [_data], 0, core_cs.cs_percentile, percent)
    result = worker.run()
    
    return pd.DataFrame(result.iloc[0,:], index=data.index, columns=["{per}% percentile".format(per=percent)])

def zscore(data):
    """Zscore.
    
    Args:
        data (pd.DataFrame): input data
    
    Returns:
        pd.DataFrame with zscores
    """
    _data = __type_check(data)

    if off_numba == False:
        worker = RayMaster("cs_zscore", ray.batch, [_data], 0, core_cs_numba.cs_zscore)
    elif off_numba == True:
        worker = RayMaster("cs_zscore", ray.batch, [_data], 0, core_cs.cs_zscore)
    result = worker.run()
    
    return pd.DataFrame(result, index=data.index, columns=data.columns)

def winsorize(data, sigma=4):
    """Winsorize.

    Args:
        data (pd.DataFrame): input data
        sigma (int/float): hurdle sigma rate, data which higher/lower than mean +- sigma * standard_deiviation are winsorized

    Returns:
        pd.DataFrame with winsorized data
    """
    _data = __type_check(data)

    if off_numba == False:
        worker = RayMaster("cs_winsorize", ray.batch, [_data], 0, core_cs_numba.cs_winsorize, sigma)
    elif off_numba == True:
        worker = RayMaster("cs_winsorize", ray.batch, [_data], 0, core_cs.cs_winsorize, sigma)
    result = worker.run()
    
    return pd.DataFrame(result, index=data.index, columns=data.columns)

def truncate(data, maxPercent):
    """Truncate.

    Args:
        data (pd.DataFrame): input data.
        maxPercent (int/float): truncate hurdle, truncate if data is larger than the hurdle.
    
    Returns:
        pd.DataFrame with truncated data.
    """
    _data = __type_check(data)

    if off_numba == False:
        worker = RayMaster("cs_truncate", ray.batch, [_data], 0, core_cs_numba.cs_truncate, maxPercent)
    elif off_numba == True:
        worker = RayMaster("cs_truncate", ray.batch, [_data], 0, core_cs.cs_truncate, maxPercent)
    result = worker.run()
    
    return pd.DataFrame(result, index=data.index, columns=data.columns)

def softmax(data):
    """Softmax.

    Args:
        data (pd.DataFrame): input data

    Returns:
        pd.DataFrame with softmax function applied data
    """
    _data = __type_check(data)

    if off_numba == False:
        worker = RayMaster("cs_softmax", ray.batch, [_data], 0, core_cs_numba.cs_softmax)
    elif off_numba == True:
        worker = RayMaster("cs_softmax", ray.batch, [_data], 0, core_cs.cs_softmax)
    result = worker.run()
    
    return pd.DataFrame(result, index=data.index, columns=data.columns)

def top(data, n, satify=1, otherwise=0):
    """Screen top 'n' assets.

    Args:
        data (pd.DataFrame): input data
        satify (int/float): value to give if an asset is in the top 'n'
        otherwise (int/float): value to give if an asset isn't in the top 'n'

    Returns:
        pd.DataFrame top 'n' elements will be 1(satisfy), otherwise 0(otherwise)
    """
    _data = __type_check(data)

    if off_numba == False:
        worker = RayMaster("cs_top", ray.batch, [_data], 0, core_cs_numba.cs_top, n, satify, otherwise)
    elif off_numba == True:
        worker = RayMaster("cs_top", ray.batch, [_data], 0, core_cs.cs_top, n, satify, otherwise)
    result = worker.run()
    
    return pd.DataFrame(result, index=data.index, columns=data.columns)

def apply(data, func, *args):
    """Apply user-defined function with one data argument.

    Args:
        data (pd.DataFrame): input data
        func (function): function to apply
        *args : arguments used in the function

    Returns:
        pd.DataFrame

    Note:
        Functions must have a 2d-np.array as an input
        Numpy array methods are availalbe, eg) lambda x: x/x.sum()

    Example:

    """
    _data = __type_check(data)

    worker = RayMaster("cs_apply", ray.batch, [_data], 0, core_cs.cs_apply, func, args)
    result = worker.run()

    return pd.DataFrame(result, index=data.index, columns=data.columns)

def dual_apply(y, x, func, *args):
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
    _y = __type_check(y)
    _x = __type_check(x)

    worker = RayMaster("ts_dual_apply", ray.batch, [_y, _x], 0, core_cs.cs_dual_apply, func, args)
    result = worker.run()

    return pd.DataFrame(result, index=y.index, columns=y.columns)

def multi_apply(data, func, *args):
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
    data = [__type_check(each_data) for each_data in data]
         
    worker = RayMaster("ts_multi_apply", ray.batch, data, 0, core_cs.cs_multi_apply, func, args)
    result = worker.run()

    return pd.DataFrame(result, index=result_idx, columns=result_col)

# def regression(y, x, returns="slope"):
#     """Correlation between two data.

#     Args:
#         y (pd.DataFrame): data for dependent variable.
#         x (pd.DataFrame): data for independent varialbe.
#         returns (str, one in ['slope','constant']): decide which to return.
    
#     Returns:
#         pd.DataFrame
    
#     Note:
#         Function implements the OLS regression with y = a + bx.
#         You can choose which to return 'a' (costant) or 'b' (slope) 
#     """
#     _y = __type_check(y)
#     _x = __type_check(x)

    