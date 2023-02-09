
import numpy as np

def ma(pba, start, end, data, *args):
    """Inner function to calulate rank.

    Args:
        data (2d-np.array): array, length is window period
        window (int): window period
        *args (tuple): delivers function settings
    
    Returns:
        2d-np.array
    """
    data = data[0]
    window = args[0][0]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        arr = data[j]
        for i in range(window, number_of_days):
            _arr = arr[i-window:i].copy()
            if np.sum(np.isnan(_arr)) == window:
                continue
            else:
                result[j,i-1] = np.nanmean(_arr)
        # tqdm update
        pba.update.remote(1)

    return result

def vama(pba, start, end, data, *args):
    """Inner function to calulate rank.

    Args:
        data (2d-np.array): array, length is window period
        window (int): window period
        *args (tuple): delivers function settings
    
    Returns:
        2d-np.array
    """
    data = data[0]
    volume = data[1]
    window = args[0][0]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        arr = data[j]
        arr_vol = volume[j]
        for i in range(window, number_of_days):
            _arr = arr[i-window:i].copy()
            _arr_vol = arr_vol[i-window:i].copy()
            if np.sum(np.isnan(_arr)) == window:
                continue
            elif np.sum(np.isnan(_arr_vol)) == window:
                continue
            else:
                result[j,i-1] = np.nansum(_arr * _arr_vol) / np.nansum(_arr_vol)
        # tqdm update
        pba.update.remote(1)

    return result

def imkkh(pba, start, end, data, *args):
    """Inner function to calulate rank.
    """
    high = data[0]
    low = data[1]
    close = data[2]

    tenkan_window = args[0][0]
    kijun_window = args[0][1]
    senkou2_window = args[0][2]
    chikou_window = args[0][3]
    window = np.max(tenkan_window, kijun_window, senkou2_window, chikou_window)

    result = np.zeros_like(close)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    result = {"tenkan":result.copy(), "kijun":result.copy(), "senkou2":result.copy(), "chikou":result.copy()}

    for j in range(start, end):
        arr_high = high[j]
        arr_low = low[j]
        arr_close = close[j]
        for i in range(window, number_of_days):
            _arr_high = arr_high[i-window:i].copy()
            _arr_low = arr_low[i-window:i].copy()
            _arr_close = arr_close[i-window:i].copy()
            if np.sum(np.isnan(_arr_high)) == window:
                continue
            elif np.sum(np.isnan(_arr_low)) == window:
                continue
            elif np.sum(np.isnan(_arr_close)) == window:
                continue
            else:
                tenkan_value = (np.nanmax(_arr_high[i-tenkan_window:i]) + np.nanmin(_arr_low[i-tenkan_window:i])) / 2 
                kijun_value = (np.nanmax(_arr_high[i-kijun_window:i]) + np.nanmin(_arr_low[i-kijun_window:i])) / 2 
                result["tenkan"][j,i-1] = tenkan_value
                result["kijun"][j,i-1] = kijun_value
                result["senkou1"][j,i-1] = (tenkan_value + kijun_value) / 2
                result["senkou2"][j,i-1] = (np.nanmax(_arr_high[i-senkou2_window:i]) + np.nanmin(_arr_low[i-senkou2_window:i])) / 2 
                result["chikou"][j,i-1] = _arr_close[-chikou_window]
        # tqdm update
        pba.update.remote(1)

    return result

def bollinger(pba, start, end, data, *args):
    """Inner function to calulate rank.
    """
    data = data[0]
    window = args[0][0]
    sigma = args[0][1]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    result = {"upper":result.copy(), "lower":result.copy()}

    for j in range(start, end):
        arr = data[j]
        for i in range(window, number_of_days):
            _arr = arr[i-window:i].copy()
            if np.sum(np.isnan(_arr)) == window:
                continue
            else:
                ma_value = np.nanmean(_arr)
                std_value = np.nanstd(_arr)
                result["upper"][j,i-1] = ma_value + sigma*std_value
                result["lower"][j,i-1] = ma_value - sigma*std_value
        # tqdm update
        pba.update.remote(1)

    return result

def cftpp(pba, start, end, data, *args):
    """Inner function to calulate rank.
    """
    high = data[0]
    low = data[1]
    close = data[2]
    window = 1

    result = np.zeros_like(close)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    result_dict = {"pp":result.copy(), "r1":result.copy(), "s1":result.copy(), "r2":result.copy(), "s2":result.copy()}

    for j in range(start, end):
        arr_high = high[j]
        arr_low = low[j]
        arr_close = close[j]
        for i in range(window, number_of_days):
            _arr_high = arr_high[i-window:i].copy()
            _arr_low = arr_low[i-window:i].copy()
            _arr_close = arr_close[i-window:i].copy()
            if np.sum(np.isnan(_arr_high)) == window:
                continue
            elif np.sum(np.isnan(_arr_low)) == window:
                continue
            elif np.sum(np.isnan(_arr_close)) == window:
                continue
            else:
                pp_value = (_arr_high[-1] + _arr_low[-1] + _arr_close[-1]) / 3
                result_dict["pp"][j,i-1] = pp_value
                result_dict["r1"][j,i-1] = 2*pp_value - _arr_low[0]
                result_dict["s1"][j,i-1] = 2*pp_value - _arr_high[0]
                result_dict["r2"][j,i-1] = pp_value + (_arr_high[0] - _arr_low[0])
                result_dict["s2"][j,i-1] = pp_value - (_arr_high[0] - _arr_low[0])
        # tqdm update
        pba.update.remote(1)

    return result_dict

def dema(pba, start, end, data, *args):
    """Inner function to calulate rank.
    """
    data = data[0]
    window = args[0][0]
    multiplier = 2/(1+window)

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        arr = data[j]
        isFirst = True
        for i in range(window, number_of_days):
            _arr = arr[i-window:i].copy()
            if np.sum(np.isnan(_arr)) == window:
                continue
            elif isFirst:
                result[j,i-1] = np.nanmean(_arr)
                isFirst = False
            else:
                ema_before = result[j,i-2]
                ema_current = multiplier*_arr[-1] + (1-multiplier)*ema_before
                result[j,i-1] = 2*ema_current - (multiplier*ema_current + (1-multiplier)*ema_before)

        # tqdm update
        pba.update.remote(1)

    return result

def envelope(pba, start, end, data, *args):
    """Inner function to calulate rank.
    """
    data = data[0]
    window = args[0][0]
    width = args[0][1]

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    result = {"upper":result.copy(), "lower":result.copy()}

    for j in range(start, end):
        arr = data[j]
        for i in range(window, number_of_days):
            _arr = arr[i-window:i].copy()
            if np.sum(np.isnan(_arr)) == window:
                continue
            else:
                ma_value = np.nanmean(_arr)
                result["upper"][j,i-1] = ma_value*(1 + width)
                result["lower"][j,i-1] = ma_value*(1 - width)
        # tqdm update
        pba.update.remote(1)

    return result

def psar(pba, start, end, data, *args):
    """Inner function to calulate rank.
    """
    high = data[0]
    low = data[1]
    af = args[0][0]
    af_value = args[0][0]
    max_af = args[0][1]
    window = 2
    
    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    result = {"psar":result.copy(), "trend":result.copy()}

    for j in range(start, end):
        arr_high = high[j]
        arr_low = low[j]
        isFirst = True
        high_price_trend_list = []
        low_price_trend_list = []
        for i in range(number_of_days):
            _arr_high = arr_high[i-window:i].copy()
            _arr_low = arr_low[i-window:i].copy()
            if np.sum(np.isnan(_arr_high)) == window:
                continue
            elif np.sum(np.isnan(_arr_low)) == window:
                continue
            elif isFirst:
                if _arr_high[0] < _arr_high[-1]:
                    isUpTrend = True
                    result["psar"][j,i-1] = np.nanmin(_arr_low)
                    result["trend"][j,i-1] = 1
                    extreme_point = np.nanmax(_arr_high)
                else:
                    isUpTrend = False
                    result["psar"][j,i-1] = np.nanmax(_arr_high)
                    result["trend"][j,i-1] = -1
                    extreme_point = np.nanmin(_arr_low)
                isFirst = False
            else:
                psar_before = result["psar"][j,i-2]
                psar_current = psar_before + af*(extreme_point - psar_before)
                if isUpTrend:
                    if psar_current > _arr_low[-1]:
                        psar_current = np.nanmax(high_price_trend_list)
                        extreme_point = _arr_low[-1]
                        af_value = af
                        high_price_trend_list = []
                        result["trend"][j,i-1] = -1
                    else:
                        extreme_point = np.nanmax(extreme_point, _arr_high[-1])
                        af_value = np.max(af_value+af, max_af)
                        high_price_trend_list.append(_arr_high[-1])
                        result["trend"][j,i-1] = 1
                else:
                    if psar_current < _arr_high[-1]:
                        psar_current = np.nanmin(low_price_trend_list)
                        extreme_point = _arr_high[-1]
                        af_value = af
                        low_price_trend_list = []
                        result["trend"][j,i-1] = 1
                    else:
                        extreme_point = np.nanmin(extreme_point, _arr_low[-1])
                        af_value = np.max(af_value+af, max_af)
                        low_price_trend_list.append(_arr_low[-1])
                        result["trend"][j,i-1] = -1

                result["psar"][j,i-2] = psar_current
                    
        # tqdm update
        pba.update.remote(1)

    return result

### Additionals ###

def ema(pba, start, end, data, *args):
    """Inner function to calulate rank.
    """
    data = data[0]
    window = args[0][0]
    multiplier = 2/(1+window)

    result = np.zeros_like(data)
    result[start:end,:] = np.nan
    number_of_days = result.shape[1] + 1

    for j in range(start, end):
        arr = data[j]
        isFirst = True
        for i in range(window, number_of_days):
            _arr = arr[i-window:i].copy()
            if np.sum(np.isnan(_arr)) == window:
                continue
            elif isFirst:
                result[j,i-1] = np.nanmean(_arr)
                isFirst = False
            else:
                ema_before = result[j,i-2]
                ema_current = multiplier*_arr[-1] + (1-multiplier)*ema_before
                result[j,i-1] = ema_current
        # tqdm update
        pba.update.remote(1)

    return result
