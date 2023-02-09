
import pandas as pd

from . utils import __type_check
from . raymaster import RayMaster, RayManager

from . core import ti_pi
from . core import ti_pi_numba

off_numba = False

### Ray Initialization ###
ray = RayManager()
ray._initialize(isWhere='cs')

### Price Indicators ###
def ma(data, window):
    """Moving Average.
    """
    _data = __type_check(data).T

    if off_numba == False:
        worker = RayMaster("Moving_Average", ray.batch, [_data], 0, ti_pi.ma, window)
    # elif off_numba == True:
    #     worker = RayMaster("MA{window}".format(window=window), ray.batch, [_data], 0, ti_pi_numba.ma)
    result = worker.run()
    
    return pd.DataFrame(result.T, index=data.index, columns=data.columns)

def vama(data, volume, window):
    """Volume Adjusted Moving Average.
    """
    _data = __type_check(data).T
    _volume = __type_check(volume).T 

    if off_numba == False:
        worker = RayMaster("Volume_Adjusted_Moving_Average", ray.batch, [_data, _volume], 0, ti_pi.vama, window)
    # elif off_numba == True:
    #     worker = RayMaster("Volume_Adjusted_Moving_Average({window})".format(window=window), ray.batch, [_data, _volume], 0, ti_pi.vama)
    result = worker.run()
    
    return pd.DataFrame(result.T, index=data.index, columns=data.columns)

def imkkh(high, low, close, tenkan_window=9, kijun_window=26, senkou2_window=52, chikou_window=26):
    """Ichi Moku Kin Kou Hyo.
    """
    _high = __type_check(high).T
    _low = __type_check(low).T
    _close = __type_check(close).T 

    if off_numba == False:
        worker = RayMaster("Ichi_Moku_Kin_Kou_Hyo", ray.batch, [_high, _low, _close], 0, ti_pi.imkkh, tenkan_window, kijun_window, senkou2_window, chikou_window)
    # elif off_numba == True:
    #     worker = RayMaster("Volume_Adjusted_Moving_Average({window})".format(window=window), ray.batch, [_data, _volume], 0, ti_pi.vama)
    result = worker.run()
    
    for key in list(result.keys()):
        result[key] = pd.DataFrame(result[key].T, index=close.index, columns=close.columns)

    return result

def bollinger(data, window, sigma=2):
    """Bollinger Band.
    """
    _data = __type_check(data).T

    if off_numba == False:
        worker = RayMaster("Bollinger_Band", ray.batch, [_data], 0, ti_pi.bollinger, window, sigma)
    # elif off_numba == True:
    #     worker = RayMaster("Volume_Adjusted_Moving_Average({window})".format(window=window), ray.batch, [_data, _volume], 0, ti_pi.vama)
    result = worker.run()
    
    for key in list(result.keys()):
        result[key] = pd.DataFrame(result[key].T, index=data.index, columns=data.columns)

    return result

def cftpp(high, low, close):
    """Chicago Floor Traders Pivotal Point.
    """
    _high = __type_check(high).T
    _low = __type_check(low).T
    _close = __type_check(close).T

    if off_numba == False:
        worker = RayMaster("Chicago_Floor_Traders_Pivotal_Point", ray.batch, [_high, _low, _close], 0, ti_pi.cftpp)
    # elif off_numba == True:
    #     worker = RayMaster("Volume_Adjusted_Moving_Average({window})".format(window=window), ray.batch, [_data, _volume], 0, ti_pi.vama)
    result = worker.run()
    
    for key in list(result.keys()):
        result[key] = pd.DataFrame(result[key].T, index=close.index, columns=close.columns)

    return result

def dema(data, window):
    """Double Exponential Moving Average.
    """
    _data = __type_check(data).T

    if off_numba == False:
        worker = RayMaster("Double_Exponential_Moving_Average", ray.batch, [_data], 0, ti_pi.dema, window)
    # elif off_numba == True:
    #     worker = RayMaster("Volume_Adjusted_Moving_Average({window})".format(window=window), ray.batch, [_data, _volume], 0, ti_pi.vama)
    result = worker.run()

    return pd.DataFrame(result.T, index=data.index, columns=data.columns)

def envelope(data, window, width):
    """Envelope.
    """
    _data = __type_check(data).T

    if off_numba == False:
        worker = RayMaster("Envelope", ray.batch, [_data], 0, ti_pi.envelope, window, width)
    # elif off_numba == True:
    #     worker = RayMaster("Volume_Adjusted_Moving_Average({window})".format(window=window), ray.batch, [_data, _volume], 0, ti_pi.vama)
    result = worker.run()

    return pd.DataFrame(result.T, index=data.index, columns=data.columns)


def psar(high, low):
    """Parabolic Stop And Reversal.
    """
    _high = __type_check(high).T
    _low = __type_check(low).T

    if off_numba == False:
        worker = RayMaster("Envelope", ray.batch, [_high, _low], 0, ti_pi.psar)
    # elif off_numba == True:
    #     worker = RayMaster("Volume_Adjusted_Moving_Average({window})".format(window=window), ray.batch, [_data, _volume], 0, ti_pi.vama)
    result = worker.run()

    return pd.DataFrame(result.T, index=high.index, columns=high.columns)

def price_channel():
    """Price Channel.
    """
    pass

def projection_band():
    """Projection Band.
    """
    pass

def t3():
    """T3.
    """
    pass

def tema():
    """Tripple Exponential Moving Average.
    """
    pass

def vidya():
    """Variable Index Dynamic Average.
    """
    pass


### Additionals ###

def ema(data, window):
    """Exponential Moving Average.
    """
    _data = __type_check(data).T

    if off_numba == False:
        worker = RayMaster("Double_Exponential_Moving_Average", ray.batch, [_data], 0, ti_pi.ema, window)
    # elif off_numba == True:
    #     worker = RayMaster("Volume_Adjusted_Moving_Average({window})".format(window=window), ray.batch, [_data, _volume], 0, ti_pi.vama)
    result = worker.run()

    return pd.DataFrame(result.T, index=data.index, columns=data.columns)