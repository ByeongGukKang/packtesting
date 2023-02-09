
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
