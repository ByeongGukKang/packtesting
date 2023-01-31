
import numpy as np

def put_data(
    input_data,
    base_index,
    base_columns
):
    if not np.array_equal(input_data.index, base_index):
        raise ValueError("Index of input data are wrong")
    if not np.array_equal(input_data.columns, base_columns):
        raise ValueError("Columns of input data are wrong")
    
    return input_data.values

