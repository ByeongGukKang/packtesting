
import numpy as np

def cal_pf_value(current_cash:float, position:list, bid_price:list, ask_price:list):
    long_position_value = np.sum(np.where(position>0, position*bid_price, 0))
    short_position_value = np.sum(np.where(position<0, position*ask_price, 0))
    return current_cash + long_position_value + short_position_value

def bid_ask_auto_defer(bid_price, ask_price, bid_size, ask_size, cash, order, position):

    long_order_adj = np.where(order>0, order, 0)
    long_order_adj = np.where(ask_size-long_order_adj>=0, long_order_adj, ask_size)
    short_order_adj = np.where(order<0, order, 0)
    short_order_adj = np.where(bid_size+short_order_adj>=0, short_order_adj, -bid_size)

    long_order_cashflow = np.where(long_order_adj>0, long_order_adj*ask_price, 0)
    short_order_cashflow = np.where(short_order_adj<0, short_order_adj*bid_price, 0)
    
    res_cashflow = -(np.sum(long_order_cashflow) + np.sum(short_order_cashflow))
    res_cash = cash + np.sum(np.array([res_cashflow]))
    res_order = long_order_adj + short_order_adj
    res_postion = position + res_order
    
    return res_cash, order-res_order, res_order, res_postion, res_cashflow