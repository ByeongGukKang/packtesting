
import pandas as pd
import numpy as np
from tqdm import tqdm

from packtest import tools
from packtest.execution import cal_pf_value, bid_ask_auto_defer

class Packtesting:

    def __init__(self, init_cash, bid_price, ask_price=None, bid_size=None, ask_size=None, name=""):
        self.__data = {} # dictionary
        self.__variable = {} # dictionary

        self.__base_index = bid_price.index # list
        self.__base_columns = bid_price.columns # list 
        self.__bid_price = bid_price.values # # array

        if type(ask_price) == type(None):
            self.__ask_price = bid_price.values # array
        else:
            self.__ask_price = tools.put_data(ask_price, self.__base_index, self.__base_columns) # array
        if type(bid_size) == type(None):
            self.__bid_size = np.zeros_like(self.__bid_price) + np.inf # array
        else:
            self.__bid_size = tools.put_data(bid_size, self.__base_index, self.__base_columns) # array
        if type(ask_size) == type(None):
            self.__ask_size = np.zeros_like(self.__bid_price) + np.inf # array
        else:
            self.__ask_size = tools.put_data(ask_size, self.__base_index, self.__base_columns) # array

        self.__cash = np.zeros(len(self.__bid_price)) # list
        self.__cash[0] = init_cash
        self.__pf_value = np.zeros(len(self.__bid_price)) # list

        self.__order = np.zeros_like(self.__bid_price) # array
        self.__order_adjusted = np.zeros_like(self.__bid_price) # array
        self.__position = np.zeros_like(self.__bid_price) # array
        self.__cashflow = np.zeros_like(self.__bid_price) # array

        self.name = name

    @property
    def _time_index(self):
        return self.__base_index
    
    @property
    def _security_columns(self):
        return self.__base_columns
    
    @property
    def _ask_price(self):
        return self.__ask_price

    @property
    def _bid_size(self):
        return self.__bid_size
    
    @property
    def _ask_size(self):
        return self.__ask_size
    
    @property
    def _cash(self):
        return self.__cash
    
    @property
    def _pf_value(self):
        return self.__pf_value
    
    @property
    def _order(self):
        return self.__order
    
    @property
    def _adjusted_order(self):
        return self.__order_adjusted

    @property
    def _position(self):
        return self.__position
    
    @property
    def _cashflow(self):
        return self.__cashflow 

    def post_data(self, key, value):
        self.__data[key] = tools.put_data(value, self.__base_index, self.__base_columns)
    
    ### For Data Packet ###
    def pack_get_now(self, current_time):
        return self._time_index[current_time]

    def pack_get_data_rolling(self, current_time, name, win_size=0):
        return self.__data[name][current_time-win_size+1:current_time+1]

    def pack_get_data_expanding(self, current_time, name):
        return self.__data[name][:current_time+1]
    
    def pack_get_variable(self, name):
        return self.__variable[name]
    
    def pack_post_variable(self, name, value):
        self.__variable[name] = value

    def pack_get_account_rolling(self, current_time, name, win_size=0):
        return getattr(self, "_"+name)[current_time-win_size+1:current_time+1]
    
    def pack_get_account_expanding(self, current_time, name):
        return getattr(self, "_"+name)[:current_time+1]

    ### User - Defined Methods ###
    def create_packet(
        self,
        current_time
    ):
        return None

    # @staticmethod
    def create_signal(
        packet
    ):
        return None

    @staticmethod
    def execution(*args):
        return bid_ask_auto_defer(*args)
    
    ### Backtesting Engine ###
    def run(
        self,
    ):
        signal = np.zeros_like(self.__order[0])
        zero_order = np.zeros_like(self.__order[0])
        tmp_order_adjust = np.zeros_like(self.__order[0])
        for time in tqdm(range(len(self.__base_index)), desc=self.name):
        # for time in range(1,200):    
            self.__order[time] = signal + tmp_order_adjust # 주문 = 직전 signal + 저번 execution에서 못하고 밀렸던 것들

            if np.array_equal(zero_order, self.__order[time]): # 주문이 없을 경우 Execution skip
                self.__cash[time] = self.__cash[time-1]
                self.__position[time] = self.__position[time-1]
            else:
                self.__cash[time], tmp_order_adjust, self.__order_adjusted[time], self.__position[time], self.__cashflow[time] = self.execution(
                                                                                                                                        self.__bid_price[time], 
                                                                                                                                        self.__ask_price[time],
                                                                                                                                        self.__bid_size[time], 
                                                                                                                                        self.__ask_size[time],
                                                                                                                                        self.__cash[time-1], 
                                                                                                                                        self.__order[time],
                                                                                                                                        self.__position[time-1]
                                                                                                                                        )  # Execution 먼저
                                                                                                                                    
            self.__pf_value[time] = cal_pf_value(self.__cash[time], self.__position[time], self.__bid_price[time], self.__ask_price[time]) # pf_value 계산

            packet = self.create_packet(time) # 뒤에 보내줄 데이터 패킷 생성

            signal = self.create_signal(packet) # 다음 Signal 생성

        print("Done")

    @property
    def result(self):
        res = {}
        res["cash"] = pd.DataFrame(self.__cash, self.__base_index, ["cash"])
        res["cashflow"] = pd.DataFrame(self.__cashflow, self.__base_index, self.__base_columns)
        res["order"] = pd.DataFrame(self.__order, self.__base_index, self.__base_columns)
        res["order_adjusted"] = pd.DataFrame(self.__order_adjusted, self.__base_index, self.__base_columns)
        res["pf_value"] = pd.DataFrame(self.__pf_value, self.__base_index, ["pf_value"])
        res["position"] = pd.DataFrame(self.__position, self.__base_index, self.__base_columns)
        
        return res
        
    def performance(self):
        pass