import os
import time

import pandas as pd
import win32com.client

class Creon:

    def __init__(self):
        self.__obj = win32com.client.Dispatch("CpUtil.CpCybos")

        self.stock_chart_fields = {
                                0:"날짜", 1:"시간", 2:"시가", 3:"고가", 4:"저가", 5:"종가", 6:"전일대비", 8:"거래량", 9:"거래대금", 10:"누적체결매도수량", 11:"누적체결매수수량",
                                12:"상장주식수", 13:"시가총액", 14:"외국인주문한도수량", 15:"외국인주문가능수량", 16:"외국인현보유수량", 17:"외국인현보유비율", 18:"수정주가일자", 19:"수정주가비율",
                                20:"기관순매수", 21:"기관누적순매수", 22:"등락주선", 23:"등락비율", 24:"예탁금", 25:"주식회전율", 26:"거래성립률", 37:"대비부호", 62:"누적체결매도수량", 63:"누적체결매수수량"
        }

        self.sleep_time = 0.25

    def login(self, loc, id, id_pass, cert_pass):
        if not self.isConected():
            try:
                os.chdir(loc)
                os.system(f"coStarter.exe /prj:cp /id:{id} /pwd:{id_pass} /pwdcert:{cert_pass} /autostart")
            except:
                raise Exception("Login failed")
        
        if not self.isConected():
            raise Exception("Login Failed")
    
    def close(self):
        self.obj.PlusDisconnect()
    
    def isConected(self):
        if self.obj.IsConnect == 0:
            return False
        elif self.obj.IsConnect == 1:
            return True
        else:
            raise Exception()
    
    @property
    def obj(self):
        return self.__obj

    def get_stock_chart(
        self,
        code,
        start,
        end,
        fields,
        period,
        by_count=False,
        count=500,
        adj_gap=False,
        adj_prc=False,
        vol_type="3"
    ):
        """Get OHLCV of (stock,sector,ELW) and related infos

        Args:
            code (str): 주식(A003540), 업종(U001), ELW(J517016)의 종목코드
            start (Y%m%d): YYYYMMDD형식으로 데이터의 마지막(가장최근) 날짜
            end (%Y%m%d): YYYYMMDD형식으로 데이터의 시작(가장오래된) 날짜
            field (list[int]):{0:"날짜", 1:"시간", 2:"시가", 3:"고가", 4:"저가", 5:"종가", 6:"전일대비", 8:"거래량", 9:"거래대금", 10:"누적체결매도수량", 11:"누적체결매수수량", (10,11은 분틱에서만 제공)
                              12:"상장주식수", 13:"시가총액", 14:"외국인주문한도수량", 15:"외국인주문가능수량", 16:"외국인현보유수량", 17:"외국인현보유비율", 18:"수정주가일자", 19:"수정주가비율",
                              20:"기관순매수", 21:"기관누적순매수", 22:"등락주선", 23:"등락비율", 24:"예탁금", 25:"주식회전율", 26:"거래성립률", 37:"대비부호", 62:"누적체결매도수량", 63:"누적체결매수수량" (62,63은 분틱에서만 제공)}
            period (str): "D":일, "W": 주, "M":월, "m":분, "T":틱
            by_count (bool): False:기간 요청시  주,월,분,틱은 불가, True:갯수로 요청이고 분,틱 모드인 경우에는 요청 갯수 및 수신 갯수를 누적해서 다음 데이터 요청을 체크해야 함.( 요청 갯수 <= 수신 갯수 비교하는 로직 추가) 
            count (int): 요청할데이터의개수(by_count가 True에만 작동)
            adj_gap (bool): 갭보정여부
            adj_prc (bool): 수정주가여부
            vol_type (str): "1":시간외거래량모두포함, "2":장종료시간외거래량만포함, "3":시간외거래량모두제외, "4":장전시간외거래량만포함
        """
        
        objStockChart = win32com.client.Dispatch("CpSysDib.StockChart")
 
        objStockChart.SetInputValue(0, code)
        objStockChart.SetInputValue(1, ord(str(int(by_count)+1)))
        objStockChart.SetInputValue(2, start)
        objStockChart.SetInputValue(3, end)
        if by_count:
            objStockChart.SetInputValue(4, count)
        objStockChart.SetInputValue(5, fields) 
        objStockChart.SetInputValue(6, ord(period))
        objStockChart.SetInputValue(8, ord(str(int(adj_gap))))
        objStockChart.SetInputValue(9, ord(str(int(adj_prc)))) 
        objStockChart.SetInputValue(10, ord(vol_type)) 

        time.sleep(self.sleep_time)
        objStockChart.BlockRequest()

        result_dict = {}
        data_length = objStockChart.GetHeaderValue(3)
        for i, field in enumerate(fields):
            tmp_list = []
            for j in range(data_length):
                tmp_list.append(objStockChart.GetDataValue(i, j))
            result_dict[self.stock_chart_fields[field]] = tmp_list
        
        return pd.DataFrame(result_dict)