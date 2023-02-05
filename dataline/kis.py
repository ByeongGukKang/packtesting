
import requests
import json
import time

import pandas as pd
from dateutil.relativedelta import relativedelta

from .tools import DateManager, raise_arg

class KIS:

    def __init__(self, key:str=None, secret:str=None, isMock:bool=False, autoLogin:bool=False, abs_loc:str=""):
        """KIS API Initication
        
        Args:
            key: api key (defalut None)
            secret: secret key (default None)
            isMock: True if account is for mock trading (default False)
            autoLogin: True if autologin is used (default False)
            abs_loc: absolute location of setting jsons, '~~~~\\'kis_setting.json, (default "")
        
        Returns:
            A KIS API instance

        """
        self.abs_loc = abs_loc
        with open(self.abs_loc+'kis_setting.json', 'r', encoding="utf-8") as f:
            self.__setting = json.load(f)
        with open(self.abs_loc+'kis_urls.json', 'r', encoding="utf-8") as f:
            self.__urls = json.load(f)
        with open(self.abs_loc+'kis_trade_id.json', 'r', encoding="utf-8") as f:
            self.__trade_id = json.load(f)

        self.__isMock = isMock

        if self.__isMock:
            self.__setting = self.__setting["mock"]
            self.__base_url = self.__urls["domain"]["mock"]
            self.__trade_id = self.__trade_id["mock"]
        else:
            self.__setting = self.__setting["real"]
            self.__base_url = self.__urls["domain"]["real"]
            self.__trade_id = self.__trade_id["real"]
        self.__urls = self.__urls["url"]

        if autoLogin:
            self.__api_key = self.__setting["api_key"]
            self.__api_secret = self.__setting["api_secret"]
        else:
            self.__api_key = key
            self.__api_secret = secret

        self.__get_auth_token()
        self.sleep_time = 0.05

    def __auto_token_expire(self):
        return (self.__setting["auth_token_expire"] <= time.time())

    def __get_auth_token(self):
        if self.__auto_token_expire():
            self.__headers = {}
            self.__params = {}

            self.__headers = {"content-type":"application/json"}

            self.__params["grant_type"] = "client_credentials"
            self.__params["appkey"] = self.__api_key
            self.__params["appsecret"] = self.__api_secret

            res = requests.post(url="https://openapi.koreainvestment.com:9443/oauth2/tokenP?", headers=self.__headers, data=json.dumps(self.__params))
            res.raise_for_status()

            res = res.json()
            self.__setting["auth_token_value"] = res["access_token"]
            self.__setting["auth_token_type"] = res["token_type"]
            self.__setting["auth_token_expire"] = time.time() + res["expires_in"] - 3600

            with open(self.abs_loc+'kis_setting.json', 'r', encoding="utf-8") as f:
                tmp = json.load(f)
            if self.__isMock:
                tmp["mock"] = self.__setting
            else:
                tmp["real"] = self.__setting
            
            with open(self.abs_loc+"kis_setting.json", "w", encoding="utf-8") as f:
                json.dump(tmp, f, ensure_ascii=False, indent="\t")

    @property
    def isMock(self):
        return self.__isMock

    @property
    def setting(self):
        return self.__setting
    
    @property
    def urls(self):
        return self.__urls

    def __isMock_available(self, key):
        if self.__isMock:
            if key not in self.__trade_id.keys():
                raise Exception("모의투자 지원하지 않음")

    def __clear_headers(self):
        self.__headers = {
            "content": "application/json",
            "authorization":"{} {}".format(self.__setting["auth_token_type"], self.__setting["auth_token_value"]),
            "appkey":self.__setting["api_key"],
            "appsecret":self.__setting["api_secret"],
        }
    
    def __clear_params(self):
        self.__params = {}
    
    def get_kr_stock_price(self, code, start, end, period="D", adjust=False):
        raise_arg("period", period, ["D","W","M","Y"])
        
        self.__clear_headers()
        self.__headers["tr_id"] = self.__trade_id["kr_stock_hist_price"]

        self.__clear_params()
        self.__params["FID_COND_MRKT_DIV_CODE"] = "J"
        self.__params["FID_INPUT_ISCD"] = code
        self.__params["FID_PERIOD_DIV_CODE"] = period
        self.__params["FID_ORG_ADJ_PRC"] = int(adjust)

        windows = DateManager.split(start, end, "%Y%m%d", backward=False, months=4)
        start_date = windows.pop(0)
        for i, end_date in enumerate(windows):
            self.__params["FID_INPUT_DATE_1"] = start_date
            self.__params["FID_INPUT_DATE_2"] = (DateManager.str_to_datetime(end_date, "%Y%m%d") - relativedelta(days=1)).strftime("%Y%m%d")

            time.sleep(self.sleep_time)
            res = requests.get(url=self.__base_url+self.__urls["kr_stock_hist_price"], headers=self.__headers, params=self.__params)
            res.raise_for_status()

            res = res.json()["output2"]

            start_date = end_date

            if i == 0:
                result = pd.DataFrame(res)
            else:
                result = pd.concat([result, pd.DataFrame(res)], axis=0)

        return result.sort_values(by=["stck_bsop_date"]).dropna(axis=0)
    
    def get_kr_daily_minute(self, code):
        self.__clear_headers()
        self.__headers["tr_id"] = self.__trade_id["kr_stock_daily_minute_price"]

        self.__clear_params()
        self.__params["FID_ETC_CLS_CODE"] = ""
        self.__params["FID_COND_MRKT_DIV_CODE"] = "J"
        self.__params["FID_INPUT_ISCD"] = code
        self.__params["FID_PW_DATA_INCU_YN"] = "N"

        windows = DateManager.split("09:00:00", "16:00:00", "%H:%M:%S", backward=False, minutes=30)
        for i, end_date in enumerate(windows):
            self.__params["FID_INPUT_HOUR_1"] = DateManager.str_to_datetime(end_date, "%H:%M:%S").strftime("%H%M%S")

            time.sleep(self.sleep_time)
            res = requests.get(url=self.__base_url+self.__urls["kr_stock_daily_minute_price"], headers=self.__headers, params=self.__params)
            res.raise_for_status()

            res = res.json()["output2"]
            
            if i == 0:
                result = pd.DataFrame(res)
            else:
                result = pd.concat([result, pd.DataFrame(res)], axis=0)

        return result.sort_values(by=["stck_cntg_hour"]).dropna(axis=0)

    def get_kr_derivative_price(self, code, start, end, period="D", div_code="F"):
        raise_arg("period", period, ["D","W","M","Y"])
        raise_arg("div_code", div_code, ["F","O","JF","JO","CF","CM","EU"])
        
        self.__clear_headers()
        self.__headers["tr_id"] = self.__trade_id["kr_derivative_hist_price"]

        self.__clear_params()
        self.__params["FID_COND_MRKT_DIV_CODE"] = div_code
        self.__params["FID_INPUT_ISCD"] = code
        self.__params["FID_PERIOD_DIV_CODE"] = period

        windows = DateManager.split(start, end, "%Y%m%d", backward=False, months=4)
        start_date = windows.pop(0)
        for i, end_date in enumerate(windows):
            self.__params["FID_INPUT_DATE_1"] = start_date
            self.__params["FID_INPUT_DATE_2"] = (DateManager.str_to_datetime(end_date, "%Y%m%d") - relativedelta(days=1)).strftime("%Y%m%d")

            time.sleep(self.sleep_time)
            res = requests.get(url=self.__base_url+self.__urls["kr_stock_hist_price"], headers=self.__headers, params=self.__params)
            res.raise_for_status()

            res = res.json()["output2"]

            start_date = end_date

            if i == 0:
                result = pd.DataFrame(res)
            else:
                result = pd.concat([result, pd.DataFrame(res)], axis=0)

        return result.sort_values(by=["stck_bsop_date"]).dropna(axis=0)