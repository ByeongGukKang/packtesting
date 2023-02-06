
import requests
import json
import time

import pandas as pd

from .tools import raise_arg

class KRX_CRAWL:

    def __init__(self):
        self.__base_url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        self.sleep_time = 0.75

    def __clear_headers(self):
        self.__headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ko-KR,ko;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            "Cookie": "",
            'Host': 'data.krx.co.kr',
            'Origin': 'http://data.krx.co.kr',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        #'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201'

    def __clear_params(self):
        self.__params = {}

    def __return_part(self):
        time.sleep(self.sleep_time)
        res = requests.get(url=self.__base_url, headers=self.__headers, params=self.__params)
        res.raise_for_status()

        res = res.json()["output"]

        return pd.DataFrame(res)
    
    def get_kr_derivatives_date_price(self, date, underlying="코스피200F"):
        self.__clear_headers()
        self.__headers["Referer"] = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201"

        self.__clear_params()
        self.__params["bld"] = "dbms/MDC/STAT/standard/MDCSTAT12501"
        self.__params["locale"] = "ko_KR"
        self.__params["trdDd"] = date
        raise_arg(
            "urderlying", 
            underlying, 
            ["코스피200F","미니코스피200F","코스피200O","코스피200위클리O","미니코스피200O","코스닥150F","코스닥150O","KRX300F","변동성지수F","섹터지수F",
            "3년국채F","5년국채F","10년국채F","3개월무위험금리F","미국달러F","달러플렉스F","엔F","유로F","위안F","금F",
            "주식F","주식O","유로스톡스50F"]
        )
        prodId_dict = {"코스피200F":"KRDRVFUK2I", "미니코스피200F":"KRDRVFUMKI","코스피200O":"KRDRVOPK2I","코스피200위클리O":"KRDRVFUMKI","미니코스피200O":"KRDRVOPMKI",
            "코스닥150F":"KRDRVFUKQI","코스닥150O":"KRDRVOPKQI","KRX300F":"KRDRVFUXI3","변동성지수F":"KRDRVFUVKI","섹터지수F":"KRDRVFUXAT",
            "3년국채F":"KRDRVFUBM3","5년국채F":"KRDRVFUBM5","10년국채F":"KRDRVFUBMA","3개월무위험금리F":"KRDRVFURFR","미국달러F":"KRDRVFUUSD","달러플렉스F":"KRDRVFXUSD",
            "엔F":"KRDRVFUJPY","유로F":"KRDRVFUEUR","위안F":"KRDRVFUCNH","금F":"KRDRVFUKGD",
            "주식F":"KRDRVFUEQU","주식O":"KRDRVOPEQU","유로스톡스50F":"KRDRVFUEST"
        }
        self.__params["prodId"] = prodId_dict[underlying]
        self.__params["trdDdBox1"] = date
        self.__params["trdDdBox2"] = date
        self.__params["mktTpCd"] = "T"
        self.__params["rghtTpCd"] = "T"
        self.__params["share"] = "1"
        self.__params["money"] = "1"
        self.__params["csvxls_isNo"] = "false"

        return self.__return_part()
    
    def get_kr_futures_recent_price(self, start, end, underlying="코스피200F"):
        self.__clear_headers()
        self.__headers["Referer"] = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201"

        self.__clear_params()
        self.__params["bld"] = "dbms/MDC/STAT/standard/MDCSTAT12701"
        self.__params["locale"] = "ko_KR"
        raise_arg(
            "urderlying", 
            underlying, 
            ["코스피200F","미니코스피200F","코스닥150F","KRX300F","변동성지수F","섹터지수F",
            "3년국채F","5년국채F","10년국채F","3개월무위험금리F","미국달러F","달러플렉스F","엔F","유로F","위안F","금F",
            "주식F","유로스톡스50F"]
        )
        prodId_dict = {"코스피200F":"KRDRVFUK2I", "미니코스피200F":"KRDRVFUMKI",
            "코스닥150F":"KRDRVFUKQI","KRX300F":"KRDRVFUXI3","변동성지수F":"KRDRVFUVKI","섹터지수F":"KRDRVFUXAT",
            "3년국채F":"KRDRVFUBM3","5년국채F":"KRDRVFUBM5","10년국채F":"KRDRVFUBMA","3개월무위험금리F":"KRDRVFURFR","미국달러F":"KRDRVFUUSD","달러플렉스F":"KRDRVFXUSD",
            "엔F":"KRDRVFUJPY","유로F":"KRDRVFUEUR","위안F":"KRDRVFUCNH","금F":"KRDRVFUKGD",
            "주식F":"KRDRVFUEQU","유로스톡스50F":"KRDRVFUEST"
        }
        self.__params["prodId"] = prodId_dict[underlying]
        if underlying == "섹터지수F":
            self.__params["subProdId"] = "KRDRVFUXAT"
        elif underlying == "주식F":
            self.__params["subProdId"] = "KRDRVFUEQU"
        self.__params["strtDd"] = start
        self.__params["endDd"] = end
        self.__params["mktTpCd"] = "T"
        self.__params["share"] = "1"
        self.__params["money"] = "1"
        self.__params["csvxls_isNo"] = "false"

        return self.__return_part()
    
    def get_kr_listed_date(self, date, index="코스피"):
        self.__clear_headers()
        self.__headers['Refer'] = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201"

        self.__clear_params()
        self.__params["bld"] = "dbms/MDC/STAT/standard/MDCSTAT00601"
        self.__params["locale"] = "ko_KR"
        raise_arg(
            "index", 
            index, 
            ["코스피","코스닥","코스피200","코스닥150"]
        )
        self.__params["tboxindIdx_finder_equidx0_28"] = index
        indIdx_dict = {"코스피":"1","코스닥":"2","코스피200":"1","코스닥150":"2"}
        self.__params["indIdx"] = indIdx_dict[index]
        indIdx2_dict = {"코스피":"001","코스닥":"001","코스피200":"028","코스닥150":"203"}
        self.__params["indIdx2"] = indIdx2_dict[index]
        self.__params["codeNmindIdx_finder_equidx0_28"] = index
        self.__params["param1indIdx_finder_equidx0_28"] = ""
        self.__params["trdDd"] = date
        self.__params["money"] = "1"
        self.__params["csvxls_isNo"] = "false"

        return self.__return_part()

class KRX_API:

    def __init__(self, key, abs_loc=""):
        self.abs_loc = abs_loc
        self.__base_url = "http://data-dbg.krx.co.kr/svc/apis"
        if type(key) == type(None):
            with open(abs_loc+'krx_setting.json', 'r', encoding="utf-8") as f:
                self.__key = json.load(f)["key"]
        else:
            self.__key = key
        with open(abs_loc+'krx_urls.json', 'r', encoding="utf-8") as f:
            self.__urls = json.load(f)

    @property
    def urls(self):
        return self.__urls

    @property
    def key(self):
        return self.__key

    def __clear_headers(self):
        self.__headers = {
            "auth_key": self.__key,
        }
    
    def __clear_params(self):
        self.__params = {}

    def get_listed(self, date, market="kospi"):
        self.__clear_headers()
        self.__clear_params()

        self.__params["basDd"] = pd.to_datetime(date).strftime("%Y%m%d")
        
        if market == "kospi":
            res = requests.get(url=self.__base_url+self.__urls["stock"]["listed_kospi"], headers=self.__headers, params=self.__params)
        elif market == "kosdaq":
            res = requests.get(url=self.__base_url+self.__urls["stock"]["listed_kosdaq"], headers=self.__headers, params=self.__params) 
        else:
            raise_arg("market", market, ["kospi", "kosdaq"])
        res.raise_for_status()

        res = res.json()["output1"]

        return pd.DataFrame(res)