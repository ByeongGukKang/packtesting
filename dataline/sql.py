import time

import pandas as pd
from sqlalchemy import create_engine

from .tools import DateManager

class QDP:

    def __init__(self, host, user, passward, database, port):
        self.__sql_engine  = create_engine("mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(
                                                                                                    user=user,
                                                                                                    pw=passward,
                                                                                                    host=host,
                                                                                                    port=port,
                                                                                                    db=database
                                                                                                )
                                        )                                       

    @property
    def table(self):
        command = "SHOW TABLE STATUS"
        df = pd.read_sql_query(sql=command, con=self.__sql_engine)
        return df

    def get(self, table, symbols=None, columns=None, start=None, end=None, symbol_check=True):
        """
        Args:
            table (str):
            symbols (list):
            columns (list):
            start (DATE or DATETIME):
            end (DATE or DATETIM):
            symbol_check (bool):
        
        Returns:
            pd.DataFrame
        """

        # Manual limit of frequent queries... TODO --> 서버단위의 Query Limit 생성?
        time.sleep(0.25)

        # The code below returns Exception if the queried symbol doesn't exist in the TABLE
        if symbol_check:
            not_exist_symbol = []
            command = "SELECT DISTINCT SYMBOL FROM {table}".format(table=table)
            exist_symbol = set(pd.read_sql_query(sql=command, con=self.__sql_engine)["SYMBOL"])
            for symbol in symbols:
                if symbol not in exist_symbol:
                    not_exist_symbol.append(symbol)
            if len(not_exist_symbol) != 0:
                raise Exception("{} doesn't exist in {table}".format(not_exist_symbol, table=table))

        # Replace arguments for SQL query
        if type(symbols) == type(None):
            symbols = "*"
        else:
            symbols = str(symbols).replace("[","(").replace("]",")")

        if type(columns) == type(None):
            columns = "*"
        else:
            columns = ["DATE", "SYMBOL"] + columns
            columns = str(columns).replace("[","(").replace("]",")").replace("'","")

        if type(start) == type(None):
            start = DateManager.now("%Y-%m-%d")
        if type(end) == type(None):
            end = DateManager.now("%Y-%m-%d")

        # SQL Query
        command = "SELECT {columns} FROM {table} ".format(columns=columns, table=table)

        if symbols != "*":
            command = command + "WHERE SYMBOL in {symbols} ".format(symbols=symbols)
        
        if "WHERE" in command:
            command = command + "AND DATE BETWEEN '{start}' AND '{end}'".format(start=start, end=end)
        else:
            command = + "WHERE DATE BETWEEN '{start}' AND '{end}'".format(start=start, end=end)

        # Run the SQL command
        df = pd.read_sql_query(sql=command, con=self.__sql_engine)

        return df