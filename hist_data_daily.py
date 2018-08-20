"""
Created on Thu May 17 14:00:02 2018

@author: l_cry
"""
# 导入模块
import tushare as ts
from datetime import datetime, timedelta
import time
import numpy as np
from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()
engine = create_engine('mysql://root:120402@127.0.0.1:3306/data_stock?charset=utf8')

stock_basics = ts.get_stock_basics()
stock_basics.reset_index(inplace=True)
stock_basics.rename(columns={'index': 'code'}, inplace=True)
# stock_basics.to_sql('stock_market_info',con=engine,if_exists='append',index=False)
now = datetime.now()
end = now.strftime("%Y-%m-%d")
start = (now - timedelta(days=1)).strftime("%Y-%m-%d")


def insert_to_db(code_list, success_list=[], none_list=[], fail_list=[]):
    for code in set(code_list) - set(success_list) - set(none_list):
        try:
            df = ts.get_h_data(code, autype=None, drop_factor=False, pause=5, retry_count=5)
            # df = ts.get_h_data(code, autype=None, drop_factor=False, pause=5, retry_count=5, start=start, end=end)
            if df.shape[0] != 0:
                df.reset_index(inplace=True)
                df['code'] = code
                df['update_time'] = datetime(2018, 8, 4, 0, 0)
                df.to_sql('hist_data_daily', con=engine, if_exists='append', index=False)
                success_list.append(code)
            else:
                none_list.append(code)
        except:
            fail_list.append(code)
            time.sleep(10)
    return success_list, none_list, fail_list


code_list = stock_basics.code

fail_list = []
success_list = []
none_list = []
i = 0
while len(set(code_list) - set(success_list) - set(none_list)) > 0 and i < 10000:
    success_list, none_list, fail_list = insert_to_db(code_list, success_list=success_list, none_list=none_list,
                                                    fail_list=fail_list)
    i += 1



re = engine.execute('SELECT DISTINCT(code) from hist_data_daily')
res = re.fetchall()
success_list = list(map(lambda x:x[0],res))




