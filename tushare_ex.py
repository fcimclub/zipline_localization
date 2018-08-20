# -*- coding: utf-8 -*-
"""
Created on Thu May 17 14:00:02 2018

@author: l_cry
"""
# 导入模块
import tushare as ts
from datetime import datetime
import time
import numpy as np
from sqlalchemy import create_engine
import pymysql
from concurrent.futures import ThreadPoolExecutor
pymysql.install_as_MySQLdb()
engine=create_engine('mysql://root:120402@127.0.0.1:3306/data_stock?charset=utf8')



        
def get_tick(code,date):
    #now=datetime.now().strftime('%Y-%m-%d')
    #df  = ts.get_today_ticks(code, retry_count=5, pause=5)
    print(code+date)
    try:
        df = ts.get_tick_data(code,date=date,src='tt')
        df['code'] = code
        df['datetime'] = date+' '+df['time']
        df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df.drop('time',axis=1,inplace=True)
    
        df.to_sql('hist_data_tick', con=engine, if_exists='append', index=False)
        print(code+'has beed inserted into db')
        time.sleep(np.random.rand())
    except:
        pass

def get_codes_tick(code_list,success_list):
    for code in set(code_list)-set(success_list):
        try:
            get_tick(code)
            success_list.append(code)
        except:
            pass
            
pool = ThreadPoolExecutor(3)            
stock_basics=ts.get_stock_basics()
stock_basics.reset_index('code',inplace=True)
code_list = stock_basics.code
# date=datetime.now().strftime('%Y-%m-%d 00:00:00')
date = '2018-08-06'
#i=0
#while i < 10000:
#    re = engine.execute('''SELECT DISTINCT(code) from hist_data_tick where datetime>"%s"'''%(date))
#    res = re.fetchall()
#    success_list = list(map(lambda x:x[0],res))
#    get_codes_tick(code_list,success_list)
#    i += 1
#    if len(set(code_list) - set(success_list)) <1:
#        break

re = engine.execute('''SELECT DISTINCT(code) from hist_data_tick where datetime>"%s"'''%(date))
# re = engine.execute('''SELECT DISTINCT(code) from hist_data_tick''')
res = re.fetchall()
success_list = list(map(lambda x:x[0],res))
for code in set(code_list)-set(success_list):
    #pool.submit(get_tick,(code,date))
    get_tick(code,date)
    






        
    