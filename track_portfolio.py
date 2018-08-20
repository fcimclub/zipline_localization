# -*- coding: utf-8 -*-
"""
Created on Thu May 17 10:37:45 2018

@author: l_cry
"""

import pandas as pd


'''
    pfo_path:投组的文件路径；
            code：股票代码，
            name：股票名称，
            num：股票数量，
    ref_path:参照指数的文件路径；需包含日期date、当日指数收盘价close
    -------------
    return：
        pfo:df
        ref:df
'''
def pre_solving(pfo_path,ref_path):
    pd.read_excel(ref_path)
    pd.read_excel(pfo_path)
    

def track_portfolio(pfo,ref,start_date,end_date=None):
    '''
        计算投组价值
    Parameters：
    ---------------
        pfo：投组df；
            code：代码
            name：名称
            num：数量
        ref：对照表df，用于计算投组日间价值
            index
                date：日期
            columns
                code：代码
            contents
                不同代码的日收盘价
    Returns：
    ---------------
        DateFrame：
            index:
                date:日期
            columns:
                code:代码
            content:
                代码不同日的市值
    '''
    df=pd.DataFrame(index=ref.index[ref.index>start_date])
    codes=ref.columns.values
    for code,num in zip(pfo.code,pfo.num):
        if code in codes:
            df[code]=num*ref[code][ref.index>start_date]
        else:
            raise Exception('some codes are not included')
    return df


def portfolio_profit(pfo,ref,index,start_date,end_date=None):
    '''
        计算投组价值
    Parameters：
    ---------------
        pfo：投组df；
            code：代码
            name：名称
            num：数量
        ref：对照表df，用于计算投组日间价值
            columns
                date：日期
                code：代码
                contents
                不同代码的日收盘价
        index：指数df，用于pfo表现的比较
            date：日期
            close：收盘价
    Returns：
    ---------------
        DateFrame：
            date:日期,
            mv:市值,	
            ivg:指数收益,	
            pvg:投组收益,	
            alp:超额收益
    '''
    df=track_portfolio(pfo,ref,start_date,end_date=None)
    return df.apply(sum,axis=1).to_frame(name='pv').merge(index,left_index=True,right_index=True,how='left')


import os
path='C:/Users/l_cry/Nextcloud/HW/gp/'
path="C:/Users/l_cry/Documents/Tencent Files/1204027935/FileRecv/投组20180521/"
files=os.listdir(path)
file_map={}
dfhs300 = pd.read_excel('C:/Users/l_cry/Documents/Tencent Files/1204027935/FileRecv/gp/hs300gp20170101.xlsx',index_col=0)
index=pd.read_excel('C:/Users/l_cry/Desktop/指数行情序列hs300.xls',sheet_name='Sheet2',index_col=0)
for file in files:
    temp=portfolio_profit(pd.read_excel(path+file,header=None,names=['code','name','num','unknown']),dfhs300,index,file[1:5]+'-'+file[5:7]+'-'+file[7:9],'')
    temp['pvg']=temp.pv.apply(lambda x :(x-temp.pv[0])/temp.pv[0])
    temp['ivg']=temp.close.apply(lambda x :(x-temp.close[0])/temp.close[0])
    temp['alp']=temp.apply(lambda x: x['pvg']-x['ivg'],axis=1)
    file_map[file]=temp
    writer = pd.ExcelWriter('C:/Users/l_cry/Desktop/'+file.split('.')[0]+'_track'+'.xlsx')
    file_map[file].to_excel(writer,'Sheet1',header=['投组价值','指数','投组收益','指数收益','超额收益'],index_label='日期')
    writer.save()


