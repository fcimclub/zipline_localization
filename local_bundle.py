

"""
本地数据库的数据导入bundle
cris@fcim
"""

import pymysql
import pandas as pd
import os
from zipline.data.bundles import register
from zipline.data.bundles.core import load,ingest
from zipline.utils.calendars import get_calendar,register_calendar
from SHCalendar import SHExchangeCalendar

connect = pymysql.Connect(host='localhost',
                          port=3306,
                          user='root',
                          passwd='120402',
                          db='data_stock',
                          charset='utf8')


def local_bundle(environ,
                 asset_db_writer,
                 minute_bar_writer,
                 daily_bar_writer,
                 adjustment_writer,
                 calendar,
                 start_session,
                 end_session,
                 cache,
                 show_progress,
                 output_dir):
    metadata, histories, symbol_map = get_basic_info()

    # 准备写入dailybar
    daily_bar_writer.write(get_hist_data(symbol_map, histories),
                           show_progress=show_progress)
    asset_db_writer.write(metadata)

    print('over')


def get_basic_info():
    sql = '''select * from stock_market_info'''
    ts_symbols = pd.read_sql(sql=sql, con=connect, index_col='code')
    symbols = []
    histories = {}
    i = 0
    for code, row in ts_symbols.iterrows():
        i += 1
        if i > 4:
            break
        sql = '''select * from hist_data_daily where code = %s''' % code
        histories[code] = pd.read_sql(sql=sql, con=connect, index_col='date')
        srow = {}
        srow['start_date'] = histories[code].index[-1]
        srow['end_date'] = histories[code].index[0]
        srow['symbol'] = code
        srow['asset_name'] = row['name']
        symbols.append(srow)

    df_symbols = pd.DataFrame(data=symbols).sort_values('symbol')
    symbol_map = pd.DataFrame.copy(df_symbols.symbol)

    # fix the symbol exchange info
    df = df_symbols.apply(func=convert_symbol_series, axis=1)

    return df, histories, symbol_map


def symbol_to_exchange(symbol):
    isymbol = int(symbol)
    if (isymbol >= 600000):
        return symbol + ".SS", "SSE"
    else:
        return symbol + ".SZ", "SZSE"


def convert_symbol_series(s):
    symbol, e = symbol_to_exchange(s['symbol'])
    s['symbol'] = symbol
    s['exchange'] = e
    return s


def get_hist_data(symbol_map, histories):
    for sid, index in symbol_map.iteritems():
        if index in histories.keys():
            history = histories[index][['open', 'high', 'close', 'amount', 'factor', 'low','volume']].copy()

            """
            writer needs format with
            [index], open, close, high, low, volume
            so we do not need to change the format from tushare
            but we need resort it
            """
            yield sid, history.sort_index()



register_calendar("SHSZ", SHExchangeCalendar(), force=True)
#singleton in python
shsz_calendar =get_calendar("SHSZ")

register(
        'local-data-bundle',
        local_bundle,
        "SHSZ"
)
ingest('local-data-bundle')

