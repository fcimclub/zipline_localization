from datetime import time
import pandas as pd
from pandas.tseries.holiday import (
    Holiday,
    AbstractHolidayCalendar
)
from pytz import timezone
from zipline.utils.calendars import TradingCalendar


class HolidayCalendar(AbstractHolidayCalendar):
    def __init__(self,holiday_list):
        self.rules = holiday_list


CHINANewYearsDay = Holiday(
    "New Year's Day",
    month=1,
    day=1,
)
guoqingDay = Holiday(
    "guoqingDay",
    month=10,
    day=1,

)


class SHExchangeCalendar(TradingCalendar):

    @property
    def name(self):
        return "SH"

    @property
    def tz(self):
        return timezone("Asia/Shanghai")

    @property
    def open_time(self):
        return time(0, 0)

    @property
    def close_time(self):
        return time(15, 0)

    @property
    def regular_holidays(self):
        holiday_list = []
        with open('holidays.txt') as f:
            for line in f.readlines():
                year = int(line[0:4])
                month = int(line[4:6])
                day = int(line[6:8])
                holiday_list.append(Holiday(line, year=year, month=month, day=day))
        return HolidayCalendar(holiday_list)
