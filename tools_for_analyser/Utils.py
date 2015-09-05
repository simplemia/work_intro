#!/usr/bin/python2.7
##-*- coding: utf-8 -*-
# author: zhangrong
# email: hanzejl@hotmail.com

import sys

import time
import datetime
import calendar

if sys.version > '2.6':
    from hashlib import md5
else:
    import md5

# function: conditions ? a : b
def ThreeOperator(conditions, a, b):
    return (conditions and [a] or [b])[0]


# change the bytes unit
def unitChange(gprs, unitIn = 'B',
               unitOut = 'M', human = False):
    units = {
        'B': 1024 ** 0,
        'K': 1024 ** 1,
        'M': 1024 ** 2,
        'G': 1024 ** 3,
        'T': 1024 ** 4,
        'P': 1024 ** 5,
    }

    if isinstance(gprs, (int, str)):
        gprs = float(gprs)
    inGprs = gprs * units[unitIn]

    if human:
        for uO in units:
            if 1 <= inGprs / units[uO] < 1024:
                return '%.1f %s'%(inGprs / units[uO], uO)

    return inGprs / units[unitOut]


class Judge:
    def __init__(self):
        pass

    @classmethod
    def isNumeric(self, number, type = "int"):
        try:
            eval("%s(%s)"%(type, number))
            return True
        except:
            return False

    @classmethod
    def isFloat(self, number):
        pass


class Silent:
    def __init__(self):
        pass

    @classmethod
    def sreturn(self, func, default = None):
        try:
            return func()
        except:
            return default


# the date time util function
# format a string into a date
class DateTimeUtil:
    # the init function
    # the flag value is in 'STRING', 'DATETIME', 'VALUES', 'TIMESTRUCT'
    def __init__(self, one, two = '', three = '',
                 four = '', five = '', six = '', seven = ''):
        self.__value = self.__factory(one)(one, two, three, four, five, six, seven)
        # self.__maxDay = calendar.monthrange(self.__value.year, self.__value.month)[1]
        self.__def__format = "%Y%m%d"
        # print(type(self.value))
        # self.value = datetime.datetime(2014, 12, 1)
        # return self


    # wrapper function
    def __factory(self, flag):
        if isinstance(flag, str):
            return lambda dStr, dFormat, *more: self.__dealWithString(dStr,
                                                                      ThreeOperator(dFormat, dFormat, "%Y%m%d"),)

        elif isinstance(flag, time.struct_time):
            return lambda timeStruct, *more: self.__dealWithTimeStruct(timeStruct)

        elif isinstance(flag, (int, float)):
            return lambda year, month, day, hour, minute, second, millisecond: \
                self.__dealWithValues(year, ThreeOperator(isinstance(month, int), month, 1),
                                      ThreeOperator(isinstance(day, int), day, 1),
                                      ThreeOperator(isinstance(hour, int), hour, 0),
                                      ThreeOperator(isinstance(minute, int), minute, 0),
                                      ThreeOperator(isinstance(second, int), second, 0),
                                      ThreeOperator(isinstance(millisecond, int), millisecond, 0),)

        elif isinstance(flag, datetime.datetime):
            return lambda dtime, *more: self.__dealWithDateTime(dtime)


    # deal with the string format
    def __dealWithString(self, dStr, dFormat):
        timeS = time.strptime(dStr, dFormat)

        return self.__dealWithTimeStruct(timeS)

    # time_struct
    def __dealWithTimeStruct(self, timeStruct):
        if not isinstance(timeStruct, time.struct_time):
            raise AttributeError

        return self.__dealWithValues(timeStruct.tm_year, timeStruct.tm_mon,
                                     timeStruct.tm_mday, timeStruct.tm_hour,
                                     timeStruct.tm_min, timeStruct.tm_sec)

    # deal with the datetime object
    def __dealWithDateTime(self, dtime):
        return dtime

    # deal with the values
    def __dealWithValues(self, year, month = 1, day = 1,
                         hour = 0, minute = 0, second = 0,
                         milliseconds = 0):
        try:
            maxDay = calendar.monthrange(year, month)[1]
            if day < 0:
                day = maxDay + 1 + day
            return datetime.datetime(year, month, day, hour,
                                     minute, second, milliseconds)
        except Exception:
            raise


    def dayDelta(self, days = 0, seconds = 0, microseconds = 0,
                 milliseconds = 0, minutes = 0, hours = 0,
                 weeks = 0, months = 0, years = 0):
        # year, month, day
        newYear, newMonth, newDay = self.__value.year, self.__value.month, self.__value.day
        if years != 0 or months != 0:
            totalMonths = (self.__value.year + years) * 12 + \
                          (self.__value.month + months) - 1
            newYear = int(totalMonths / 12)
            newMonth = totalMonths % 12 + 1
            newMaxDay = calendar.monthrange(newYear, newMonth)[1]
            newDay = ThreeOperator(self.__value.day > newMaxDay, newMaxDay, self.__value.day)

        self.__value = datetime.datetime(newYear, newMonth, newDay, self.__value.hour,
                                         self.__value.minute, self.__value.second,
                                         self.__value.microsecond)

        newDate = self.__value + datetime.timedelta(days, seconds, microseconds,
                                                    milliseconds, minutes, hours, weeks)

        self.__init__(newDate)

        return self


    @classmethod
    def dayDelta_s(self, orignalDate, days = 0, seconds = 0, microseconds = 0,
                   milliseconds = 0, minutes = 0, hours = 0,
                   weeks = 0, months = 0, years = 0):
        orignalDateCopy = orignalDate.copy()

        return orignalDateCopy.dayDelta(days, seconds, microseconds,
                                        milliseconds, minutes,
                                        hours, weeks, months, years)

    # some getter methor, maxday, year, month,
    # day, hour, minute, second, millisecond
    def maxDay(self):
        return calendar.monthrange(self.__value.year, self.__value.month)[1]

    # if the current date is first Date
    def firstDay(self, flag = 'MONTH', days = 1):
        results = self.__value.day == 1

        if flag == 'YEAR':
            results = results and (self.__value.month == 1)

        return results

    # if the current date is last Date
    def lastDay(self, flag = 'MONTH', days = 1):
        results = self.__value.day == self.maxDay()

        if flag == 'YEAR':
            results = results and (self.__value.month == 12)

        return results

    # judge the if the month is first month
    def firstMonth(self):
        return self.__value.month == 1

    # judge if the month is last month
    def lastMonth(self):
        return self.__value.month == 12

    # return datetime class
    def datetime(self):
        return self.__value

    # tomorrow
    def tomorrow(self):
        self.dayDelta(days = 1)

        return self

    def yesterday(self):
        self.dayDelta(days = -1)

        return self

    # pervious month
    def previous(self):
        self.dayDelta(months = -1)

        return self

    # next month
    def next(self):
        self.dayDelta(months = 1)

        return self

    # the year of current date
    def year(self):
        return self.__value.year

    # the month of currrent date
    def month(self):
        return self.__value.month

    # the day of current date
    def day(self):
        return self.__value.day

    # the hour of current date
    def hour(self):
        return self.__value.hour

    # the minute of current date
    def minute(self):
        return self.__value.minute

    # the second of current date
    def second(self):
        return self.__value.second

    # the weekday of current day
    def weekday(self):
        return self.__value.weekday()

    # the microsecond of current day
    def microsecond(self):
        return self.__value.microsecond

    # format the current date and time to string
    def format(self, outFormat = "%Y-%m-%d %H:%M:%S", isoFormat = False):
        try:
            if isoFormat:
                return self.__value.isoformat()
            return self.__value.strftime(outFormat)
        except Exception:
            raise

    # return the today
    @classmethod
    def today(self):
        return DateTimeUtil(datetime.datetime.today())

    # return the now
    @classmethod
    def now(self):
        return DateTimeUtil(datetime.datetime.now())

    # return the utc now
    @classmethod
    def utcnow(self):
        return DateTimeUtil(datetime.datetime.utcnow())

    # time format
    @classmethod
    def formatChange(self, string, inFormat = "%Y-%m-%d", outFormat = "%Y%m%d"):
        try:
            return DateTimeUtil(string, inFormat).format(outFormat)
        except Exception:
            raise

    # range
    def range(self, endDate, sep = 1):
        sd, dates, e = self, [], endDate - 1

        while sd <= e:
            dates.append(sd)
            sd = sd + 1

        return dates

    # some operator override
    # add, +
    def __add__(self, days):
        return DateTimeUtil.dayDelta_s(self, days)

    # sub, -
    def __sub__(self, days):
        if isinstance(days, int):
            return DateTimeUtil.dayDelta_s(self, -days)
        elif isinstance(days, DateTimeUtil):
            return (self.datetime() - days.datetime()).days

    # print
    def __str__(self):
        return self.format(isoFormat = True)

    # equal X == Y
    def __eq__(self, cmpDate):
        try:
            return self.__value == cmpDate.datetime()
        except:
            raise

    # not equal X != Y
    def __ne__(self, cmpDate):
        try:
            return self.__value != cmpDate.datetime()
        except:
            raise

    # less then X < Y
    def __lt__(self, cmpDate):
        try:
            return self.__value < cmpDate.datetime()
        except:
            raise

    # less equal X <= Y
    def __le__(self, cmpDate):
        try:
            return self.__value <= cmpDate.datetime()
        except:
            raise

    # grater then X >= Y
    def __gt__(self, cmpDate):
        try:
            return self.__value > cmpDate.datetime()
        except:
            raise

    # greater equal X >= Y
    def __ge__(self, cmpDate):
        try:
            return self.__value >= cmpDate.datetime()
        except:
            raise

    # the assign for =
    def assign(self):
        return DateTimeUtil(self.datetime())


    # the copy
    def copy(self):
        return DateTimeUtil(self.datetime())
