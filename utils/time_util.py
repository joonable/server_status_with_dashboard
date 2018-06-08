import time
import datetime


def get_yesterday():
    yesterday = datetime.date.today() - datetime.timedelta(1)
    return yesterday


def time_str_to_time_stamp(time_str):
    time_str = time.mktime(datetime.datetime.strptime(time_str, "%Y%m%d%H%M").timetuple())
    time_stamp = str(int(time_str))
    return time_stamp


def get_time_stamp(date_, time_):
    time_str = date_.strftime('%Y%m%d') + time_
    time_stamp = time_str_to_time_stamp(time_str)
    return time_stamp
