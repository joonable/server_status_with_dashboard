import pymysql as ms
import abc
import time
import datetime
import sys
import data.config as config

from data.data_helper import icd_dict
ESJob = [4, 5]
MySQLJob = [7, 8, 9, 10]
ZKJob = [7, 8, 9, 10]

class Data():
    def __init__(self):
        self.requiredColumns = None


class Scraper(abc.ABC):

    def __init__(self):
        abc.ABC.__init__(self)
        self.config = config

        # self.lev1 = "검색 시스템"
        # self.lev2 = None
        # self.lev3 = None
        # self.lev4 = None
        # self.lev5 = None

        self.jobList = []
        self.date = datetime.date.today().strftime("%Y%m%d")
        self.time = datetime.datetime.now().time().strftime("%H%M%S")
        self.datetime = self.date + self.time
    # @abc.abstractmethod
    # def (self):
    #     print("Base")

    @abc.abstractmethod
    def getInformation(self):
        pass

    def assertJobList(self, jobList):
        for job in jobList:
            assert job in self.jobList
        self.jobList = jobList


class ESClusterScraper(Scraper):
    def __init__(self, jobList = None):
        self.jobList = ESJob
        self.assertJobList(jobList)
        Scraper.__init__(self)


    def getInformation(self):
        currentLev3 = None
        temp = {}
        for job in self.jobList:
            if icd_dict[job]["LEV3"] != currentLev3:
                currentLev3 = icd_dict[job]["LEV3"]
                #TODO       temp = "getSomeInfo"
            icd_dict[job]["MNTPNG_RESULT"] = temp[icd_dict[job]["LEV5"]]


class MySQLScraper(Scraper):
    def __init__(self, jobList = None):
        self.jobList = MySQLJob
        self.assertJobList(jobList)
        Scraper.__init__(self)

    def func(self):
        currentLev4 = None
        for job in self.jobList:
            if icd_dict[job]["LEV4"] != currentLev4:
                pass        # TODO      문자열을 파싱하여 Ranking vs. A_B로 나눠서 해보자


class ZKScraper(Scraper):
    def __init__(self, jobList = None):
        for job in jobList:
            assert job in ZKJob
        Scraper.__init__(self)

    def getInformation(self):
        pass