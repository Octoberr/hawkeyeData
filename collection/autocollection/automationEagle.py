# coding:utf-8
"""
Wangmeng Song
June 16, 2017
chang
Wangmeng Song
June 16, 2017
修改
Wangmeng Song
June 22, 2017
修改
Wangmeng Song
June 23, 2017
June 24, 2017
June 28,2017
"""


import json
import requests
import socket
import time
import datetime
import os
import pymongo
from retrying import retry
from apscheduler.schedulers.blocking import BlockingScheduler


BROWSER = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 ' \
          '(KHTML, like Gecko) Chrome/40.0.2214.115 Safari/537.36'
HEADER = {'Connection': 'keep-alive', 'User-Agent': BROWSER}
AK = 'DIm4fQkBGR2aZWorGMDPILqclf46gAX5'
serviceID = 139760


class EAGLE:
    def getBDAPI(self, entityName, startTime, endTime, page):
        global AK
        global serviceID
        url = 'http://yingyan.baidu.com/api/v3/track/gettrack?ak={}&service_id={}&entity_name={}&' \
              'start_time={}&end_time={}&page_index={}&page_size=5000'.format(AK, serviceID, entityName, startTime, endTime, page)
        jsondata = json.loads(self.queryJson(url))
        return jsondata

    @retry(stop_max_attempt_number=5, stop_max_delay=30000, wait_fixed=50)
    def queryJson(self, url):
        while True:
            try:
                result = requests.get(url, headers=HEADER).text
            except (socket.timeout, requests.exceptions.Timeout):  # socket.timeout
                raise Exception('timeout', url)
            except requests.exceptions.ConnectionError:
                raise Exception('connection error', url)
            else:
                try:
                    json.loads(result)
                except ValueError:
                    print "no json return, retry."
                except:
                    print "unknown error, retry."
                else:
                    break
        return result

    def writeErrorLog(self, entityname, unixStartTime, unixEndTime):
        logfile = datetime.datetime.now().strftime("%Y%m%d") + '.log'
        if os.path.isfile(logfile):
            f = open(logfile, 'a+')
        else:
            f = open(logfile, 'w')
        f.write('entityname:{}, unixStartTime:{}, unixEndTime:{}, fail\n'.format(entityname, unixStartTime, unixEndTime))
        f.close()

    def bjtimeToUnixtime(self, nowtime):
        unixTime = int(time.mktime(time.strptime(nowtime, '%Y-%m-%d %H:%M:%S')))
        return unixTime

    def unixtimeToBjTime(self, nowUnixtime):
        bjtime = datetime.datetime.fromtimestamp(nowUnixtime)
        return bjtime

    def getTheStartTime(self):
        from config import mongo_config
        client = pymongo.MongoClient(host=mongo_config['host'], port=mongo_config['port'])
        db = client.swmdb
        eagleyedates = db.hawkeyedata
        cursor = eagleyedates.find({}, {"loc_time": 1, "unixEndTime": 1}).sort([("unixEndTime", -1)]).limit(1)
        for element in cursor:
            getunixtime = element['unixEndTime']
            bjtime = self.unixtimeToBjTime(getunixtime)
            return bjtime

    def insertintomongo(self, hawkeyedate):
        from config import mongo_config
        client = pymongo.MongoClient(host=mongo_config['host'], port=mongo_config['port'])
        db = client.swmdb
        eagleyedates = db.hawkeyedata
        eagleyedates.insert(hawkeyedate)
        print datetime.datetime.now(), 'insert mongodb success'

    def processprovidedate(self, hawkeyeData, entityname, unixEndTime):
        addentityname = hawkeyeData['points']
        for element in addentityname:
            element['entityname'] = entityname
            element['unixEndTime'] = unixEndTime
        return addentityname

    def nextPage(self, hawkeyedate, entityname, unixStartTime, unixEndTime):
        pageSum = hawkeyedate['total']/5000
        lastPage = pageSum + 1
        for i in xrange(1, lastPage):
            restdata = self.getBDAPI(entityname, unixStartTime, unixEndTime, i)
            eagledates = self.processprovidedate(restdata, entityname, unixEndTime)
            self.insertintomongo(eagledates)

    def startGetHawkEyeData(self):
        entityNameVec = [u'191', u'901', u'905', u'906', u'909', u'910', u'912', u'913', u'915', u'918', u'920', u'921', u'922', u'923', u'925',
                         u'926', u'928', u'929', u'930', u'932', u'933', u'935', u'936', u'938', u'939', u'950', u'951', u'952', u'956', u'958',
                         u'960', u'961', u'962', u'963', u'965', u'966', u'968', u'969', u'971', u'972', u'975', u'976', u'978', u'979', u'980',
                         u'981', u'982', u'983', u'985', u'986', u'987', u'988', u'989', u'991']
        startBjDate = self.getTheStartTime()
        # print "startdata", startBjDate
        if startBjDate is not None:
            endDate = datetime.datetime.now().date()
            if endDate - datetime.timedelta(days=1) <= startBjDate.date():
                unixStartTime = self.bjtimeToUnixtime(str(startBjDate))
                endDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                unixEndTime = self.bjtimeToUnixtime(endDate)
                for entityname in entityNameVec:
                    hawkeyeData = self.getBDAPI(entityname, unixStartTime, unixEndTime, 1)
                    if (hawkeyeData['status'] is 0) and (hawkeyeData['total'] is 0):
                        continue
                    elif (hawkeyeData['status'] is 0) and (hawkeyeData['total'] <= 5000):
                        eagledates = self.processprovidedate(hawkeyeData, entityname, unixEndTime)
                        self.insertintomongo(eagledates)
                    elif (hawkeyeData['status'] is 0) and (hawkeyeData['total'] > 5000):
                        eagledates = self.processprovidedate(hawkeyeData, entityname, unixEndTime)
                        self.insertintomongo(eagledates)
                        self.nextPage(hawkeyeData, entityname, unixStartTime, unixEndTime)
                    else:
                        self.writeErrorLog(entityname, unixStartTime, unixEndTime)
            else:
                timeTable = ["00:00:00", "12:00:00", "23:59:59"]
                endBjdate = datetime.datetime.now().date()
                for entityname in entityNameVec:
                    startDate = startBjDate.date() + datetime.timedelta(days=1)
                    while startDate < endBjdate:
                        for i in xrange(len(timeTable) - 1):
                            startTime = str(startDate) + " " + timeTable[i]
                            endTime = str(startDate) + " " + timeTable[i + 1]
                            unixStartTime = self.bjtimeToUnixtime(startTime)
                            unixEndTime = self.bjtimeToUnixtime(endTime)
                            hawkeyeData = self.getBDAPI(entityname, unixStartTime, unixEndTime, 1)
                            if (hawkeyeData['status'] is 0) and (hawkeyeData['total'] is 0):
                                continue
                            elif (hawkeyeData['status'] is 0) and (hawkeyeData['total'] <= 5000):
                                eagledates = self.processprovidedate(hawkeyeData, entityname, unixEndTime)
                                self.insertintomongo(eagledates)
                            elif (hawkeyeData['status'] is 0) and (hawkeyeData['total'] > 5000):
                                eagledates = self.processprovidedate(hawkeyeData, entityname, unixEndTime)
                                self.insertintomongo(eagledates)
                                self.nextPage(hawkeyeData, entityname, unixStartTime, unixEndTime)
                            else:
                                self.writeErrorLog(entityname, unixStartTime, unixEndTime)
                        startDate += datetime.timedelta(days=1)
        else:
            timeTable = ["00:00:00", "12:00:00", "23:59:59"]
            endBjdate = datetime.datetime.now().date()
            for entityname in entityNameVec:
                startDate = datetime.date(2017, 05, 03)
                while startDate < endBjdate:
                    for i in xrange(len(timeTable)-1):
                        startTime = str(startDate) + " " + timeTable[i]
                        endTime = str(startDate) + " " + timeTable[i+1]
                        unixStartTime = self.bjtimeToUnixtime(startTime)
                        unixEndTime = self.bjtimeToUnixtime(endTime)
                        hawkeyeData = self.getBDAPI(entityname, unixStartTime, unixEndTime, 1)
                        if (hawkeyeData['status'] is 0) and (hawkeyeData['total'] is 0):
                            continue
                        elif (hawkeyeData['status'] is 0) and (hawkeyeData['total'] <= 5000):
                            eagledates = self.processprovidedate(hawkeyeData, entityname, unixEndTime)
                            self.insertintomongo(eagledates)
                        elif (hawkeyeData['status'] is 0) and (hawkeyeData['total'] > 5000):
                            eagledates = self.processprovidedate(hawkeyeData, entityname, unixEndTime)
                            self.insertintomongo(eagledates)
                            self.nextPage(hawkeyeData, entityname, unixStartTime, unixEndTime)
                        else:
                            self.writeErrorLog(entityname, unixStartTime, unixEndTime)
                    startDate += datetime.timedelta(days=1)


def main():
    print datetime.datetime.now(), "Start collecting"
    HK = EAGLE()
    HK.startGetHawkEyeData()


if __name__ == '__main__':
    print datetime.datetime.now(), "The program has started"
    main()
    scheduler = BlockingScheduler()
    # scheduler.add_job(some_job, 'interval', hours=1)
    scheduler.add_job(main, 'interval', minutes=5)
    scheduler.start()