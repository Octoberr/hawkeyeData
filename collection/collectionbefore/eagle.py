# coding:utf-8
"""
Wangmeng Song
June 14, 2017
"""


import json
import requests
import socket
import time
import datetime
import os
import pymongo
from retrying import retry


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

    @retry
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
        unixTime = int(time.mktime(time.strptime(nowtime, '%Y-%m-%d%H:%M:%S')))
        return unixTime

    def insertintomongo(self, hawkeyedate):
        from config import mongo_config
        client = pymongo.MongoClient(host=mongo_config['host'], port=mongo_config['port'])
        db = client.swmdb
        eagleyedates = db.eagleyedates
        eagleyedates.insert(hawkeyedate)
        print 'insert mongodb success'

    def processprovidedate(self, hawkeyeData, entityname,unixStartTime, unixEndTime):
        eagledates = {}
        eagledates['unixEndTime'] = unixEndTime
        eagledates['unixStartTime'] = unixStartTime
        eagledates['entityname'] = entityname
        eagledates['distance'] = hawkeyeData['distance']
        eagledates['endPoint'] = hawkeyeData['end_point']
        eagledates['points'] = hawkeyeData['points']
        eagledates['startPoint'] = hawkeyeData['start_point']
        eagledates['tollDistance'] = hawkeyeData['toll_distance']
        return eagledates

    def nextPage(self, hawkeyedate, entityname, unixStartTime, unixEndTime):
        pageSum = hawkeyedate['total']/5000
        lastPage = pageSum + 1
        for i in xrange(1, lastPage):
            restdata = self.getBDAPI(entityname, unixStartTime, unixEndTime, i)
            eagledates = self.processprovidedate(restdata, entityname, unixStartTime, unixEndTime)
            self.insertintomongo(eagledates)

    def startGetHawkEyeData(self):
        timeTable = ["00:00:00", "12:00:00", "23:59:59"]
        entityNameVec = [191, 901, 905, 906, 909, 910, 912, 913, 915, 918, 920, 921, 922, 923, 925,
                         926, 928, 929, 930, 932, 933, 935, 936, 938, 939, 950, 951, 952, 956, 958,
                         960, 961, 962, 963, 965, 966, 968, 969, 971, 972, 975, 976, 978, 979, 980,
                         981, 982, 983, 985, 986, 987]
        for entityname in entityNameVec:
            startDate = datetime.date(2017, 05, 03)
            while startDate < datetime.datetime.now().date():
                for i in xrange(len(timeTable)-1):
                    startTime = str(startDate) + timeTable[i]
                    endTime = str(startDate) + timeTable[i+1]
                    unixStartTime = self.bjtimeToUnixtime(startTime)
                    unixEndTime = self.bjtimeToUnixtime(endTime)
                    hawkeyeData = self.getBDAPI(entityname, unixStartTime, unixEndTime, 1)
                    if (hawkeyeData['status'] is 0) and (hawkeyeData['total'] is 0):
                        continue
                    elif (hawkeyeData['status'] is 0) and (hawkeyeData['total'] <= 5000):
                        eagledates = self.processprovidedate(hawkeyeData, entityname, unixStartTime, unixEndTime)
                        self.insertintomongo(eagledates)
                    elif (hawkeyeData['status'] is 0) and (hawkeyeData['total'] > 5000):
                        eagledates = self.processprovidedate(hawkeyeData, entityname, unixStartTime, unixEndTime)
                        self.insertintomongo(eagledates)
                        self.nextPage(hawkeyeData, entityname, unixStartTime, unixEndTime)
                    else:
                        self.writeErrorLog(entityname, unixStartTime, unixEndTime)
                startDate += datetime.timedelta(days=1)


def main():
    print "Started!"
    HK = EAGLE()
    HK.startGetHawkEyeData()


if __name__ == '__main__':
    main()