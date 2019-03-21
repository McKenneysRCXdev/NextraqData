import numpy as np
import requests as r
import datetime as d
from threading import Thread
from time import sleep
import csv
import re
import urllib3 #TO remove any of the HTTP warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#The purpose of this code is to export driving data for McKenney's employees from NexTraq.

def listOfUrlRequests():
    #IN DEVELOPMENT - to used in getAllRequests and replace getOneRequest
    cookieDict = manageCookie()
    requestList = []
    vehicleIDArray = fleetIDs()
    # vehicleIDArray = ['69281','176083', '68661']
    dateArray = listOfDays()
    # dateArray = ['03/13/2019', '03/14/2019', '03/15/2019']
    for vehicleID in vehicleIDArray:
        for date in dateArray:
            URL = 'https://go.nextraq.com/json/mobiles/trail?lastThirty=false&date=' + date + '&startTime=0&endTime=86340000&mobileId=' + vehicleID + '&calcDistance=true&includeEvents=true&rnd=1552582940908'
            requestList.append(URL)

    maxToPullAtOnce = 10
    reqHandler = asyncRequestHandler(maxToPullAtOnce)
    for currURL in requestList:
        reqHandler.createNewRequest(currURL, methodType='get', callback=getOneRequest,
                                    timeout=100, cookies=cookieDict)
    reqHandler.startRequests()
    FH.close()

def processOneRequest(JSONdata):
    dataTable = [['TimeStamp', 'Street', 'City', 'Zip Code', 'County','Posted Speed (MPH)',
                'Department', 'Driver ID', 'Mobile ID',
                'Speed Display','Longitude', 'Latitude', 'Heading', 'Calculated Mileage']] #There are 14 columns

    for row in JSONdata:
        buildRow = [0] * 14
        for group in row:
            if group == 'dateDisplay':
                value = row[group] #Grab that timestamp '09/12/2018 5:43:39 AM EDT'
                if type(value) is not str:
                    value = str(value)
                buildRow[0] = value
            elif group == 'address':
                addressPointsOfInterest = ['street','city', 'zip', 'county', 'postedSpeedMph']
                try:
                    for subGroupAddress in row[group]:
                        try:
                            indexA = addressPointsOfInterest.index(subGroupAddress)
                            value = row[group][subGroupAddress]
                            if type(value) is not str:
                                value = str(value)
                            buildRow[indexA + 1] = value
                        except:
                            pass
                except:
                    pass
            elif group == 'mobile':
                for subGroupM in row[group]:
                    if subGroupM == 'primaryFleet':
                        primaryFleetPointsOfInterest = ['name']
                        try:
                            for subSubGroupMP in row[group][subGroupM]:
                                try:
                                    indexPF = primaryFleetPointsOfInterest.index(subSubGroupMP)
                                    value = row[group][subGroupM][subSubGroupMP]
                                    if type(value) is not str:
                                        value = str(value)
                                    buildRow[indexPF + 6] = value
                                except:
                                    pass
                        except:
                            pass
                    # elif subGroupM == 'driver':    #This was taken out due to issues with people not having this set up on the Nextraq
                    #     if len(row[group][subGroupM]['driver'] > 0):
                    #         driverPointsOfInterest = ['firstName', 'lastName', 'driverId']
                    #         for subSubGroupDr in row[group][subGroupM]:
                    #             try:
                    #                 indexPF = driverPointsOfInterest.index(subSubGroupDr)
                    #                 value = row[group][subGroupM][subSubGroupDr]
                    #                 if type(value) is not str:
                    #                     value = str(value)
                    #                 buildRow[indexPF + 8] = value
                    #             except:
                    #                 pass
                    #
                    elif subGroupM == 'name':
                        value = row[group][subGroupM]
                        if type(value) is not str:
                            value = str(value)
                        buildRow[7] = value
                    elif subGroupM == 'mobileId':
                        value = row[group][subGroupM]
                        if type(value) is not str:
                            value = str(value)
                        buildRow[8] = value
            elif group == 'speedDisplay':
                value = row[group]
                if type(value) is not str:
                    value = str(value)
                buildRow[9] = value
            elif group == 'lon':
                value = row[group]
                if type(value) is not str:
                    value = str(value)
                buildRow[10] = value
            elif group == 'lat':
                value = row[group]
                if type(value) is not str:
                    value = str(value)
                buildRow[11] = value
            elif group == 'heading':
                value = row[group]
                if type(value) is not str:
                    value = str(value)
                buildRow[12] = value
            elif group == 'calculatedMileage':
                value = row[group]
                if type(value) is not str:
                    value = str(value)
                buildRow[13] = value
            value = 0
        dataTable.append(buildRow)

    print(dataTable)
    return dataTable

def getOneRequest(reqResponse):
    if reqResponse.status_code == 403:
        return
    checkResponse = reqResponse.status_code
    if checkResponse == 401:  # 400 is the error if there is no data at that station for that time period. The default station is referenced to gain data for that time stamp.
        print("Get New Cookie")
    elif checkResponse != 200:
        print(checkResponse)
    # print(reqResponse.text)
    JSONdata = reqResponse.json()
    dataTable = processOneRequest(JSONdata['data']['tracks'])
    print(dataTable)
    global FH
    if FH == None:
        FH = open('NextraqData.csv', 'w',newline='')
        writer = csv.writer(FH)
        writer.writerows(dataTable)
    else:
        writer = csv.writer(FH)
        writer.writerows(dataTable[1:])

FH = None

def getOneRequestVanilla(vehicle, givenDate, cookie):
    URL = 'https://go.nextraq.com/json/mobiles/trail?lastThirty=false&date='+ givenDate +'&startTime=0&endTime=86340000&mobileId='+ vehicle +'&calcDistance=true&includeEvents=true&rnd=1552582940908'

    rawData = r.get(URL, cookies = cookie)
    checkResponse = rawData.status_code
    if checkResponse == 401:  # 400 is the error if there is no data at that station for that time period. The default station is referenced to gain data for that time stamp.
        print("Get New Cookie")
    elif checkResponse != 200:
        print(checkResponse)
    JSONdata = rawData.json()
    return JSONdata['data']['tracks']

def fleetIDs():
    #Vehicle IDs can be pulled with the following Javascript code used in the console of this website
    #   https://go.nextraq.com/instaview/map.html
    #Code Starts Here:
    # var vehicleIDs = [];
    # for (currTag of document.getElementsByClassName("checkboxIndent")) {
    #   var tagID = currTag.getElementsByTagName("input")[0].id;
    #   vehicleIDs.push(tagID.substr(tagID.lastIndexOf("-")+1));
    # }
    # vehicleIDs.toString(); // prints out the code

    vehicleIDs = "154165,73279,225897,154096,69281,93558,176083,96590,190681,69294,96579,301500,69291,267966,67716,73269,69099,86216,86218,74689,191175,74190,332861,311200,74196,69279,60697,190667,113855,269138,86220,176075,113882,190559,191503,196853,301835,166683,74215,69006,114412,113839,196747,68661,74193,86223,323244,348519,67704,73265,302240,47758,166718,333160,155100,149433,95246,348459,67744,74229,323174,69017,176089,20077,332753,73289,60694,267564,74202,153951,369307,74697,302173,114407,348723,333360,332679,193347,348606,311647,74211,74186,113866,147013,348717,334471,196391,69622,334226,158166,225440,311541,301966,154115,233641,348529,311789,260435,74724,86222,176070,221170,176091,20079,176090,268871,202070,348580,166609,348759,269689,47762,196326,301442,202261,69283,302488,202023,69623,74201,324433,270077,348653,96592,348782,348670,348707,69021,348679,268300,73281,74187,74210,301045,74204,204878,73272,69292,373045,147004,348807,147007,83935,146676,14563,95243,369817,301623,96576,234007,196861,334208,333282,229925,69100,96597,74219,146308,226975,333225,190613,226575,301869,67765,96573,310298,73298,69288,69297,261337,146309,302008,147012,323340,96598,301199,146431,348875,348834,113874,60698,69305,348808,196064,311322,310528,113884,196143,74224,69097,93559,47765,47754,96572,96591,333492,310361,96580,153944,348837,113856,191049,166641,191828,310766,166195,166406,348631,176111,147015,96595,96581,311153,74726,166416,166423,95247,74192,301642,196360,76876,47768,113887,196169,146677,47761,60688,74213,233859,201932,369547,113840,67720,166386,348534,69003,113903,96569,348883,267320,47766,96567,67762,95242,69285,69225,334527,74218,333010,348681,114400,114422,114399,69020,114415,74208,166375,348646,73253,166391,371539,113858,233735,220700,301853,202003,78729,73267,268186,20074,113828,147310,146317,83934,73283,67710,146429,113826,113854,96585,149135,147005,155096,166491,166722,166738,166411,166768,166762,166747,166514,166749,166580,166644,166630,166743,166390,166750,166744,166740,166719,73261,73254,69277,74188,176087,176080,74205,191069,190099,190431,190573,191078,190584,191539,158242,165156,190737,69298,190753,74226,61494,73290,334334,190732,196689,196042,190746,113845,196809,348719,194166,60691,233576,234092,233797,233691,233954,233898,233511,233615,233752,233498,233853,233553,201795,73301,201850,202006,158840,201871,196603,96593,74222,233484,73299,176078,69096,69295,76878,154164,69008,176116,74191,69095,176085,176081,260161,194388,202084,74695,202136,261546,190445,202210,201848,178688,233767,260075,114421,259887,258960,262398,74221,67698,60693,47763,269403,47770,155080,260209,74688,96571,69287,14566,95244,96575,96578,74072,73275,73274,96568,93561,302099,301719,301503,301543,302423,300677,301751,302306,301118,302164,113857,20075,114416,96574,302026,95241,194323,300844,301833,300973,113876,93556,113873,96570,86219,114436,95245,113871,113843,166656,20076,154184,114408,74225,67774,73260,153947,155078,73287,69300,20080,60696,146428,69280,76877,96577,155112,323683,332749,348783,67769,324352,322081,333636,323723,333625,323789,323671,333914,324135,322654,333104,154193,60692,154176,96596,113905,333153,47756,78727,333114,333797,96594,334290,334354,333084,334051,333095,78726,333385,333933,343641,343207,344165,332620,348905,190335,369697,369776,348895,47757,372433,348440,369718,372369,373184,371496,370107,191296,197043,197201,197271,197225,149417,69091,73297,113863,74228,156795,69282,225616,348710,60699,147104,344101,343954,113888,373592,370597,371491,371391,372383,370658,371548,374284,370212,369371,370205,369840,373646,372803,371520,371884,373558,373249,373237,372745,372532,370991,369619,370948,371382,369465,371506,374036,369591,370417,372350,370335,370895,370319,370635,372658,372954,371899,373531,372692,371779,371300,372955,372334,370745,372275,373269,373366,372854,373410,371925,372402,373784,373137,369701,370671,372844,372292,371461,369658,373046,369435,371166,371102,373948,370476,369567,371457,374118,372845,371653,372699,370011,370662,373581,371131,372313,372059,370485,369359,370278,371103,370069,369738,301790,301859,293243,293289,293308,74199"
    vehicleIDArray = vehicleIDs.split(',')
    return vehicleIDArray

def listOfDays():
    #you can only pull 200 days in the past
    maxHistoryInNextraq = 199
    dateArray = []
    today = d.datetime.today()
    for t in range(maxHistoryInNextraq):
        dateDateTime = today - d.timedelta(float(t))
        date = dateDateTime.strftime('%m/%d/%Y')
        dateArray.append(date)
    return dateArray

def manageCookie():
    # Login information - Used Candace's login
    # The Cookie can be obtained from the tracker site: https://go.nextraq.com/find/history.html?cmd=find&mobileId=176083&endDate=03/14/2019
    # ^^^ adjust for dates as necessary - you can only pull 200 days in the past.
    # From tracker website
    #   You can only view one day of records at a time

    Cookie = '_ga=GA1.2.1426985901.1552939345; __utmc=85701263; __utmz=85701263.1552939366.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utma=85701263.1426985901.1552939345.1553107836.1553112773.11; __utmt=1; JSESSIONID=1Xtm0Ysh6SPMQVi4Yc5scANn.web-1; __utmb=85701263.4.10.1553112773'
    splitCookie = Cookie.split('; ')
    CookieDict = {}
    for value in range(len(splitCookie)):
        cookieValuesArray = splitCookie[value].split('=')
        CookieDict[cookieValuesArray[0]] = cookieValuesArray[1]
    return CookieDict

class asyncRequestHandler:

    def __init__(self, numberThreads = 10):
        self.maxThreads = numberThreads
        self.numActiveThreads = 0
        self.requestList = []
        self.nextRequestIndex = 0
        self.responsesRecieved = 0

    def sendOffNextRequest(self):
        if self.numActiveThreads < self.maxThreads and self.nextRequestIndex < len(self.requestList):
            # is good to send off a request
            currRequest = self.requestList[self.nextRequestIndex]
            assert isinstance(currRequest, asyncRequest), "Requests must be of the asyncRequest class."
            currRequest.makeThread() # starts the request
            self.numActiveThreads += 1
            self.nextRequestIndex += 1

    def finishOffRequest(self):
        # this should already be handled in the request callback, so don't do anything
        self.numActiveThreads -= 1
        self.responsesRecieved += 1
        pass

    def createNewRequest(self, urlLink, requestHeaders={}, methodType="get", callback=None,
                         timeout=15, cookies=None, requestData = "", urlParams = {}):
        newRequest = asyncRequest(self, urlLink,
                                  timeout = timeout, methodType = methodType, callback = callback, data=requestData,
                                  verify = False, headers = requestHeaders, cookies = cookies, params= urlParams)
        self.requestList.append(newRequest)

    def startRequests(self, awaitCompletion = True):
        while self.numActiveThreads < self.maxThreads and self.nextRequestIndex < len(self.requestList):
            self.sendOffNextRequest()

        if awaitCompletion == True:
            self.awaitCompletion()

    def awaitCompletion(self, timeStepSeconds = 0.1):
        # now loop until all requests are finished
        while self.responsesRecieved < len(self.requestList):
            sleep(0.1)  # pause for a few seconds

    def callbackResponseFunc(self):
        self.finishOffRequest()
        self.sendOffNextRequest()

class asyncRequest:
    # adapted from https://stackoverflow.com/a/44917020, or https://stackoverflow.com/questions/16015749/in-what-way-is-grequests-asynchronous
    requestMethods = {'get': r.get, 'post': r.post}

    def __init__(self, parentHandler, urlLink, *args, methodType= 'get', callback=None, timeout=15, **kwargs):
        assert isinstance(parentHandler, asyncRequestHandler), "Parent Handler must be of type asyncRequestHandler class."
        self.parentHandler = parentHandler
        self.url = urlLink
        self.transactionArgs = args
        if methodType.lower() not in asyncRequest.requestMethods:
            raise ValueError("Method type needs to be one of {}; method given was {}".format(
                asyncRequest.requestMethods.keys(), methodType))
        self.method = asyncRequest.requestMethods[methodType.lower()]
        self.callback = callback
        self.timeout = timeout
        self.kwargs = kwargs

    def makeThread(self):
        self.kwargs['timeout'] = self.timeout
        self.kwargs['hooks'] = {'response': self.callbackFunc}
        givenArgs = [self.url] + list(self.transactionArgs)
        thread = Thread(target=self.method, args=givenArgs, kwargs=self.kwargs)
        thread.start()

    def callbackFunc(self, response, *args, **kwargs):
        # returns a requests object
        if self.callback != None:
            self.callback(response)
        response.close() # closes the information
        self.parentHandler.callbackResponseFunc()


listOfUrlRequests()

