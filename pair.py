# Filename: pair.py
import sys
import numpy
import time
from operator import add
import pandas as pd
import datetime
import json
import urllib2
import pytz
from bs4 import BeautifulSoup
from datetime import datetime
from pandas.io.data import DataReader
import riak
import numpy as np
import statsmodels.api as stat
import statsmodels.tsa.stattools as ts
from riak import RiakClient, RiakObject
import boto, urllib2
from   boto.ec2 import connect_to_region
import os
import sys

#start is furthest day back and end is most recent day.  Grabs all data in between and stores in riak as json
#runs through a file of ticker values
def getData(tickerFile, dataSource, start, end, riakIP):

    rc = RiakClient(protocol='pbc',host = riakIP, pb_port=8087)#set up riak connection
    added = []#list of successful adds
    notAdded = []#list of unsuccessful adds
    stock = pd.read_csv(tickerFile,sep='\t',header=None)#read in stock tickers

    #loop over all stock tickers
    for i in range(0,len(stock.head(100))):
        
        ticker = stock.ix[i,0]
        if getDataByTicker(ticker,dataSource,start,end,riakIP) == 0:
            notAdded.append(ticker)
        else:
            added.append(ticker)
    return added, notAdded

#start is furthest day back and end is closest to today, Store single stock data in riak
#only grabs one stock
def getDataByTicker(ticker, dataSource, start, end, riakIP):

    rc = RiakClient(protocol='pbc',host = riakIP, pb_port=8087)
    #get daily data for each ticker
    gtemp = pd.DataFrame()
    bucket = rc.bucket('stocks')
    try:
        gtemp = DataReader(ticker,  dataSource, start, end)
        print ticker
    except:
        pass
        
        #didnt get any data
    if len(gtemp) == 0:
        return 0
    #got data
    else:
        
        for j in range(0,len(gtemp.index)):
            
            #upload json to Riak Bucket
            date = gtemp.index[j].date()
            riakKey = str(ticker + '_' + str(date))
            riakVal = {'OPEN': gtemp.values[j,0],\
                        'HIGH': gtemp.values[j,1],\
                        'LOW': gtemp.values[j,2], \
                        'CLOSE': gtemp.values[j,3], \
                        'VOLUME': gtemp.values[j,4],\
                        'DATE': str(date),\
                        'TICKER': str(ticker)}
                
            obj = RiakObject(rc, bucket, riakKey)
                
            obj.add_index("ticker_bin", str(ticker))
            obj.add_index("year_int", int(date.year))
            obj.add_index("month_int", int(date.month))
            obj.add_index("day_int", int(date.day))
                
            obj.content_type = 'text/json'
            #obj.data = riakVal
            obj.data = json.dumps(riakVal)
            obj.store()

    return len(gtemp.index)

def downloadStock(ticker,dataSource,start,end):
    gtemp = pd.DataFrame()
    try:
        gtemp = DataReader(ticker,  dataSource, start, end)
        print ticker
    except:
        pass
    return gtemp

def writeHistory(ticker, data, riakIP):
    rc = RiakClient(protocol='pbc',host = riakIP, pb_port=8087)
    bucket = rc.bucket('stocks')
    gtemp = data
    if len(gtemp) == 0:
        return 0
    else:

        for j in range(0,len(gtemp.index)):
                
                #upload json to Riak Bucket
                date = gtemp.index[j].date()
                riakKey = str(ticker + '_' + str(date))
                riakVal = {'OPEN': gtemp.values[j,0],\
                            'HIGH': gtemp.values[j,1],\
                            'LOW': gtemp.values[j,2], \
                            'CLOSE': gtemp.values[j,3], \
                            'VOLUME': gtemp.values[j,4],\
                            'DATE': str(date),\
                            'TICKER': str(ticker)}
                    
                obj = RiakObject(rc, bucket, riakKey)
                    
                obj.add_index("ticker_bin", str(ticker))
                obj.add_index("year_int", int(date.year))
                obj.add_index("month_int", int(date.month))
                obj.add_index("day_int", int(date.day))
                    
                obj.content_type = 'text/json'
                #obj.data = riakVal
                obj.data = json.dumps(riakVal)
                obj.store()

    return len(gtemp.index)

#searches riak bucket via 2i query and returns a dict of the data
def riakSearchData(searchBucket, searchTerm, searchVal1, searchVal2,riakIP):
    myData = {}#empty dict
    myBucket = RiakClient(protocol='pbc',host = riakIP, pb_port=8087).bucket(searchBucket)
    #check wether 1 or 2 search terms
    if searchVal2 != None:
        for key in myBucket.get_index(searchTerm, searchVal1, searchVal2): #get all keys with 2i match
            myData[key] = json.loads(myBucket.get(key).data)#store data for each key
    else:
        for key in myBucket.get_index(searchTerm, searchVal1):#get all keys with 2i match
            myData[key] = json.loads(myBucket.get(key).data)#store data for each key
    return myData

#store an individual key value pair in a bucket
def storeKV(myBucket, myKey, myVal, riakIP):
    riak.RiakClient(protocol='pbc',host = riakIP, pb_port=8087).bucket(myBucket).new(myKey, data = myVal).store()
    return

#delete a key from a bucket, provide feedback to ensure deletion
def deleteKey(delBucket, delKey,riakIP):
    riak.RiakClient(protocol='pbc',host = riakIP, pb_port=8087).bucket(delBucket).delete(delKey)
    if riak.RiakClient(protocol='pbc',host = riakIP, pb_port=8087).bucket(delBucket).get(delKey).data == None:
        print 'Successful delete: %s' % delKey
    else:
        print 'Failed delete: %s' % delKey
    return

#delete key from bucket, no feedback
def quickDeleteKey(delBucket,delKey, riakIP):
    riak.RiakClient(protocol='pbc',host = riakIP, pb_port=8087).bucket(delBucket).delete(delKey)
    return

#delete all keys in a bucket, no feedback  
def quickDeleteAllKeys(delBucket,riakIP):
    for keys in  riak.RiakClient(protocol='pbc',host = riakIP, pb_port=8087).bucket(delBucket).stream_keys():
        for delKey in keys:
            quickDeleteKey(delBucket, delKey,riakIP)      
    print 'Done'
    return

#delete all keys in a bucket, with feedback
def deleteAllKeys(delBucket,riakIP):
    delList = []
    try:
        for keys in  riak.RiakClient(protocol='pbc',host = riakIP, pb_port=8087).bucket(delBucket).stream_keys():
            for delKey in keys:
                deleteKey(delBucket, delKey,riakIP)
                delList.append(delKey)
    except:
        print 'delete error'
        pass
    return delList

#get all key value pairs from a bucket
def getAllKV(myBucket,riakIP):
    myData = {}
    riak_bucket = riak.RiakClient(protocol='pbc',host = riakIP, pb_port=8087).bucket(myBucket)
    for keys in riak_bucket.stream_keys():
        for key in keys:
            tempData = riak_bucket.get(key).data
            print('Key: %s Value: %s' % (key, tempData))
            myData[key] = tempData
    return myData

#get single value for a key in a bucket
def getValue(myBucket, myKey,riakIP):
    myVal = json.loads(riak.RiakClient(protocol='pbc',host = riakIP, pb_port=8087).bucket(myBucket).get(myKey).data)
    return myVal

#Take a tuple of tuples in and return something
def pairAnalysis(pairTuple, ndays, beginDay = 0, zThresh = 2, critLevel = '5%'):
    
    #pair tuple looks like ([tickerA, [data]],[tickerB,[data]])
    #input is assumed to be same length and sorted by date with most recent date first
    
    #unwrap first stock ticker and data
    stockA = pairTuple[0]
    stockAData = list(stockA[1])
    
    #unwrap the data for stockA
    stockADates = [x[2] for x in stockAData]
    stockAClose = [x[0] for x in stockAData]
    stockAVolume = [x[1] for x in stockAData]
    
    #unwrap second stock ticker and data
    stockB = pairTuple[1]
    stockBData = list(stockB[1])
   
    #unwrap stockB data
    stockBDates = [x[2] for x in stockBData]
    stockBClose = [x[0] for x in stockBData]
    stockBVolume = [x[1] for x in stockBData]
    
    pair = pairCalc(stockAClose,stockBClose,beginDay,ndays, zThresh, critLevel)
    #if pair tradeable, add some more info
    if type(pair) is list:
            pair.insert(0,stockADates[beginDay])
            pair.insert(0,stockB[0])
            pair.insert(0,stockA[0])
            return pair
    else:
        return pair

def pairCalc(tsA,tsB,beginDay,ndays,zThresh = 2, critLevel = '5%'):
    
    #perform engle granger cointegration test
    if beginDay < 0 or beginDay >= ndays or zThresh < 0 or not (critLevel in ['1%','5%','10%']):
        print 'input error'
        return 0

    coint = egct(tsA[beginDay:ndays],tsB[beginDay:ndays], critLevel)
    
    #if coint return 0, then the two timeseries are not cointegrated
    if (coint[0] != 1):
        return 0
    #else calculate stuff
    else:
        #signal = tsA[0] - beta*tsB[0] - CONSTANT = normal gaussian with mean 0
        signal = [a - coint[1][1]*b - coint[1][0] for a in tsA[beginDay:ndays] for b in tsB[beginDay:ndays]]
        sigMean = numpy.mean(signal)
        sigStd = numpy.std(signal)
        #zscore is (signal - signalMean) / signalStd
        zscore = (signal[beginDay] - sigMean)/sigStd
        #if current zscore is larger than zThresh, possible pair to trade
        if abs(zscore) > zThresh:
            return [tsA[0],tsB[0], zscore, coint[1][1], sigMean, sigStd]
    return 1

#write tradeable pair back into riak
def writePairs(pairList,bucketName,riakIP):
    
    #tradeable pairs are lists
    tradeable = [x for x in pairList if type(x) is list]
    
    for pair in tradeable:
        writeSinglePair(pair,bucketName,riakIP)
    
    #return a list of written pairs
    return tradeable
       
#write a signle pair to riak
#assumes pair is in a list of values
def writeSinglePair(pair,bucketName,riakIP):
    
    rc = RiakClient(protocol='pbc',host = riakIP, pb_port=8087)
    bucket = rc.bucket(bucketName)
    
    #create key value pairs to stock in riak
    key = str(str(pair[0])+ '_' + str(pair[1]))
    val = {'StockA': pair[0], \
                'StockB': pair[1], \
                'Date': pair[2],\
                'CloseA': pair[3], \
                'CloseB': pair[4], \
                'ZScore': pair[5],\
                'Beta': pair[6],\
                'SignalMean': pair[7],\
                'SignalSD': pair[8]}
    myDate = pair[2].split('-')
    obj = RiakObject(rc, bucket, key)
        
    #add 2i tags
    obj.add_index("stocka_bin", str(pair[0]))
    obj.add_index("stockb_bin", str(pair[3]))
    obj.add_index("year_int", int(myDate[0]))
    obj.add_index("month_int", int(myDate[1]))
    obj.add_index("day_int", int(myDate[2]))
    obj.content_type = 'text/json'
    obj.data = val
    obj.data = json.dumps(val)
    #store
    obj.store()
    
    #return a list of written pairs
    return pair   
    
#return 1 if the two series are cointegrated and 0 otherwise, return regression parameters either way
#assumes y,x are aligned and of equal length
#critLevel can be '1%', '5%' or '10%'
def egct(y, x,critLevel):
    
    #must add a constant row of 1s to dependent variable, its a multidimensional regression thing
    x = stat.add_constant(x)
    #get residuals
    result = stat.OLS(y, x).fit()
    #regression parameters, slope and intercept
    regPar = result.params
    #run augmented dickey fuller test of stationarity of residuals
    #null hypothesis is stationaity of timeseries
    adfResults = ts.adfuller(result.resid, maxlag=0, regression='c', autolag=None, store=False, regresults=True)
    #test statistic
    tstat = adfResults[0]
    #critical value
    critVal = adfResults[2][critLevel]
    #if test stat is less than critical value, accept null hyptohesis of stationarity
    if tstat < critVal:
        return [1,regPar]
    else:
        return [0,regPar]

#get all values for a stock from riak  
#return close,volume,date values in a list of list
def riakGetStock(searchVal,riakIP):
    myData = []
    myBucket = RiakClient(protocol='pbc',host = riakIP, pb_port=8087).bucket('stocks')
    for key in myBucket.get_index('ticker_bin', searchVal): # get all from 2002 to 2012
        value = json.loads(myBucket.get(key).data)
        myData.append([(value['CLOSE']), (value['VOLUME']), str(value['DATE'])])
    return myData

#quick function to sort a list of list on the inner list 3 value(date)
def mySort(s,n):

    try:
        sortList = list(s)
        sortList.sort(key=lambda x: x[n], reverse=True)
    except:
        print "error using mySort"
        return 0

    return sortList

#cut length of time series to n
def myFilter(s,n):
    if type(s) is list:
        try:
            return s[0:n]
        except:
            print 'error using myFilter'
            return 0
    else:
        print 'not a list'
        return 0

def bootCluster(accessKey,secretKey,region,instanceType):

    conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=accessKey,aws_secret_access_key=secretKey)
    instances = [i for r in conn.get_all_instances() for i in r.instances]

    #start all non running instances
    myInst = []
    awsHosts = []
    awsIPs = []

    for i in instances:
        if i.state == 'stopped' and i.instance_type == instanceType:
            conn.start_instances(i.id)
            myInst.append(str(i.id))

    for i in myInst:
        i.update()

        while i.state != 'running':
            print 'waiting for: ' + str(i.id)
            time.sleep(2)
            i.update()

        awsHosts.append(str(i.dns_name))
        awsIPs.append(str(i.private_ip_address))

        print str(i.id)+ ' is running'

    return myInst, awsHosts, awsIPs

def stopCluster(accessKey,secretKey,region,instanceType):

    conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=accessKey,aws_secret_access_key=secretKey)
    instances = [i for r in conn.get_all_instances() for i in r.instances]

    myInst = []

    for i in instances:
        if i.state == 'running' and i.instance_type == instanceType:
            conn.stop_instances(i.id)
            myInst.append(str(i.id))

    for i in myInst:
        i.update()
    
        while i.state != 'stopped':
            print 'waiting for: ' + str(i.id)
            time.sleep(2)
            i.update()
        print str(i.id)+ ' is stopped'

    return myInst

def submitSparkJob(sparkJob):

    os.system('fab -R worker '+ sparkJob)

    return 'Submitted: ' + sparkJob

def getDNSIP(accessKey,secretKey,region,instanceType):

    conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=accessKey,aws_secret_access_key=secretKey)
    awsHosts = []
    awsIPs = []
    instances = [i for r in conn.get_all_instances() for i in r.instances]

    for i in instances:
        if i.state == 'running' and i.instance_type == 't2.medium':
            awsHosts.append(str(i.dns_name))
            awsIPs.append(str(i.private_ip_address))

    return awsHosts, awsIPs

def updateDate(riakIP):

    print '::::Writing Date of New Update::::'
    newUpdate = {'Year': datetime.now().year,\
                   'Month': datetime.now().month,\
                   'Day': datetime.now().day,\
                   'Hour': datetime.now().hour,\
                   'Minute': datetime.now().minute}
    print newUpdate
    storeKV("meta", "update", json.dumps(newUpdate), riakIP)

    return

# End of pair.py