
from datetime import datetime, timedelta
from pyspark import SparkContext
import pandas as pd
import numpy
import time
from pair import *
import boto, urllib2
from   boto.ec2 import connect_to_region
from   fabric.api import env, run, cd, settings, sudo
from   fabric.api import parallel
import os
import sys

accessKey = 'insertAccessKey'
secretKey = 'insertSecretKey'
region = "us-east-1"
instanceType = 't2.medium'
depFilePath1 = '/home/ubuntu/deploy/pair.py'
depFilePath2 = '/home/ubuntu/deploy/NYSE.txt'

awsHosts, awsIPs = getDNSIP(accessKey,secretKey,region,instanceType)

riakIP = awsIPs[0]
masterPort = 7077
masterURL = str('spark://'+str(riakIP)+":"+str(masterPort))

sc = SparkContext(masterURL, "UpdateData")
sc.addPyFile(depFilePath1)
#Grab data from google finance api
#Daily stock data will be grab for all tickers in tickerFile from today until lastDay
#and written to riak bucket 'stocks'

today = datetime(datetime.now().year, datetime.now().month, datetime.now().day)#todays year,month,day
print '::::Getting Date of Last Update::::'
lastUpdate = getValue("meta", "update", riakIP)#get date of last update from riak
print lastUpdate

lastDayUpdated = datetime(lastUpdate['Year'],lastUpdate['Month'],lastUpdate['Day'])#last day to download stock data from
updateStart = lastDayUpdated - timedelta(days=5)
tickerFile = depFilePath2#file contains all ticker,name pairs on NYSE
dataSource = 'google'#download from 'google' or 'yahoo

stocks = pd.read_csv(tickerFile,sep='\t',header=None)#read in stock ticker,name pairs
tickers = list(stocks[0])#extract tickers
#These operations will populate riak with data
dataGet = sc.parallelize(tickers[0:100],100).map(lambda x:
	(x,downloadStock(x,dataSource,updateStart,today))).map(lambda x: writeHistory(x[0],x[1], riakIP)).collect()

print dataGet

updateDate(riakIP)



