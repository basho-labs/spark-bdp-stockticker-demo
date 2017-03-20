
from datetime import datetime
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

accessKey = 'put aws access key here'
secretKey = 'put aws secret key here'
region = "us-east-1"
instanceType = 't2.medium'
depFilePath1 = '/home/ubuntu/deploy/pair.py'
depFilePath2 = '/home/ubuntu/deploy/NYSE.txt'


awsHosts, awsIPs = getDNSIP(accessKey,secretKey,region,instanceType)

riakIP = awsIPs[0]
masterPort = 7077
masterURL = str('spark://'+str(awsIPs[0])+":"+str(masterPort))

sc = SparkContext(masterURL, "PopulateData")
sc.addPyFile(depFilePath1)
#Grab data from google finance api
#Daily stock data will be grab for all tickers in tickerFile from today until lastDay
#and written to riak bucket 'stocks'

today = datetime(datetime.now().year, datetime.now().month, datetime.now().day)#todays year,month,day
lastDay = datetime(2000,1,1)#last day to download stock data from
tickerFile = depFilePath2#file contains all ticker,name pairs on NYSE
dataSource = 'google'#download from 'google' or 'yahoo

stocks = pd.read_csv(tickerFile,sep='\t',header=None)#read in stock ticker,name pairs
tickers = list(stocks[0])#extract tickers
#These operations will populate riak with data
dataGet = sc.parallelize(tickers[0:100],100).map(lambda x:
	(x,downloadStock(x,dataSource,lastDay,today))).map(lambda x: writeHistory(x[0],x[1], riakIP)).collect()

updateDate(riakIP)
