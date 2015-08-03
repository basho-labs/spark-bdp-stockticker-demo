# Filename: riak-spark.py

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
from riak import RiakClient, RiakObject
import riak

REGION       = os.environ.get("AWS_EC2_REGION")
env.user      = "ubuntu"
env.key_filename = ["kp1.pem"]

accessKey = 'AKIAIGBU3O2I45SZV57A'
secretKey = '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'
conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=accessKey,aws_secret_access_key=secretKey)
reservations = conn.get_all_instances()
awsHosts = []
awsIPs = []
instances = [i for r in conn.get_all_instances() for i in r.instances]
for i in instances:
	if str(i.__dict__['_state']) == 'running(16)':
		awsHosts.append(str(i.__dict__['dns_name']))
		awsIPs.append(str(i.__dict__['private_ip_address']))


riakIP = awsIPs[0]
numWorkers = 4
masterPort = 7077
masterURL = str('spark://'+str(awsIPs[0])+":"+str(masterPort))

sc = SparkContext(masterURL, "AnalyzeData")
sc.addPyFile('/home/ubuntu/deploy/pair.py')
#Grab data from google finance api
#Daily stock data will be grab for all tickers in tickerFile from today until lastDay
#and written to riak bucket 'stocks'

today = datetime(datetime.now().year, datetime.now().month, datetime.now().day)#todays year,month,day
lastDay = datetime(2000,1,1)#last day to download stock data from
tickerFile = '/home/ubuntu/deploy/NYSE.txt'#file contains all ticker,name pairs on NYSE
dataSource = 'google'#download from 'google' or 'yahoo

stocks = pd.read_csv(tickerFile,sep='\t',header=None)#read in stock ticker,name pairs
tickers = list(stocks[0])#extract tickers


print 'HERE!'
minVol = 20000#minimum volatilty to filter on
minDays = 2000#minimum amount of data points needed
zThresh = 2
beginDay = 0
ndays = 100
critLevel = '1%' #can be '1%', '5%', or '10%'
writeBucket = 'tradeEntries'
print 'HERE!'
print deleteAllKeys(writeBucket,riakIP =riakIP)
#Gather the data into rdd and transform so that pairAnalysis can be run on each pair of stocks

#Spark 
#1:For each ticker we grab the data from riak using riakGetStock
#2:Filter out ticker,data pairs that have less than minDays worth of data
#3:Filter out all ticker,data pairs that have a mean volatility less than minVol
#4:Sort each tickers,data pairs data by date with most recent data at the beginning of the array using mySort
#5:Cut all ticker,data pairs data to be of length minDays using myFilter and cache the rdd in memory
print 'HERE!'
d = sc.parallelize(tickers[0:100]).map(lambda x: (x, riakGetStock(x,riakIP =riakIP)))\
    .filter(lambda x: len(x[1]) > minDays)\
    .filter(lambda x: numpy.mean([i[1] for i in x[1]]) > minVol)\
    .map(lambda x: (x[0],mySort(x[1],2)))\
    .map(lambda x: (x[0],myFilter(x[1],minDays))).cache()

#Analyze all stock pairs and return the results    
    
#Spark    
#from the cahced rdd d create a cartesian product of all possible ticker pairs
#then for each ticker pair run pairAnalysis which returns either a number or a list of values
#collect the rdd

pairs = d.cartesian(d)\
    .map(lambda x: pairAnalysis(x,ndays,beginDay,zThresh))\
    .filter(lambda x: type(x) is list)\
    .coalesce(20)\
    .map(lambda x: writeSinglePair(x,writeBucket,riakIP =riakIP))\
    .collect()
print pairs
print getAllKV('tradeEntries',riakIP =riakIP)

