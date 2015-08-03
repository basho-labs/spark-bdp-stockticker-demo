
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

accessKey = 'AKIAIGBU3O2I45SZV57A'
secretKey = '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'
conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=accessKey,aws_secret_access_key=secretKey)
reservations = conn.get_all_instances()
awsHosts = []
awsIPs = []
instances = [i for r in conn.get_all_instances() for i in r.instances]
for i in instances:
	if str(i.__dict__['_state']) == 'running(16)' and i.instance_type == 't2.medium':
		awsHosts.append(str(i.__dict__['dns_name']))
		awsIPs.append(str(i.__dict__['private_ip_address']))


riakIP = awsIPs[0]
numWorkers = 4
masterPort = 7077
masterURL = str('spark://'+str(awsIPs[0])+":"+str(masterPort))

sc = SparkContext(masterURL, "UpdateData")
sc.addPyFile('/home/ubuntu/deploy/pair.py')
#Grab data from google finance api
#Daily stock data will be grab for all tickers in tickerFile from today until lastDay
#and written to riak bucket 'stocks'

today = datetime(datetime.now().year, datetime.now().month, datetime.now().day)#todays year,month,day
print '::::Getting Date of Last Update::::'
lastUpdate = getValue("meta", "update", riakIP)#get date of last update from riak
print lastUpdate

lastDayUpdated = datetime(lastUpdate['Year'],lastUpdate['Month'],lastUpdate['Day'])#last day to download stock data from
updateStart = lastDayUpdated - timedelta(days=5)
tickerFile = '/home/ubuntu/deploy/NYSE.txt'#file contains all ticker,name pairs on NYSE
dataSource = 'google'#download from 'google' or 'yahoo

stocks = pd.read_csv(tickerFile,sep='\t',header=None)#read in stock ticker,name pairs
tickers = list(stocks[0])#extract tickers
#These operations will populate riak with data
dataGet = sc.parallelize(tickers[0:100],100).map(lambda x:
	(x,downloadStock(x,dataSource,updateStart,today))).map(lambda x: writeHistory(x[0],x[1], riakIP)).collect()

print dataGet
print '::::Writing Date of New Update::::'
newUpdate = {'Year': today.year,\
               'Month': today.month,\
               'Day': today.day,\
               'Hour': datetime.now().hour,\
               'Minute': datetime.now().minute}
print newUpdate
storeKV("meta", "update", json.dumps(newUpdate), riakIP)


