#!/usr/bin/python
import boto, urllib2
from   boto.ec2 import connect_to_region
import os
import sys
import time
from pair import *


accessKey = 'insertAccessKey'
secretKey = 'insertSecretKey'
region = "us-east-1"
clusterInstType = 't2.medium'
sparkLocation ='/usr/lib/riak/lib/data_platform-1/priv/spark-worker'
filePath = '/home/ubuntu/deploy/downloadStocks.py'
depFilePath = '/home/ubuntu/deploy/pair.py'
deployMode = 'client'
myInst, awsHosts,awsIPs = bootCluster(accessKey,secretKey,region,clusterInstType)
riakIP = awsIPs[0]
masterPort = 7077
masterURL = str('spark://'+str(riakIP)+":"+str(masterPort))
time.sleep(60)
submitSparkJob(filePath, masterURL, depFilePath, deployMode, sparkLocation)
newUpdate = getValue("meta", "update", riakIP)#get date of last update from riak
print newUpdate
stopped = stopCluster(accessKey,secretKey,region,clusterInstType)


