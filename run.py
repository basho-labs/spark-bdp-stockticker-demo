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
sparkJob1 = 'runUpdate'
sparkJob2 = 'runAnalysis'
myInst, awsHosts,awsIPs = bootCluster(accessKey,secretKey,region,clusterInstType)
riakIP = awsIPs[0]
time.sleep(60)
oldUpdate = getValue("meta", "update", riakIP)#get date of last update from riak
print oldUpdate
submitSparkJob(sparkJob1)
time.sleep(2)
submitSparkJob(sparkJob2)
newUpdate = getValue("meta", "update", riakIP)#get date of last update from riak
print newUpdate
stopped = stopCluster(accessKey,secretKey,region,clusterInstType)
