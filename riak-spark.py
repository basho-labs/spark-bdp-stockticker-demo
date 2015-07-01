# Filename: riak-spark.py

from datetime import datetime
from pyspark import SparkContext
import pandas as pd
import numpy
import time
from pair import *

sc = SparkContext("local", "Simple App")
sc.addPyFile('./code/pair.py')
#Grab data from google finance api
#Daily stock data will be grab for all tickers in tickerFile from today until lastDay
#and written to riak bucket 'stocks'

today = datetime(datetime.now().year, datetime.now().month, datetime.now().day)#todays year,month,day
lastDay = datetime(2000,1,1)#last day to download stock data from
tickerFile = './code/NYSE.txt'#file contains all ticker,name pairs on NYSE
dataSource = 'google'#download from 'google' or 'yahoo

stocks = pd.read_csv(tickerFile,sep='\t',header=None)#read in stock ticker,name pairs
tickers = list(stocks[0])#extract tickers
#These operations will populate riak with data
#t0 = time.time()
#dataGet = sc.parallelize(tickers[0:100]).map(lambda x: getDataByTicker(x,dataSource,lastDay,today)).collect()#get data and write to riak for each ticker
#t1 = time.time()
#total = t1-t0
#total

minVol = 20000#minimum volatilty to filter on
minDays = 2000#minimum amount of data points needed
zThresh = 2
beginDay = 0
ndays = 100
critLevel = '5%' #can be '1%', '5%', or '10%'
delKeys = deleteAllKeys('tradeEntries')
#Gather the data into rdd and transform so that pairAnalysis can be run on each pair of stocks

#Spark 
#1:For each ticker we grab the data from riak using riakGetStock
#2:Filter out ticker,data pairs that have less than minDays worth of data
#3:Filter out all ticker,data pairs that have a mean volatility less than minVol
#4:Sort each tickers,data pairs data by date with most recent data at the beginning of the array using mySort
#5:Cut all ticker,data pairs data to be of length minDays using myFilter and cache the rdd in memory

t0 = time.time()#time begin
d = sc.parallelize(tickers[0:100]).map(lambda x: (x, riakGetStock(x)))\
    .filter(lambda x: len(x[1]) > minDays)\
    .filter(lambda x: numpy.mean([i[1] for i in x[1]]) > minVol)\
    .map(lambda x: (x[0],mySort(x[1])))\
    .map(lambda x: (x[0],myFilter(x[1],minDays))).cache()

#Analyze all stock pairs and return the results    
    
#Spark    
#from the cahced rdd d create a cartesian product of all possible ticker pairs
#then for each ticker pair run pairAnalysis which returns either a number or a list of values
#collect the rdd

pairs = d.cartesian(d)\
    .map(lambda x: pairAnalysis(x,ndays,beginDay,zThresh))\
    .filter(lambda x: type(x) is list)\
    .map(lambda x: writeSinglePair(x))\
    .cache().collect()
t1 = time.time()#time end

total = t1-t0
total

getAllKV('tradeEntries')

