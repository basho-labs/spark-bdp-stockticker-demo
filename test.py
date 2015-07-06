from pair import *
import numpy as np
import datetime as dt

def test_mySort_1():

	testList = [[1,2], [3,4], [2,1], [3,5]]

	assert mySort(testList,0) == [[3, 4], [3, 5], [2, 1], [1, 2]]
 	assert mySort(testList, 9) == 0

def test_myFilter_1():

	testList = [[1,2], [3,4], [2,1], [3,5]]

	assert len(myFilter(testList, 3)) == 3
	assert myFilter(testList, len(testList)+1) == testList
	assert myFilter('myString', 3) == 0
	assert myFilter(testList, 'test') == 0

def test_egct_1():

	testTSA = range(100)
	testTSB = [x*2 for x in testTSA]+ np.random.normal(0,.01,100)
	testTSC = np.random.normal(0,1,100)

	assert egct(testTSB,testTSC,'1%')[0] == 0
	assert egct(testTSB,testTSA,'10%')[0] == 1


def test_pairCalc_1():

	#set up time series
	testTSA = range(100)
	testTSB = [x*2 for x in testTSA]+ np.random.normal(0,.01,100)
	testTSC = np.random.normal(0,1,100)
	testTSD = [x*4 for x in testTSA + np.random.normal(0,.1,100)]

	assert pairCalc(testTSB,testTSC,0,100,2,'10%') == 0
	assert pairCalc(testTSB,testTSC,0,10000,2,'10%') == 0
	assert pairCalc(testTSB,testTSC,100,100,2,'10%') == 0
	assert pairCalc(testTSB,testTSC,0,100,2,'100%') == 0

	#testTSB and testTSD are cointegrated time series
	assert len(pairCalc(testTSB,testTSD,0,100,0,'10%')) == 6

def test_pairAnalysis_1():
	import pandas as pd
	#setup time series
	testTSA = range(100)
	testTSB = [x*2 for x in testTSA]+ np.random.normal(0,.01,100)
	testTSC = np.random.normal(0,1,100)
	testTSD = [x*4 for x in testTSA + np.random.normal(0,.1,100)]
	testTSE = [x**2 - x**3 + 111*x for x in testTSA]

	pairTupleA = (['A',zip(testTSB,testTSB,testTSB)],['B',zip(testTSD,testTSD,testTSD)])
	pairTupleB = (['A',zip(testTSB,testTSB,testTSB)],['B',zip(testTSE,testTSE,testTSE)])

	assert len(pairAnalysis(pairTupleA,100,0,0,'10%')) == 9
	assert pairAnalysis(pairTupleB,100,0,0,'10%') == 0


























