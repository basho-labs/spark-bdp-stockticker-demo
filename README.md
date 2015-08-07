# riak-spark-project

The goal of this project was to create a use case for the Basho Data Platform.  

The use case is a pairs trading signal generator.  The idea is to store historical stock data in Riak by way of the
BDP, then analyze all possible stock pairs (naively, 9 million) on the New York Stock Exchange.  The analysis involves checking for
checking cointegration between the two stocks in a pair and then determining if the cointegration signal is above 
or below a predetermined standard deviation threshold.  All pairs that meet the criteria are then written back into
the BDPs Riak cluster.  All analysis is done using the BDPs Spark cluster.

The BDP and the project code was loaded onto an Ubuntu 12.04 AMI on AWS.  I created 5 t2.medium instances with this AMI
for my BDP cluster (which includes Riak and Spark).  Then I created a t2.micro instance to act as my code launcher. 
Crontab was used on the t2.miro to boot up my BDP cluster each night at 1am and submit jobs to Spark.  The jobs 
include updating the historical stock data in the Riak cluster and then running the signal generation analysis on the
updated data.  The project successfully runs and shows off the utility of the Basho Data Platform.
