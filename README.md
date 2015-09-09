# riak-spark-project


Overview
----
The goal of this project was to create a an automated trading signal generator use case for the Basho Data Platform.  The signal generator would be run using a cluster of AWS EC2 machines that would boot up each night after the markets closed.  After the cluster boots up, the Riak database is updated with the previous days market data.  The database is initally filled with approx. 10 years of data on every stock currently trading on the NYSE provided by Google Finance API.  Once the database is updated, then analysis creates all possible stock pairs (9 million) and runs an Engle-Granger Cointegration test. Stocks that are cointegrated are then filtered by using a standard deviation parameter.  All pairs that are cointegrated and currently outside a standard deviation threshold are then written back into the Riak database for potentially further analysis before being traded.  This process is repeated each night at 1am.

Quick Start
----
Create a blank AWS Ubuntu 12.04 AMI
Download and install the Basho Data Platform on the newly created AMI
Clone this repo on the AMI in dir /home/ubuntu/deploy
On the AMI install pip then run
```
pip install -r /path/to/requirements.txt
```
Now save the AMI
Create one EC2 t2.micro with this AMI, this will act as the launcher
Create five EC2 t2.mediums with this AMI, these will act as compute cluster






Files:

NYSE.txt - this file contains stock symbols for the New York Stock Exchange.

fabfile.py - this file is a Fabric library that can be used to create BDP clusters on AWS.

pair.py - this file is a python library containing methods used to boot cluster, download data, retreive data from Riak, run analysis, write data to Riak, and shutdown cluster.

populateData.py - this file is used to initial file Riak with historical raw stock data.

requirements.txt - pythn librarys that need to be installed with pip on all machines in the cluster and client machine.

riak-spark.py - this file contains the main functionality of the project.  It is where Spark analysis is done.

run.py - this file is run from the client machine to boot cluster, update data, run analysis, and shutdown cluster.

setupBDPAWS.sh - shell script to automatically launch and configure a BDP Spark cluster on AWS.  Uses fabfile.py library

setupCron.py - this file is used to configure cron on client machine.

updateData.py - this file updates raw stock data

The setup to use the project is the following:

Install BDP and requirements.txt, and copy this repo to an AWS AMI

From this new AMI, create one EC2 to act as the launcher/client and N EC2s to act as the computing cluster.  The launcher instance should be different from the cluster instances.  Otherwise you will have to modify some code in fabfile.py and pair.py

On the cluster nodes, run setupBDPAWS.sh to tie BDP Spark nodes together.  This only needs to be done once since they will automatically connect to each other after each restart.

On the launcher node, run setupCron.py so the program will run each night at 1 am.

Setup complete.

Warnings:

To run full prgram you must modify 'tickers[0:100]' to 'tickers' in riak-spark.py and updateData.py.
This program should not be run on anything less than one launcher/client node and 5 cluster nodes.  The run time will be >> 2 hours otherwise.
This program was tested and runs, however, there is no guarantee of success or accuracy and some shaking may be neccesary.
