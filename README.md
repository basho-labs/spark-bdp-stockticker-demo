# riak-spark-project


Overview
----
The goal of this project was to create a an automated trading signal generator use case for the Basho Data Platform.  The signal generator would be run using a cluster of AWS EC2 machines that would boot up each night after the markets closed.  After the cluster boots up, the Riak database is updated with the previous days market data.  The database is initally filled with approx. 10 years of data on every stock currently trading on the NYSE provided by Google Finance API.  Once the database is updated, then analysis creates all possible stock pairs (9 million) and runs an Engle-Granger Cointegration test. Stocks that are cointegrated are then filtered by using a standard deviation parameter.  All pairs that are cointegrated and currently outside a standard deviation threshold are then written back into the Riak database for potentially further analysis before being traded.  This process is repeated each night at 1am.

Quick Start
----
1. Create a blank AWS Ubuntu 12.04 AMI
2. Download and install the Basho Data Platform on the newly created AMI
3. Clone this repo on the AMI in dir /home/ubuntu/deploy
4. On the AMI install pip then run `sudo pip install -r /path/to/requirements.txt`
5. Save the AMI
6. Create one EC2 t2.micro with this AMI, this will act as the launcher
7. Create five EC2 t2.mediums with this AMI, these will act as compute cluster
8. From /home/ubunut/deploy dir on t2.micro, run the following to set up BDP cluster: `sudo sh setupBDPAWS.sh`
This should tie together the BDP cluster.  This only needs to be run once.
9. From /home/ubunut/deploy dir on t2.micro, run the following to setup automated running:`python setupCron.py`
10. Populate the database by running the following: `python populateData.py`

This completes the setup.  You now have a single t2.micro that will boot a sleeping 5 node BDP t2.medium cluster each night at 1 am, update the BDP Riak database, run analysis on all stock pairs on the NYSE, write the results back to the BDP Riak database, shut down the 5 node BDP t2.medium cluster, and wait until tomorrow to do it all over again, ad infintum.





Files
----
1. NYSE.txt: this file contains stock symbols for the New York Stock Exchange.
2. fabfile.py: this file is a Fabric library that can be used to create BDP clusters on AWS.
3. pair.py: this file is a python library containing methods used to boot cluster, download data, retreive data from Riak, run analysis, write data to Riak, and shutdown cluster.
4. populateData.py: this file is used to initial file Riak with historical raw stock data.
5. requirements.txt: pythn librarys that need to be installed with pip on all machines in the cluster and client machine.
6. riak-spark.py: this file contains the main functionality of the project.  It is where Spark analysis is done.
7. run.py: this file is run from the client machine to boot cluster, update data, run analysis, and shutdown cluster.
8. setupBDPAWS.sh: shell script to automatically launch and configure a BDP Spark cluster on AWS.  Uses fabfile.py library
9. setupCron.py: this file is used to configure cron on launcher machine.
10. updateData.py: this file updates raw stock data


Warnings
----
1. To run full prgram you must modify 'tickers[0:100]' to 'tickers' in riak-spark.py and updateData.py.
This program should not be run on anything less than one launcher/client node and 5 cluster nodes.  The run time will be >> 2 hours otherwise.
2. This program was tested and runs, however, there is no guarantee of success or accuracy and some shaking may be neccesary.
3. You need to place your AWS access key and secret key in some files before running
4. You need to ensure correct permissions on AWS for automatic booting and shut down as well as opening all incoming and outgoing ports in the security group
