# Basho Data Platform Spark Stock Ticker Demo

The Basho Data Platform (BDP) integrated NoSQL databases, caching, in-memory analytics, and search components into a single deployment and management stack.

This project is designed to leverage BDP for stock analysis on AWS. It has been tested with BDP 1.0 Enterprise. Please [open an Issue](https://github.com/basho-bin/spark-bdp-stockticker-demo/issues) if you have any difficulty deploying your own copy after reading the Installation.

**Note:** This code is for reference purposes only and should not be run in production. This is very unlikely to make you money on the New York Stock Exchange (NYSE).

## Quick Links

* [Overview](#overview)
* [Installation](#installation)
  * [Configuration](#configuration)
* [Architecture](#architecture)
* [License and Authors](#license-and-authors)
  * [Maintainers](#maintainers)

## Overview
This began as an internship at Basho. Read more about this project in [this blog post](http://basho.com/posts/technical/algorithms-and-stock-tickers-with-apache-spark-my-summer-internship-with-basho).

The goal of this project was to create an automated trading signal generator use case for the Basho Data Platform.  The signal generator would be run using a cluster of AWS EC2 machines that would boot up each night after the markets closed.  After the cluster boots up, the Riak KV database is updated with the previous day's market data.  

The database is initially filled with approximately 10 years of data on every stock currently trading on the New York Stock Exchange (NYSE) provided by Google Finance API.  Once the database is updated, then analysis creates all possible stock pairs (9 million) and runs an Engle-Granger Cointegration test. Stocks that are cointegrated are then filtered by using a standard deviation parameter.  All pairs that are cointegrated and currently outside a standard deviation threshold are then written back into the Riak KV database for further analysis by a separate application before being traded.  This process is repeated each night at 1am local time.

## Installation
In order to run this demo, the following steps must be completed in order.

1. Create a blank AWS Ubuntu 12.04 AMI
2. Download and install the Basho Data Platform on the newly created AMI
3. Clone this repo on the AMI in dir `/home/ubuntu/deploy`
4. On the AMI install `pip` then run `sudo pip install -r /path/to/requirements.txt`
5. Save the AMI
6. Create one (1) EC2 t2.micro with this AMI, this will act as the launcher
7. Create five (5) EC2 t2.mediums with this AMI, these will act as the compute cluster
8. From `/home/ubuntu/deploy` directory on the t2.micro system, run the following to set up BDP cluster: `sudo sh setupBDPAWS.sh`

This should form the BDP cluster. This only needs to be run once. To confirm the cluster status, run `sudo data-platform-admin services` on any node in the cluster and you should see:

     $ sudo data-platform-admin services
     Running Services:
     +------------+---------------+--------------+
     |   Group    |    Service    |     Node     |
     +------------+---------------+--------------+
     |spark-master|my-spark-master|riak@127.0.0.1|
     |spark-master|my-spark-worker|riak@127.0.0.2|
     |spark-master|my-spark-worker|riak@127.0.0.3|
     |spark-master|my-spark-worker|riak@127.0.0.4|
     |spark-master|my-spark-worker|riak@127.0.0.5|
     |spark-master|my-spark-worker|riak@127.0.0.6|
     +------------+---------------+--------------+


9. From the `/home/ubuntu/deploy` directory on the t2.micro system, run the following command to automate the process described under [Overview](#overview): `python setupCron.py`
10. Populate the database by running the following: `python populateData.py`

### Configuration

1. This program should not be run on anything less than the recommended number of AWS instances
1. Users must modify the 'tickers[0:100]' to 'tickers' in `riak-spark.py` and `updateData.py`
3. You need to place your AWS access key and secret key in `riak-spark.py`, `populateData.py`, `fabfile.py`, `run.py` and `updateData.py`
4. You need to ensure correct permissions on AWS for automatic booting and shutdown as well as opening all incoming and outgoing ports in the security group

This completes the setup.  You now have a single t2.micro that will boot a sleeping 5 node BDP t2.medium cluster each night at 1am, update the BDP Riak database, run analysis on all stock pairs on the NYSE, write the results back to the BDP Riak database, shutdown the 5 node BDP t2.medium cluster, and wait until tomorrow to do it all over again, ad infintum.


## Architecture
Here is what each file does:

* NYSE.txt: plaintext file that contains stock symbols for the New York Stock Exchange
* fabfile.py: Fabric library that can be used to create BDP clusters on AWS
* pair.py: python library containing methods used to boot the BDP cluster, download data, retrieve data from Riak KV, run the analysis, write data to Riak KV, and shut down the cluster
* populateData.py: run for the initial fill of the BDP Riak database with raw historical stock data
* requirements.txt: list of python libraries that need to be installed with `pip` on all machines in this demo
* riak-spark.py: this file contains the main functionality of the project.  It is where Spark analysis is done
* run.py: run from the client machine to boot the BDP cluster, update data, run analysis, and shut down the cluster
* setupBDPAWS.sh: script that uses `fabfile.py` to automatically launch and configure a BDP Spark cluster on AWS
* setupCron.py: configures cron on the t2.micro system to schedule the 1am run
* updateData.py: Used to update raw stock data


## License and Authors

* Author: [Korrigan Clark](https://github.com/korry8911)

### Maintainers
* Korrigan Clark ([GitHub](https://github.com/korry8911))
* and You! [Read up](https://github.com/basho-labs/the-riak-community) and get involved

You can [read the full guidelines](http://docs.basho.com/riak/latest/community/bugs/) for bug reporting and code contributions on the Riak Docs. And **thank you!** Your contribution is incredibly important to us.

Copyright (c) 2014 Basho Technologies, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
