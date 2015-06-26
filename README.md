# riak-spark-project

This project uses Riak to store historical daily stock data for all stocks on the NYSE and Spark to analyze all possible stock pairs.

To run the project you must first install and setup Riak.  You must set the backend to LevelDB and the port on the local host to 8087.  Instruction to do so can be found here:

http://docs.basho.com/riak/latest/downloads/
http://docs.basho.com/riak/latest/ops/advanced/backends/leveldb/

Additionally, you must have Spark and pyspark installed. Instructions to do so can be found here:

http://blog.cloudera.com/blog/2014/08/how-to-use-ipython-notebook-with-apache-spark/
http://ramhiser.com/2015/02/01/configuring-ipython-notebook-support-for-pyspark/

If you want to run the ipython notebook with spark you must enter this command: PYSPARK_DRIVER_PYTHON=ipython PYSPARK_DRIVER_PYTHON_OPTS="notebook" ./bin/pyspark

When ipython ntebook loads, a Spark Context is automatically created in a variable named sc.  You may need to install some package dependencies that the methods require:

sys
numpy
time
operator
pyspark
pandas
datetime
json
riak
urllib2
pytz
pandas
bs4
datetime
statsmodels

Once everything is installed and configured you can run the program.  Everything is assumed to be running locally.  Run time will vary depending on how many stock are analyzed.  For 100 stocks the total time is around 3-4 minutes.  For 3000 stocks the total run time will be around 8-10 hours.