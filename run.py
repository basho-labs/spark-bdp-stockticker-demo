#!/usr/bin/python
import boto, urllib2
from   boto.ec2 import connect_to_region
import os
import sys
import time
from pair import *


accessKey = 'AKIAIGBU3O2I45SZV57A'
secretKey = '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'
conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=accessKey,aws_secret_access_key=secretKey)
instances = [i for r in conn.get_all_instances() for i in r.instances]

#start all non running instances
for i in instances:
	if i.state != 'terminated' and i.instance_type == 't2.medium':
		conn.start_instances(i.__dict__['id'])

for i in instances:
	if i.state != 'terminated' and i.instance_type == 't2.medium':
		while i.state != 'running':
			print 'waiting for: ' + str(i.id)
	   		time.sleep(2)
	   		i.update()
		print str(i.id)+ ' is running'

time.sleep(40)

awsHosts = []
awsIPs = []
instances = [i for r in conn.get_all_instances() for i in r.instances]

for i in instances:
	if str(i.__dict__['_state']) == 'running(16)' and i.instance_type == 't2.medium':
		awsHosts.append(str(i.__dict__['dns_name']))
		awsIPs.append(str(i.__dict__['private_ip_address']))


riakIP = awsIPs[0]
print str(riakIP)
os.system('sudo /usr/lib/riak/lib/data_platform-1/priv/spark-worker/bin/spark-submit  \
    /home/ubuntu/deploy/updateData.py \
    --master spark://'+str(riakIP)+' \
    --deploy-mode cluster \
    --py-files /home/ubuntu/deploy/pair.py')

os.system('sudo /usr/lib/riak/lib/data_platform-1/priv/spark-worker/bin/spark-submit  \
    /home/ubuntu/deploy/riak-spark.py \
    --master spark://'+str(riakIP)+' \
    --deploy-mode cluster \
    --py-files /home/ubuntu/deploy/pair.py')


newUpdate = getValue("meta", "update", riakIP)#get date of last update from riak
print newUpdate

for i in instances:
	if i.state != 'terminated' and i.instance_type == 't2.medium':
		conn.stop_instances(i.__dict__['id'])

for i in instances:
	if i.state != 'terminated' and i.instance_type == 't2.medium':
		while i.state != 'stopped':
			print 'waiting for: ' + str(i.id)
	   		time.sleep(2)
	   		i.update()
		print str(i.id)+ ' is stopped'
