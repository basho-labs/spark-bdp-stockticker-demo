#!/usr/bin/python
import boto, urllib2
from   boto.ec2 import connect_to_region
import os
import sys
import time


accessKey = 'AKIAIGBU3O2I45SZV57A'
secretKey = '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'
conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=accessKey,aws_secret_access_key=secretKey)
instances = [i for r in conn.get_all_instances() for i in r.instances]

#start all non running instances
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
