import boto, urllib2
from   boto.ec2 import connect_to_region
from   fabric.api import env, run, cd, settings, sudo, put
from   fabric.api import parallel
import os
import sys


REGION       = os.environ.get("AWS_EC2_REGION")
env.user      = "ubuntu"
env.key_filename = ["kp1.pem"]

accessKey = 'AKIAIGBU3O2I45SZV57A'
secretKey = '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'
conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=accessKey,aws_secret_access_key=secretKey)
reservations = conn.get_all_instances()
awsHosts = []
awsIPs = []
awsMicro = []
allHosts = []
instances = [i for r in conn.get_all_instances() for i in r.instances]
for i in instances:
	if str(i.__dict__['_state']) == 'running(16)':
		allHosts.append(str(i.__dict__['dns_name']))
		if str(i.__dict__['instance_type']) == 't2.medium':
			awsHosts.append(str(i.__dict__['dns_name']))
			awsIPs.append(str(i.__dict__['private_ip_address']))
		if str(i.__dict__['instance_type']) == 't2.micro':
			awsMicro.append(str(i.__dict__['dns_name']))


if len(awsHosts) > 1:
	env.roledefs = {
	    'master': [awsHosts[0]],
	    'slaves': awsHosts[1:len(awsHosts)],
	    'worker': [awsHosts[1]],
	    'all': allHosts,
	    'allCLuster': awsHosts,
	    'launcher': awsMicro
	} 
else:
	env.roledefs = {
	    'all': allHosts,
	    'allCLuster': awsHosts,
	    'launcher': awsMicro
	} 

def test():
	sudo('hostname -I')

@parallel
def setupAll1():
	sudo('riak stop')
	sudo('sed -i -e "s/127\.0\.0\.1/`hostname -I | tr -d \'[[:space:]]\'`/" /etc/riak/riak.conf')
	sudo('sed -i -e "s/## listener.leader_latch.internal =/listener.leader_latch.internal =/g" /etc/riak/riak.conf')
	sudo('riak-admin reip riak@127.0.0.1 riak@`hostname -I`')
	sudo('bash -c "echo "SPARK_MASTER_IP="`hostname -I` >> /usr/lib/riak/lib/data_platform-1/priv/spark-master/conf/spark-env.sh"')
	sudo('riak start')

@parallel
def joinSlaves():
	sudo('riak-admin cluster join riak@' + awsIPs[0])

def createCluster():
	sudo('riak-admin cluster plan')
	sudo('riak-admin cluster commit')
	sudo('riak-admin cluster status')

@parallel
def riakEnsemble():
	sudo('echo "riak_ensemble_manager:enable()." | sudo riak attach')

def bucketSetup():
	sudo('riak-admin bucket-type create strong \'{"props":{"consistent":true}}\'')
	sudo('riak-admin bucket-type create maps \'{"props":{"datatype":"map"}}\'')
	sudo('riak-admin bucket-type activate maps')
	sudo('riak-admin bucket-type status maps')

def addMasterService():
	sudo('data-platform-admin add-service-config my-spark-master spark-master\
	 LEAD_ELECT_SERVICE_HOSTS="'+awsIPs[0]+':5323,'+awsIPs[1]+':5323,'+awsIPs[2]+':5323,'+awsIPs[3]+':5323" \
	 RIAK_HOSTS="'+awsIPs[0]+':8087,'+awsIPs[1]+':8087,'+awsIPs[2]+':8087,'+awsIPs[3]+':8087"')

def addWorkerService():
	sudo('data-platform-admin add-service-config my-spark-worker spark-worker MASTER_URL="spark://'+awsIPs[0]+':7077"')


def startMaster():
	ip = sudo('hostname -I')
	sudo('data-platform-admin start-service riak@'+ip+' my-spark-group my-spark-master')

@parallel
def startWorker():
	ip = sudo('hostname -I')
	sudo('data-platform-admin start-service riak@'+ip+' my-spark-group my-spark-worker')

@parallel
def stopMaster():
	with settings(warn_only=True):
		ip = sudo('hostname -I')
		sudo('data-platform-admin stop-service riak@'+ip+' my-spark-group my-spark-master')
		sudo('pkill -f deploy.master.Master')
@parallel
def stopWorker():
	with settings(warn_only=True):
		ip = sudo('hostname -I')
		sudo('data-platform-admin stop-service riak@'+ip+' my-spark-group my-spark-master')
		sudo('pkill -f deploy.worker.Worker')

@parallel
def deployCode():
	print env.host
	put('/Users/korriganclark/Desktop/bdptest/deploy','/home/ubuntu/')


@parallel
def deleteCode():
	print env.host
	sudo('rm -rf /home/ubuntu/deploy')


def runAnalysis():
	sudo('/usr/lib/riak/lib/data_platform-1/priv/spark-worker/bin/spark-submit  \
		/home/ubuntu/deploy/riak-spark.py \
		--master spark://'+awsIPs[0]+':7077 \
		--py-files /home/ubuntu/deploy/pair.py')

def runUpdate():
	sudo('/usr/lib/riak/lib/data_platform-1/priv/spark-worker/bin/spark-submit  \
		/home/ubuntu/deploy/updateData.py \
		--master spark://'+awsIPs[0]+':7077 \
		--deploy-mode cluster \
		--py-files /home/ubuntu/deploy/pair.py')








