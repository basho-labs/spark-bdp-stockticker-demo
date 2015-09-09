import boto, urllib2
from   boto.ec2 import connect_to_region
from   fabric.api import env, run, cd, settings, sudo, put
from   fabric.api import parallel
import os
import sys


REGION       = os.environ.get("AWS_EC2_REGION")
env.user      = "ubuntu"
env.key_filename = ["insertKeyPair.pem"]
accessKey = 'insertAccessKey'
secretKey = 'insertSecretKey'
conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=accessKey,aws_secret_access_key=secretKey)
reservations = conn.get_all_instances()

awsIPs = []
awsMicro = []
allHosts = []
awsMedium = []
awsClusterIPs = []

instances = [i for r in conn.get_all_instances() for i in r.instances]
for i in instances:
	if str(i.__dict__['_state']) == 'running(16)':
		allHosts.append(str(i.__dict__['dns_name']))
		awsIPs.append(str(i.__dict__['private_ip_address']))

		if str(i.__dict__['instance_type']) == 't2.medium':
			awsMedium.append(str(i.__dict__['dns_name']))
			awsClusterIPs.append(str(i.__dict__['private_ip_address']))

		if str(i.__dict__['instance_type']) == 't2.micro':
			awsMicro.append(str(i.__dict__['dns_name']))


if len(awsMedium) > 1:
	env.roledefs = {
	    'master': [awsMedium[0]],
	    'slaves': awsMedium[1:len(awsMedium)],
	    'worker': [awsMedium[1]],
	    'all': allHosts,
	    'allCluster': awsMedium,
	    'launcher': awsMicro
	} 
else:
	env.roledefs = {
	    'all': allHosts,
	    'allCluster': allHosts,
	    'launcher': awsMicro
	} 


@parallel
def test():
	sudo('hostname -I')

def checkServices():
	sudo('data-platofrm-admin services')

@parallel
def riakPing():
	sudo('riak ping')

@parallel
def restartRiak():
	with settings(warn_only=True):
		sudo('riak restart')
@parallel
def restartRiak1():
	with settings(warn_only=True):
		sudo('riak stop')
		sudo('riak start')

def clusterStatus():
	with settings(warn_only=True):
		sudo('riak-admin cluster status')

def ensembleStatus():
	with settings(warn_only=True):
		sudo('riak-admin ensemble-status')	

@parallel
def riakStop():
	with settings(warn_only=True):
		sudo('riak stop')

@parallel
def riakStart():
	with settings(warn_only=True):
		sudo('riak start')

@parallel
def setupAll1():
	#sudo('riak stop')
	sudo('sed -i -e "s/127\.0\.0\.1/`hostname -I | tr -d \'[[:space:]]\'`/" /etc/riak/riak.conf')
	sudo('sed -i -e "s/## listener.leader_latch.internal =/listener.leader_latch.internal =/g" /etc/riak/riak.conf')
	sudo('riak-admin reip riak@127.0.0.1 riak@`hostname -I`')
	#sudo('bash -c "echo "SPARK_MASTER_IP="`hostname -I` >> /usr/lib/riak/lib/data_platform-1/priv/spark-master/conf/spark-env.sh"')
	#sudo('riak start')

@parallel
def setupAll2():
	with settings(warn_only=True):
		ip = sudo('hostname -I')
		sudo('riak stop')
		sudo('bash -c "echo "SPARK_MASTER_IP="'+ip+' >> /usr/lib/riak/lib/data_platform-1/priv/spark-master/conf/spark-env.sh"')
		sudo('bash -c "echo "SPARK_MASTER_IP="'+ip+'  >> /usr/lib/riak/lib/data_platform-1/priv/spark-worker/conf/spark-env.sh"')
		sudo('bash -c "echo "SPARK_MASTER_IP="'+ip+'  >> /usr/lib/riak/lib/data_platform-1/priv/spark/conf/spark-env.sh"')
		sudo('riak start')

@parallel
def setupAll3():
	with settings(warn_only=True):
		ip = sudo('hostname -I')

		sudo('riak stop')
		sudo('bash -c "echo \'SPARK_MASTER_IP='+ip+' >> /usr/lib/riak/lib/data_platform-1/priv/spark-master/conf/spark-env.sh\'"')
		sudo('sed --in-place=bak \'s/distributed_cookie = .*/distributed_cookie = riak_bdp/\' /etc/riak/riak.conf')
		sudo('sed --in-place=bak \'s/nodename = .*/nodename = riak@'+ip+'/\' /etc/riak/riak.conf')
		sudo('sed --in-place=bak \'s/listener.http.internal = .*/listener.http.internal = 0.0.0.0:8098/\' /etc/riak/riak.conf')
		sudo('sed --in-place=bak \'s/listener.protobuf.internal = .*/listener.protobuf.internal = 0.0.0.0:8087/\' /etc/riak/riak.conf')
		 
		sudo('bash -c "echo \'handoff.ip = 0.0.0.0\' >> /etc/riak/riak.conf"')
		sudo('bash -c "echo \'listener.leader_latch.internal = '+ip+':5323\' >> /etc/riak/riak.conf"')
		sudo('bash -c "echo \'listener.leader_latch.external = '+ip+':15323\' >> /etc/riak/riak.conf"')

		
		sudo('riak-admin reip riak@127.0.0.1 riak@`hostname -I`')
		sudo('riak start')

@parallel
def riakreip():
	with settings(warn_only=True):
		sudo('riak stop')
		sudo('riak-admin reip riak@127.0.0.1 riak@`hostname -I`')
		sudo('riak start')

@parallel
def joinSlaves():
	sudo('riak-admin cluster join riak@' + awsClusterIPs[0])

def createCluster():
	with settings(warn_only=True):
		sudo('riak-admin cluster plan')
		sudo('riak-admin cluster commit')
		sudo('riak-admin cluster status')

def createMaps():
	with settings(warn_only=True):
		sudo('riak-admin bucket-type create strong \'{"props":{"consistent":true}}\'')
		sudo('riak-admin bucket-type create maps \'{"props":{"datatype":"map"}}\'')
		sudo('riak-admin bucket-type activate maps')
		sudo('riak-admin bucket-type status maps')

def addServices():
	with settings(warn_only=True):
		LLSH = ''
		for i in awsClusterIPs:
			LLSH = LLSH + str(i) + ':5323,'
		LLSH = LLSH.rstrip(',')

		RH = ''
		for i in awsClusterIPs:
			RH = RH + str(i) + ':8087,'
		RH = RH.rstrip(',')

		sudo('data-platform-admin add-service-config my-spark-master spark-master \
		HOST="'+awsClusterIPs[0]+'" \
		LEAD_ELECT_SERVICE_HOSTS="'+LLSH+'"\
		RIAK_HOSTS="'+RH+'"')
		
		sudo('data-platform-admin add-service-config my-spark-worker spark-worker MASTER_URL="spark://'+awsClusterIPs[0]+':7077" SPARK_WORKER_PORT=8081')

@parallel
def deleteSparkEnv():
	sudo('rm /usr/lib/riak/lib/data_platform-1/priv/spark/conf/spark-env.sh')
	sudo('rm /usr/lib/riak/lib/data_platform-1/priv/spark-master/conf/spark-env.sh')
	sudo('rm /usr/lib/riak/lib/data_platform-1/priv/spark-worker/conf/spark-env.sh')

def checkServices():
	sudo('data-platform-admin services')

@parallel
def riakEnsemble():
	with settings(warn_only=True):
		sudo('echo "riak_ensemble_manager:enable()." | sudo riak attach')

def startMaster():
	ip = sudo('hostname -I')
	sudo('data-platform-admin start-service riak@'+ip+' my-spark-group my-spark-master')

@parallel
def startWorker():
	ip = sudo('hostname -I')
	sudo('data-platform-admin start-service riak@'+ip+' my-spark-group my-spark-worker')

@parallel
def stopMaster1():
	with settings(warn_only=True):
		ip = sudo('hostname -I')
		sudo('data-platform-admin stop-service riak@'+ip+' my-spark-group my-spark-master')
		sudo('pkill -f master')
@parallel
def stopWorker1():
	with settings(warn_only=True):
		ip = sudo('hostname -I')
		sudo('data-platform-admin stop-service riak@'+ip+' my-spark-group my-spark-worker')
		sudo('pkill -f worker')

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
		--master spark://'+awsIPs[0]+':7077 \
		--py-files /home/ubuntu/deploy/pair.py \
         /home/ubuntu/deploy/riak-spark.py')

def runUpdate():
	sudo('/usr/lib/riak/lib/data_platform-1/priv/spark-worker/bin/spark-submit  \
		--master spark://'+awsIPs[0]+':7077 \
		--py-files /home/ubuntu/deploy/pair.py \
           /home/ubuntu/deploy/updateData.py ')

def runPopulate():
    sudo('/usr/lib/riak/lib/data_platform-1/priv/spark-worker/bin/spark-submit  \
         --master spark://'+awsIPs[0]+':7077 \
         --py-files /home/ubuntu/deploy/pair.py \
         /home/ubuntu/deploy/downloadStocks.py ')

def testSparkCluster():
	with settings(warn_only=True):
		sudo('/usr/lib/riak/lib/data_platform-1/priv/spark-worker/bin/spark-submit \
			--master spark://'+awsIPs[0]+':7077 \
			/usr/lib/riak/lib/data_platform-1/priv/spark-worker/examples/src/main/python/pi.py 100')









