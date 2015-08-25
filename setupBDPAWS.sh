fab -R allCluster riakStop
fab -R allCluster setupAll1
fab -R allCluster riakStart
fab -R slaves joinSlaves
fab -R master createCluster
fab -R master createMaps
fab -R allCluster restartRiak1
sleep 60
fab -R master addServices
sleep 3
fab -R master startMaster
fab -R slaves startWorker
