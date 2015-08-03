#!/usr/bin/python
from crontab import CronTab
#init cron
cron   = CronTab()

#add new cron job
job  = cron.new(command='python /home/ubuntu/deploy/run.py')

#job settings
job.hour().on(1)
job.minute().on(0)
job.enable()
cron.write()
