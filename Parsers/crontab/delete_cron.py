from crontab import CronTab


my_cron = CronTab(user='sholpan')
for job in my_cron:
    if job.command == 'python3 /home/sholpan/PycharmProjects/crontab/instagram.py \'1626737751\' 01.12.19 01.09.19':
        my_cron.remove(job)
        my_cron.write()