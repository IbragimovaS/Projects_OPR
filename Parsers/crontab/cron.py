from crontab import CronTab


my_cron = CronTab(user='sholpan')
job = my_cron.new(command='python3 /home/sholpan/PycharmProjects/crontab/instagram.py \'1626737751\' 01.12.19 01.09.19')
job.minute.every(5)


#my_cron.remove(job)
my_cron.write()
# for job in my_cron:
# iter = my_cron.find_command('python3')
# for i in iter:
#     print(i, '\n')