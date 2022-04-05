import datetime

with open("/home/user/PycharmProjects/crontab/Output.txt", 'a') as the_file:
    the_file.write("Write at: %s" %datetime.datetime.now() + '\n')

the_file.close()
