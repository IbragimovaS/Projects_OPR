import datetime

with open("/home/sholpan/PycharmProjects/crontab/Output.txt", 'a') as the_file:
    the_file.write("Write at: %s" %datetime.datetime.now() + '\n')

the_file.close()