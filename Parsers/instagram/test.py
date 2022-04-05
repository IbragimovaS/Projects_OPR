import requests
import os
import random
#telegram bot for notification
def telegram_bot_sendtext(bot_message):
    bot_token = '##########TOKEN##########'
    bot_chatID = '###########CHAT ID############'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()

text = 'sudo /home/user/parsers_insta/openvpn_control.sh'
os.system(text)
vpn_country = os.listdir("/home/user/config/")

rand_item = str("\'" + vpn_country[random.randrange(len(vpn_country))] + "\'")

test = ' sudo /home/user/parsers_insta/random_city.sh '
os.system(test + rand_item)
telegram_bot_sendtext(rand_item + " Connected")
