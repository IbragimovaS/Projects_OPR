import requests
import os
import random

def telegram_bot_sendtext(bot_message):
    bot_token = '971984391:AAEvv1mXexkqTy4ujlv1Jg_LYAo8oGAZNFE'
    bot_chatID = '91344390'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()
text = 'sudo /home/dastrix/parsers_insta/openvpn_control.sh'
os.system(text)
vpn_country = os.listdir("/home/dastrix/config/")

rand_item = str("\'" + vpn_country[random.randrange(len(vpn_country))] + "\'")

#path = '/home/sholpan/Desktop/other_os/' + rand_item
#print(path)
test = ' sudo /home/dastrix/parsers_insta/random_city.sh '
os.system(test + rand_item)
telegram_bot_sendtext(rand_item + " Connected")
