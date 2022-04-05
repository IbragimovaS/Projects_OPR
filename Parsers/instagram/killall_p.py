import requests
import os
import random
import datetime
def telegram_bot_sendtext(bot_message):
    bot_token = '########TOKEN#######'
    bot_chatID = '#########CHAT ID#########'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()
text = 'sudo /home/user/parsers_insta/killall_python3.sh'
telegram_bot_sendtext("All parsers were terminated")
os.system(text)

