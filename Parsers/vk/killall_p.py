import requests
import os
import random
import datetime
def telegram_bot_sendtext(bot_message):
    bot_token = '948653580:AAEv9Ucp3MH6T3L_DB94Laekdnl-NQBKyzI'
    bot_chatID = '91344390'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()
text = 'sudo /home/dastrix/Vk/killall_python3.sh'
#text = 'echo HELLO WORLD'
telegram_bot_sendtext("All VKandTGM parsers were terminated")
os.system(text)

