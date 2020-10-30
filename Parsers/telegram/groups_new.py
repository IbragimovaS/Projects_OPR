import configparser
from telethon.sync import TelegramClient
# для корректного переноса времени сообщений в json
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import GetHistoryRequest
import datetime

from itertools import dropwhile, takewhile
import pytz
import ibm_db_dbi as db
import time
utc=pytz.UTC
# Считываем учетные данные
config = configparser.ConfigParser()
config.read("config_for_groups_new.ini")

# Присваиваем значения внутренним переменным
api_id = '1871291'
api_hash = 'd60adb276ae0fcc6d4fe415bc65d0636'
username = 'samal_li'


#Создадим объект клиента Telegram API:
client = TelegramClient(username, api_id, api_hash)
client.start()

# url = input("Введите ссылку на канал или чат: ")
url = ["JYSANpartiasy",
       'oqchat',
       'I2eDehazwHKFie6ylV2z6Q',
       "@dvkYUG",
       "@dvkSEVER",
       "@dvkZAPAD"
       ]

#"@dvkSEVER","@dvkZAPAD"
all_messages_full = []

def insert_into_postgres(m):
    connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
    con = db.connect(connection_text, "", "")
    cur = con.cursor()
    full = client(GetFullUserRequest(m['from_id']))
    print(full)
    try:
        channel_id = m['to_id']['chat_id']
    except:
        channel_id = None
    object_id = m['id']

    published_date = m['date']
    likes = 0
    try:
        text = m['message']
        text = text.encode(encoding='UTF-8', errors='strict')
    except:
        text = 'WEX_ERROR'
    author_id = full.user.id
    author_name = str(full.user.first_name) + ' ' + str(full.user.last_name)
    author_name = author_name.encode(encoding='UTF-8', errors='strict')
    author_url = full.user.username
    parent_id = m['reply_to_msg_id']
    source_id = 5

    sql_data_update = """ update TL_MEDIA_GROUPS_TGM_2W set comment_likes = %s, comment_text = %s, author_name = %s where object_id= \'%s\' and author_id=\'%s\'"""
    sql_data_select = """select id from TL_MEDIA_GROUPS_TGM_2W where object_id = \'%s\' and author_id=\'%s\'"""
    sql_data_insert = """insert into TL_MEDIA_GROUPS_TGM_2W (comment_id, object_id, published_date, comment_likes, comment_text, author_id, author_name, author_url, parent_id, source_id) values (?,?,?,?,?,?,?,?,?,?)"""
    cur.execute("""select id from TL_MEDIA_GROUPS_TGM_2W where object_id = \'{}\' and author_id=\'{}\'""".format(object_id, author_id))
    # print(sql_data_select, (object_id, author_id))
    one_row = cur.fetchone()
    print(one_row)
    if one_row is not None:
        print('Updated - ', object_id, ' object')
        #cur.execute(sql_data_update, (likes, text, author_name, object_id, channel_id))
    else:
        print('Inserted - ', object_id, ' object')
        print(sql_data_insert,(channel_id,object_id, published_date, likes, text, author_id, author_name, author_url, parent_id, source_id))
        cur.execute(sql_data_insert,(channel_id,object_id, published_date, likes, text, author_id, author_name, author_url, parent_id, source_id))
    con.commit()
    cur.close()
    con.close()
def dump_all_messages(channel):
    offset_msg = 0
    limit_msg = 1000
    all_messages = []
    total_messages = 0
    total_count_limit = 0
    while True:
        history = client(GetHistoryRequest(peer=channel, offset_id=offset_msg, offset_date=None, add_offset=0, limit=limit_msg,max_id=0, min_id=0, hash=0))
        if not history.messages:
            break
        messages = history.messages
        SINCE = datetime.datetime.now()
        UNTIL = SINCE - datetime.timedelta(days=15)
        print(SINCE, '  ', UNTIL)

        for message in takewhile(lambda p: p.date > utc.localize(UNTIL), dropwhile(lambda p: p.date > utc.localize(SINCE), messages)):
            # print(message.message)
            insert_into_postgres(message.to_dict())
            time.sleep(1)
            all_messages_full.append(message.to_dict())
            all_messages.append(message.to_dict())
        time.sleep(1)
        offset_msg = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        # print(total_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

for i in url:

    channel = client.get_entity(i)
    messages = dump_all_messages(channel)

