import configparser
from telethon.sync import TelegramClient
# для корректного переноса времени сообщений в json
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import GetHistoryRequest
import datetime
import sys
import psycopg2
from itertools import dropwhile, takewhile
import pytz
import ibm_db_dbi as db

utc=pytz.UTC
# Считываем учетные данные
config = configparser.ConfigParser()
config.read("config.ini")

# Присваиваем значения внутренним переменным
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']

#Создадим объект клиента Telegram API:
client = TelegramClient(username, api_id, api_hash)
client.start()

url = input("Введите ссылку на канал или чат: ")
# url = sys.argv[1]
channel = client.get_entity(url)
print(channel)
all_messages_full = []

def insert_into_db2():
    connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
    con = db.connect(connection_text, "", "")
    cursor = con.cursor()
    for m in all_messages_full:
        full = client(GetFullUserRequest(m['from_id']))
        print(full)
        channel_id = m['to_id']['chat_id']
        object_id = m['id']
        published_date = m['date']
        likes = 0
        text = m['message']
        author_id = full.user.id
        author_name = str(full.user.first_name) + ' ' + str(full.user.last_name)
        author_url = full.user.username
        parent_id = m['reply_to_msg_id']
        source_id = 5

        sql_data_update = """ update tl_media_groups_tgm set likes = %s, text = %s where object_id= \'%s\' and channel_id=\'%s\'"""
        sql_data_select = """select id from tl_media_groups_tgm where object_id = \'%s\' and channel_id=\'%s\'"""
        sql_data_insert = """insert into tl_media_groups_tgm (channel_id, object_id, published_date, likes, text, author_id, author_name, author_url, parent_id, source_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor.execute(sql_data_select, (object_id, channel_id))
        one_row = cursor.fetchone()
        if one_row is not None:
            print('Updated - ', object_id, ' object')
            cursor.execute(sql_data_update, (likes, text, object_id, channel_id))
        else:
            print('Inserted - ', object_id, ' object')
            print(sql_data_insert, (channel_id, object_id, published_date, likes, text, author_id, author_name, author_url, parent_id, source_id))
            cursor.execute(sql_data_insert, (channel_id, object_id, published_date, likes, text, author_id, author_name, author_url, parent_id, source_id))
        con.commit()
    cursor.close()
    con.close()
def insert_into_postgres():
    conn = psycopg2.connect(dbname='postgres', user='postgres', password='Mukanov13', host='localhost')
    cur = conn.cursor()
    for m in all_messages_full:
        object_id = m['id']
        published_date = m['date']
        channel_id = m['to_id']['channel_id']
        likes = 0
        comments = 0
        views = m['views']
        reposts = 0
        caption = ''
        text = m['message']
        try:
            url_attachment = m['media']
            try:
                url_attachment = m['media']['url']
            except:
                url_attachment = ''
        except TypeError:
            url_attachment = ''
        url_channel = '@' + str(channel.username)
        source_id = 5

        sql_data_update = """ update tl_media_channels_tgm set views = %s, text = %s where object_id= \'%s\' and channel_id=\'%s\'"""
        sql_data_select = """select id from tl_media_channels_tgm where object_id = \'%s\' and channel_id=\'%s\'"""
        sql_data_insert = """insert into tl_media_channels_tgm (object_id, published_date, channel_id, likes, comments, views, reposts, caption, text, url_attachment, url_channel,source_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cur.execute(sql_data_select, (object_id, channel_id))
        one_row = cur.fetchone()
        if one_row is not None:
            print('Updated - ', object_id, ' object')
            cur.execute(sql_data_update, (views, text, object_id, channel_id))
        else:
            print('Inserted - ', object_id, ' object')
            cur.execute(sql_data_insert, (object_id, published_date, channel_id, likes, comments, views, reposts, caption, text, url_attachment, url_channel, source_id))
        conn.commit()
    cur.close()
    conn.close()
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
        UNTIL = SINCE - datetime.timedelta(days=10)
        print(SINCE, '  ', UNTIL)

        for message in takewhile(lambda p: p.date > utc.localize(UNTIL), dropwhile(lambda p: p.date > utc.localize(SINCE), messages)):
            print(message.date)
            all_messages_full.append(message.to_dict())
            all_messages.append(message.to_dict())

        offset_msg = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        # print(total_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

dump_all_messages(channel)
insert_into_postgres()
