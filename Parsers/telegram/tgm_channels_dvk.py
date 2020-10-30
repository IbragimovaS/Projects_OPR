import configparser
from telethon.sync import TelegramClient
# для корректного переноса времени сообщений в json
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import GetHistoryRequest
import datetime

from itertools import dropwhile, takewhile
import pytz
import ibm_db_dbi as db

utc=pytz.UTC
# Считываем учетные данные
config = configparser.ConfigParser()
config.read("config.ini")

# Присваиваем значения внутренним переменным
api_id = '1756553'
api_hash = '2a6307df5f16aeff8d99b6bafb75f9d0'
username = 'super_mama01'

#Создадим объект клиента Telegram API:
client = TelegramClient(username, api_id, api_hash)
client.start()

url = [ "@yelikbayev",
        "@geo_2019",
        "@nonamestan",
        "@kaliyevchannel",
        "@selteyev_view",
        "@aqsaqalkz",
        "@nehabar",
        "@strashniyzhuz",
        "@strogiiagashka",
        "@yyedilov",
        "@Zanamiviehali",
        "@Kazdvk",
        "@oyanqazaqstan",
        "@alibekovkz",
        "@yrashev",
        "@FINANCEkaz",
        "@hommeskz",
        "@balgabaev_rinat",
        "@thetechkz"]#input("Введите ссылку на канал или чат: ")
# url = sys.argv[1]


def insert_into_postgres(m):
    connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
    con = db.connect(connection_text, "", "")
    cur = con.cursor()

    object_id = m['id']
    published_date = m['date']
    channel_id = m['to_id']['channel_id']
    likes = 0
    comments = 0
    print(m)
    try:
        views = m['views']
    except:
        views = 0
    reposts = 0
    caption = ''
    try:
        text = m['message']
        text = text.encode(encoding='UTF-8', errors='strict')
    except:
        text = "WEX_ERROR"
    try:
        url_attachment = m['media']
        try:
            url_attachment = m['media']['url']
        except:
            url_attachment = ''
    except TypeError:
        url_attachment = ''
    except:
        url_attachment = None
    try:
        url_channel = '@' + str(channel.username)
    except:
        url_channel = None
    print(url_channel)
    source_id = 5

    sql_data_update = """ update TL_MEDIA_CHANNELS_TGM_2W set views = %s, text = %s where object_id= \'%s\' and channel_id=\'%s\'"""
    sql_data_select = """select id from TL_MEDIA_CHANNELS_TGM_2W where object_id = \'%s\' and channel_id=\'%s\'"""
    sql_data_insert = """insert into TL_MEDIA_CHANNELS_TGM_2W (object_id, published_date, channel_id, likes, comments, views, reposts, caption, text, url_attachment, url_channel,source_id) values (?,?,?,?,?,?,?,?,?,?,?,?)"""
    print(cur.execute( """select id from TL_MEDIA_CHANNELS_TGM_2W where object_id = \'{}\' and channel_id=\'{}\'""".format(object_id, channel_id)))
    print("""select id from TL_MEDIA_CHANNELS_TGM_2W where object_id = \'{}\' and channel_id=\'{}\'""".format(object_id, channel_id))
    one_row = cur.fetchone()
    print(one_row)
    if one_row is None:
        print('Inserted - ', object_id, ' object')
        cur.execute(sql_data_insert, (object_id, published_date, channel_id, likes, comments, views, reposts, caption, text, url_attachment, url_channel, source_id))
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
        UNTIL = SINCE - datetime.timedelta(days=5)
        print(SINCE, '  ', UNTIL)

        for message in takewhile(lambda p: p.date > utc.localize(UNTIL), dropwhile(lambda p: p.date > utc.localize(SINCE), messages)):
            print(message.date)
            all_messages_full.append(message.to_dict())
            all_messages.append(message.to_dict())
            print(message.to_dict())
            insert_into_postgres(message.to_dict())

        offset_msg = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        # print(total_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break
for i in url:
    channel = client.get_entity(i)
    print(channel)
    all_messages_full = []
    print(channel.username)
    dump_all_messages(channel)

#insert_into_postgres()
