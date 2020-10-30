# -*- coding: utf-8 -*-
import requests
import time
import datetime
import ibm_db_dbi as db
import pandas
import re

def telegram_bot_sendtext(bot_message):
    bot_token = '873733574:AAFmyXY_cvkErr9YpYNw7Qf3Y87d1Sjpgg4'
    bot_chatID = '91344390'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()

def get_channels():
    connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
    con = db.connect(connection_text, "", "")
    cursor = con.cursor()
    sql = 'select channel_id from tl_media_channels where source_id = 1 order by id'
    df = pandas.read_sql(sql, con)

    df['channel_id_wa'] = df['CHANNEL_ID'].str.replace("'", "")
    df1 = df['channel_id_wa'].values.tolist()

    channels_from_db = []
    #print("___________",count)
    for r in df1:
        print(r)
        channels_from_db.append(r)
    return channels_from_db

def getjson(url, data=None):
    response = requests.get(url, params=data)
    print(response.url, '\n')
    return response.json()


# 1546300800  1551398400    1554854400  	1556668800  1557273600  1556712000
def get_posts_count(access_token, owner_id):

    time.sleep(0.6)
    wall = getjson("https://api.vk.com/method/wall.get",
                   {'owner_id': owner_id,
                    'extended': '0',
                    'access_token': access_token,
                    'v': '5.95'
                    })
    time.sleep(0.6)
    group_id=owner_id.replace("-","")
    group = getjson("https://api.vk.com/method/groups.getMembers",
                   {'group_id': group_id,
                    'fields': 'bdate, city',
                    'access_token': access_token,
                    'v': '5.95'
                    })

    posts_count = wall['response']['count']
    number_of_followers = group['response']['count']
    inserted_date = datetime.datetime.now()

    #print(number_of_followers, posts_count, inserted_date, "---------------------")

    connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
    con = db.connect(connection_text, "", "")
    cursor = con.cursor()

    cursor.execute('select 1 from tl_media_channels where channel_id=\'\'\'{channel_id}\'\'\' and source_id=1'.format(channel_id=owner_id))
    one_row = cursor.fetchone()
    if one_row is not None:
        print('Update {channel_id} channel.'.format(channel_id=owner_id), datetime.datetime.now())

        sql_update = "update tl_media_channels set posts_count={posts_count}, number_of_followers={number_of_followers} where channel_id = \'\'\'{channel_id}\'\'\' and source_id=1".format(
            posts_count=posts_count, number_of_followers=number_of_followers, channel_id=owner_id)

        sql_history = "insert into tl_media_channels_history (inserted_date, channel_id, number_of_followers, posts_count, source_id) values (?,?,?,?,?)"

        cursor.execute(sql_history, (inserted_date, "\'" + owner_id + "\'", number_of_followers, posts_count, 1))

        cursor.execute(sql_update)

    else:
        print('Insert ', owner_id, 'object.')

        sql = "insert into tl_media_channels (channel_id, number_of_followers, posts_count, source_id) values (?,?,?,?)"

        cursor.execute(sql, ("\'" + owner_id + "\'", number_of_followers, posts_count, 1))

    con.commit()
    cursor.close()
    con.close()

access_token = '47c1f1c5426dae177fd25f3ecf36d07ac043711298452dc9cfe1e9f8e9c096bed5e32ecfa90916f26ceac'

# owners = {
#     '-78866709', '-63107488', '-85851456', '-25369917', '-95028087', '-62657007', '-28857956',
#     '-24894771', '-23860985', '-47049092', '-24690318', '-128906097', '-80133701', '-70436737',
#     '-82505072', '-62908829', '-73034921', '-26063271', '-67078897', '-58192442', '-89616553',
#     '-33156709','-9693056', '-31643537', '-8029171', '-101821296', '-130809599', '-31746395',
#     '-103280991', '-15561055', '-136218184', '-46347230', '-38024103', '-92691943', '-96627920',
#     '-74392755', '-112606725', '-32144563', '-50433089', '-49618068', '-61782075', '-27535035',
#     '-29539204', '-40603241', '-31297981', '-38257191', '-107268913', '-21723674', '-50021181',
#     '-61761744', '-25018942', '-40499944', '-59498363', '-21032493', '-132300543', '-88922585',
#     '-23741867', '-70670006', '-155575493', '-150609964', '-113180603', '-85533039', '-138312191',
#     '-45839574', '-38522918', '-118580942', '-171051653', '-45728398', '-153867463',
#     '-36610826', '-29213922', '-51301689', '-45953169', '-39461227', '-56345579', '-115351633',
#     '-46273797', '-79148382', '-107730657', '-78348128', '-48341171', '-49567599', '-4579565',
#     '-177243015', '-170894869', '-177765614', '-41879362',
#     '-91053278', '-129359322', '-78415652', '-99221650', '-35729072', '-83080205', '-34499952',
#     '-48760195', '-40137153', '-41438670',
#     '-58984516', '-51208183', '-52236923', '-59901774', '-68347204', '-35788661', '-35005429',
#     '-69230091', '-83188631', '-78866709', '-63107488', '-89760765', '-25743865', '-36597739',
#     '-183594655','-36622653','-108255381','-78657178', '-53550522'
#
# }
owners = {
    '-76802530', '-119266569', '-119088009', '-70932451', '-171583237', '-175576659'
}
bot_message = "UpdateChannelVK RUNNING" + "  " + str(datetime.datetime.now())
telegram_bot_sendtext(bot_message)
channels_from_db = get_channels()
print(channels_from_db)
try:
	for owner_id in owners:
	    get_posts_count(access_token, owner_id)

	connection_last = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
	con_last = db.connect(connection_last, "", "")
	cursor_last = con_last.cursor()

	sql_update_last = "update tl_media_channels_history th set channel_name = (select channel_name from tl_media_channels where channel_id=th.channel_id limit 1)," \
		          " biography = (select biography from tl_media_channels where channel_id=th.channel_id limit 1) where source_id=1"
	print(sql_update_last)
	cursor_last.execute(sql_update_last)

	con_last.commit()
	cursor_last.close()
	con_last.close()
	time.sleep(1800)
except:
	bot_message = "UpdateChannelVK ERROR" + "  " + str(datetime.datetime.now())
	telegram_bot_sendtext(bot_message)


















