# -*- coding: utf-8 -*-
import requests
import time
import datetime
import csv
import ibm_db_dbi as db
import re
import pandas

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
    sql = 'select channel_id from tl_media_channels where source_id = 1 order by id limit(6) offset(127)'
    df = pandas.read_sql(sql, con)

    df['channel_id_wa'] = df['CHANNEL_ID'].str.replace("'","")
    df1 = df['channel_id_wa'].values.tolist()
    print(df1)
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

#1546300800  1551398400    1554854400	1556668800 1557273600   1556712000   1557792000 1558742400

def get_date(date):
    new_date = date - datetime.timedelta(days=2)
    date_timestamp = datetime.datetime.timestamp(new_date)
    print("__________________________________", date_timestamp)
    return date_timestamp

def get_new_posts(access_token, owner_id, count=100, offset=0, date=get_date(datetime.datetime.now())):
    """"take access_token, owner_id (group_id), count (default=100), offset (default=0)
    and returns all posts from vk group in a list of dictionaries and the number of posts
    in second variable"""
    all_posts = []
    t = True
    while True:
        time.sleep(0.6)
        wall = getjson("https://api.vk.com/method/wall.get",
                       {'owner_id': owner_id,
                        'count': count,
                        'extended': '1',
                        'access_token': access_token,
                        'offset': offset,
                        'v': '5.95'
                        })

        count_posts = wall['response']['count']
        posts = wall['response']['items']
        group_name = wall['response']['groups'][0]['screen_name']
        print(group_name)
        for post in posts:
            if (post['date'] > date):
                all_posts.append(post)
            else:
                t = False

        parsed_posts_count = len(all_posts)

        if len(all_posts) >= count_posts:
            break
        if not t:
            break
        else:
            offset += 100
    return all_posts, count_posts, parsed_posts_count, group_name


def make_posts(all_posts):
    """takes in a list of dictionaries with posts, converts the data in a
    new structure and returns a new list of dictionaries with the posts"""
    filtered_data = []
    for post in all_posts:
        try:
            id = post['id']
        except:
            id = 0

        try:
            date = datetime.datetime.fromtimestamp(int(post['date'])).strftime('%d-%m-%Y-%H:%M:%S')
        except:
            date = ''

        try:
            timestamp = post['date']
        except:
            timestamp = ''

        try:
            likes = post['likes']['count']
        except:
            likes = 0

        try:
            reposts = post['reposts']['count']
        except:
            reposts = 0

        try:
            comments_count = post['comments']['count']
        except:
            comments_count = 0

        try:
            views = post['views']['count']
        except:
            views = 0

        try:
            text = post['text']
        except:
            text = 'WEX_ERROR'

        try:
            post_source = post['post_source']
        except:
            post_source = ''

        all_attachments = ''
        # att_str = ''
        try:
            attachments = post['attachments']

            if attachments:
                for att in attachments:

                    if att['type'] == 'video':
                        video_photo = att['video']['photo_800']['url']
                        print(video_photo)
                        all_attachments += video_photo + "\n"
                        # att_str += att + "\n"

                    if att['type'] == 'photo':
                        photo_url = att['photo']['sizes']
                        photo_url = photo_url[-1]
                        print(photo_url)
                        all_attachments += photo_url['url'] + "\n"

                    if att['type'] == 'link':
                        att_link = att['link']['url']
                        all_attachments += att_link[-1] + "\n"

        except:
            attachments = ''

        filtered_post = {
            'id': id,
            'date': date,
            'timestamp': timestamp,
            'likes': likes,
            'reposts': reposts,
            'comments_count': comments_count,
            'views': views,
            'text': text,
            'attachments': all_attachments,
            'post_source': post_source
        }
        filtered_data.append(filtered_post)
    return filtered_data


def get_user(user_id):
    time.sleep(0.5)
    user_info = getjson("https://api.vk.com/method/users.get",
                        {
                            'user_ids': str(user_id).replace("-", ""),
                            'access_token': access_token,
                            'fields': 'city, domain, bdate',
                            'v': '5.95'
                        }
                        )

    try:
        user_info = user_info['response'][0]

        try:
            FIO = user_info['first_name'] + " " + user_info['last_name']
        except:
            FIO = ""

        try:
            city = user_info['city']['title']
        except:
            city = ""

        try:
            bdate = user_info['bdate']
        except:
            bdate = ""

        try:
            url = "www.vk.com/" + user_info['domain']
        except:
            url = ""
    except:
        FIO = ""
        city = ""
        bdate = ""
        url = ""
    try:
        try:
            dtf = datetime.datetime.strptime(bdate, '%d.%m.%Y')
        except:
            try:
                dtf = datetime.datetime.strptime(bdate, '%d.%m')
            except:
                dtf = datetime.datetime.strptime(bdate, '')
    except:
        dtf = datetime.datetime.strptime('', '')

    bdate = dtf.strftime('%Y-%m-%d')

    return FIO, city, bdate, url


def get_new_comments(access_token, owner_id, post_id, count=100, offset=0, thread_items_count=10):
    """"take access_token, owner_id (group_id), post_id, count (default=100), offset (default=0), thread_items_count (default = 10)
        and returns all comments from vk group in a list of dictionaries and the number of comments
        in second variable"""
    all_comments = []
    all_profiles = []
    thread_items_counter = 0
    t = True
    while True:
        time.sleep(0.6)
        comment = getjson("https://api.vk.com/method/wall.getComments",
                          {
                              'owner_id': owner_id,
                              'post_id': post_id,
                              'count': count,
                              'access_token': access_token,
                              'offset': offset,
                              'extended': 1,
                              'thread_items_count': thread_items_count,
                              'v': '5.95'
                          }
                          )

        count_comments = comment['response']['count']
        comments = comment['response']['items']
        subcomments = []

        for com in comments:
            if com['thread']['count'] > 0:
                subcomments += (com['thread']['items'])
            if com['thread']['count'] > 10:
                thread_items_counter = thread_items_counter + (com['thread']['count']) - 10

        comments += subcomments
        all_profiles = comment['response']['profiles']
        all_comments += comments

        if len(all_comments) >= count_comments or (len(all_comments) < count_comments and len(subcomments) == 0):
            break
        if not t:
            break
        else:
            offset += 100
    return all_comments, count_comments, all_profiles


def make_comments(post_id, all_comments, all_profiles):
    """takes in a list of dictionaries with comments, converts the data in a
    new structure and returns a new list of dictionaries with the comments"""
    filtered_comments_total = []
    for comment in all_comments:
        try:
            id = comment['id']
        except:
            id = 0

        try:
            person_id = comment['from_id']
        except:
            person_id = 0

        try:
            date = datetime.datetime.fromtimestamp(int(comment['date'])).strftime('%d-%m-%Y-%H:%M:%S')
        except:
            date = ''

        try:
            timestamp = comment['date']
        except:
            timestamp = ''

        try:
            likes = comment['likes']['count']
        except:
            likes = 0

        try:
            text = comment['text']
        except:
            text = 'WEX_ERROR'

        """url = ''
        scr_name = ''
        try:
            for prof in all_profiles:
                if person_id == prof['id']:
                    try:
                        scr_name = prof['screen_name']
                        url = 'https://vk.com/' + scr_name
                    except:
                        scr_name = prof['id']
                        url = 'deleted'
        except:
            scr_name = ''
            url = ''"""

        scr_name, city, bdate, url = get_user(person_id)

        filtered_comments = {
            'id': id,
            'person_id': person_id,
            'date': date,
            'timestamp': timestamp,
            'likes': likes,
            'text': text,
            'post_id': post_id,
            'scr_name': scr_name,
            'url': url,
            'city': city,
            'bdate': bdate
        }
        filtered_comments_total.append(filtered_comments)
    return filtered_comments_total


def write_posts_csv(data, filename='/home/ruslan/PycharmProjects/posts.csv', encoding='utf-8'):
    """Recieves data as list of dictionaries, the file name as a string with format,
    encoding by default utf-8, returns csv file"""
    with open(filename, 'a', newline='', encoding=encoding) as csvfile:
        fieldnames = ['id', 'date', 'timestamp', 'likes', 'reposts', 'comments_count', 'views', 'text', 'attachments',
                      'post_source']

        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames, extrasaction='ignore')

        # writer.writeheader()

        writer.writerows(data)

        print('Data written to csv, filename')

    csvfile.close()


def write_comments_csv(data, filename='/home/ruslan/PycharmProjects/comments.csv', encoding='utf-8'):
    """Recieves data as list of dictionaries, the file name as a string with format,
    encoding by default utf-8, returns csv file"""
    with open(filename, 'a', newline='', encoding=encoding) as csvfile:
        fieldnames = ['id', 'person_id', 'date', 'timestamp', 'likes', 'text', 'post_id', 'url', 'scr_name']

        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames, extrasaction='ignore')

        # writer.writeheader()

        writer.writerows(data)

        print('Data written to csv, filename')

    csvfile.close()


def insert_into_db_test(posts, owner_id, group_name):
    # connection_text = "DATABASE=sample;HOSTNAME=192.168.2.23;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=1q2w3e4R;"
    connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
    con = db.connect(connection_text, "", "")
    cursor = con.cursor()
    for post in posts:
        # ['id','date','timestamp','likes','reposts','comments_count','views', 'text', 'attachments','post_source']
        object_id = post['id']
        cursor.execute(
            'select 1 from tl_media_data_vk where object_id = \'{object_id}\' and channel_id=\'\'\'{channel_id}\'\'\''.format(object_id=object_id, channel_id=owner_id))
        one_row = cursor.fetchone()
        if one_row is not None:
            print('Update {object_id} object.'.format(object_id=object_id), datetime.datetime.now())

            inserted_date = datetime.datetime.now()
            object_id = post.get('id', 'null')
            date = "\'" + post.get('date')[6:10] + post.get('date')[2:6] + post.get('date')[:2] + " " + post.get(
                'date')[11:] + ".0\'"
            channel_id = "\'" + owner_id + "\'"
            likes = post.get('likes', 'null')
            comments = post.get('comments_count', 'null')
            views = post.get('views', 'null')
            reposts = post.get('reposts', 'null')
            caption = ""
            text = "\'" + post.get('text', 'null').replace("'", "''").replace("\n+", " ") + "\'"
            text = text.encode(encoding='UTF-8', errors='strict')
            url_attachment = "\'" + post.get('attachments', 'null') + "\'"
            url_channel = "https://vk.com/" + group_name
            source_id = 1

            emoji_pattern = re.compile("["
                                       u"\U0001F600-\U0001F64F"  # emoticons
                                       u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                       u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                       u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                       "]+", flags=re.UNICODE)

            sql_update = 'update tl_media_data_vk set likes = {likes}, comments = {comments}, reposts={reposts}, views = {views} where object_id= \'{object_id}\' and channel_id=\'\'\'{channel_id}\'\'\''.format(likes=likes, comments=comments, reposts=reposts, views=views, object_id=object_id, channel_id=owner_id)

            sql_history_test = "insert into tl_media_data_vk_history (object_id, inserted_date, published_date, channel_id, likes, comments,views,reposts, caption, text, url_attachment, url_channel, source_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?)"

            cursor.execute(sql_history_test, (
            object_id, inserted_date, date, channel_id, likes, comments, views, reposts, caption, text,
            url_attachment[:255], url_channel, source_id))

            cursor.execute(sql_update)

        else:
            print('Insert ', object_id, 'object.', datetime.datetime.now())

            object_id = post.get('id', 'null')
            date = "\'" + post.get('date')[6:10] + post.get('date')[2:6] + post.get('date')[:2] + " " + post.get(
                'date')[11:] + ".0\'"
            channel_id = "\'" + owner_id + "\'"
            likes = post.get('likes', 'null')
            comments = post.get('comments_count', 'null')
            views = post.get('views', 'null')
            reposts = post.get('reposts', 'null')
            caption = ""
            text = "\'" + post.get('text', 'null').replace("'", "''").replace("\n+", " ") + "\'"
            text = text.encode(encoding='UTF-8', errors='strict')
            url_attachment = "\'" + post.get('attachments', 'null') + "\'"
            url_channel = "https://vk.com/" + group_name
            source_id = 1

            sql_1_test = "insert into tl_media_data_vk (object_id, published_date, channel_id, likes, comments,views,reposts, caption, text, url_attachment, url_channel, source_id) values (?,?,?,?,?,?,?,?,?,?,?,?)"

            cursor.execute(sql_1_test, (
            object_id, date, channel_id, likes, comments, views, reposts, caption, text, url_attachment[:255],
            url_channel, source_id))

        con.commit()

    cursor.close()
    con.close()


def insert_into_db_test_com(comments, owner_id):
    channel_id = "\'" + owner_id + "\'"
    connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
    con = db.connect(connection_text, "", "")
    cursor = con.cursor()
    for comment in comments:
        comment_id = comment['id']
        object_id = comment.get('post_id', 'null')
        cursor.execute('select comment_id from tl_media_comments_vk where comment_id = \'{comment_id}\' and object_id={object_id}'.format(comment_id=comment_id, object_id=object_id))
        one_row = cursor.fetchone()
        # print(comments_id_db)
        if one_row is not None:
            print('Update {comment_id} comment.'.format(comment_id=comment_id), datetime.datetime.now())
            object_id = comment.get('post_id', 'null')
            likes = comment.get('likes', 'null')
            author_name = comment.get('scr_name', 'null').replace("'", "''")
            author_url = comment.get('url', 'null')
            city = comment.get('city', 'null').replace("'", "''")
            bdate = comment.get('bdate', 'null')

            #print(author_name," ",author_url)

            sql_update = "update tl_media_comments_vk set comment_likes = {likes}, author_name =\'{author_name}\', author_url =\'{author_url}\', city =\'{city}\', bdate =\'{bdate}\', channel_id=\'\'{channel_id}\'\' where comment_id=\'{comment_id}\' and object_id={object_id}".format(
                likes=likes, author_name=author_name, author_url=author_url, city=city, bdate=bdate,comment_id=comment_id, channel_id=channel_id, object_id=object_id)
            print(sql_update)
            cursor.execute(sql_update)
        else:
            print('Insert ', comment_id, 'comment.', datetime.datetime.now())

            comment_id = comment.get('id', 'null')
            date = "\'" + comment.get('date')[6:10] + comment.get('date')[2:6] + comment.get('date')[
                                                                                 :2] + " " + comment.get('date')[
                                                                                             11:] + ".0\'"
            object_id = comment.get('post_id', 'null')
            comment_likes = comment.get('likes', 'null')
            comment_text = "\'" + comment.get('text', 'null').replace("'", "\"").replace("\n", " ").replace(",",
                                                                                                            "") + "\'"
            comment_text = comment_text.encode(encoding='UTF-8', errors='strict')
            author_id = comment.get('person_id', 'null')
            author_name = comment.get('scr_name', 'null').replace("'", "''")
            author_url = comment.get('url', 'null')
            city = comment.get('city', 'null').replace("'", "''")
            bdate = comment.get('bdate', 'null')

            source_id = 1
            sql_1_test = "insert into tl_media_comments_vk (comment_id, published_date, object_id, comment_likes, comment_text,author_id,author_name, author_url, source_id, city, bdate, channel_id) values (?,?,?,?,?,?,?,?,?,?,?,?)"
            print(sql_1_test, comment_id, object_id, channel_id)
            cursor.execute(sql_1_test, [comment_id, date, object_id, comment_likes, comment_text, author_id, author_name, author_url, source_id,city, bdate, channel_id])

        con.commit()

    cursor.close()
    con.close()

access_token = 'd2d581fc6393044c9a77799594da4a809a8971d89e65b8c13a6fb8d5779565bd662aede7897f03a05e803'

while True:
    bot_message = "VkAli RUNNING" + "  " + str(datetime.datetime.now())
    telegram_bot_sendtext(bot_message)
    channels_from_db = { '-119976621'}
    print(channels_from_db)
    for owner_id in channels_from_db:
        all_posts, count_posts, parsed_posts_count, group_name = get_new_posts(access_token, owner_id)
        print('All posts count', count_posts, 'and parsed posts', parsed_posts_count)
        pposts = make_posts(all_posts)

#        print(pposts)
        insert_into_db_test(pposts, owner_id, group_name)
        all_comments = []
        for post_id in pposts:
            comments, comments_count, all_profiles = get_new_comments(access_token, owner_id, post_id['id'])
            if post_id['comments_count'] > 0:
                all_comments += make_comments(post_id['id'], comments, all_profiles)
        insert_into_db_test_com(all_comments, owner_id)
#        print(all_comments)

