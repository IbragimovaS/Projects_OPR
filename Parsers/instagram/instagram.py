# -*- coding: utf-8 -*-
import datetime
import instaloader
import time
from itertools import dropwhile, takewhile
import requests
import psycopg2
from crontab import CronTab
import sys


def telegram_bot_sendtext(bot_message):
    bot_token = '766790954:AAFgBHzh6FPOpHakusiqNSPQ7z5d_kFJCAg'
    bot_chatID = '91344390'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()


L = instaloader.Instaloader()
# print("enter channel ID: ")
# channel_id = {input()}
channel_id = {sys.argv[1]}#{'1626737751'}
# print("enter from_date: ")
# from_date = input()
# print("enter  to date: ")
# to_date = input()
conn = psycopg2.connect(dbname='postgres', user='postgres', password='Mukanov13', host='localhost')
cur = conn.cursor()
users_cron = CronTab()
# try:
bot_message = "InstaNegative0 Running" + "  " + str(datetime.datetime.now())
telegram_bot_sendtext(bot_message)
for id in channel_id:
    sql_data_update = """ UPDATE tl_media_data_inst SET likes = %s, comments = %s, views = %s, reposts = %s, caption = %s, text = %s  WHERE object_id = %s"""
    print('Downloading posts from ', id)
    profile = instaloader.Profile.from_id(L.context, id)
    print(profile)
    posts = profile.get_posts()
    # SINCE = datetime.datetime.strptime(from_date, '%d.%m.%y')
    # UNTIL = datetime.datetime.strptime(to_date, '%d.%m.%y')
    SINCE = datetime.datetime.strptime(sys.argv[2], '%d.%m.%y') #datetime.datetime.now()
    UNTIL = datetime.datetime.strptime(sys.argv[3], '%d.%m.%y') #SINCE - datetime.timedelta(days=22)
    print(SINCE, '  ', UNTIL)
    for post in takewhile(lambda p: p.date > UNTIL, dropwhile(lambda p: p.date > SINCE, posts)):
        print(post.date)
        # print('instagram.com/p/'+post.shortcode, ' ', post.owner_id, ' ', post.owner_username)
        object_id = str(post.mediaid)
        published_date = post.date_local
        channel_id = str(post.owner_id)
        likes = post.likes
        comments = post.comments
        views = post.video_view_count or 0
        reposts = 0
        caption = 'null'
        text = post.caption
        # text = text.encode(encoding='UTF-8', errors='strict')
        url_attachment = post.url
        shortcode = post.shortcode

        url_channel = 'https://www.instagram.com/p/' + shortcode
        source_id = 1002
        try:
            location = post.location.name
        except:
            location = 'null'
        cur.execute('select id from tl_media_data_inst where object_id=\'{object_id}\''.format(object_id=object_id))
        one_row = cur.fetchone()
        # print(comments_id_db)
        if one_row is not None:
            print('Updated - ', object_id, ' object')
            cur.execute(sql_data_update, (likes, comments, views, reposts, caption, text, object_id))
        else:
            print('Inserted - ', object_id, ' object')
            cur.execute(
                "INSERT INTO tl_media_data_inst(object_id, published_date, channel_id, likes, comments, views, reposts, caption, text, url_attachment, url_channel, source_id, location) " + "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (object_id, published_date, channel_id, likes, comments, views, reposts, caption, text, url_attachment,
                 url_channel, source_id, location))
        conn.commit()

        time.sleep(3)
        comments_t = post.get_comments()
        for comment in comments_t:
            sql_comment_update = """ UPDATE tl_media_comments_inst SET comment_likes = %s, comment_text = %s  WHERE comment_id = %s"""
            comment_id = str(comment.id)
            c_object_id = str(post.mediaid)
            c_published_date = comment.created_at_utc
            comment_text = comment.text
            # comment_text = comment_text.encode(encoding='UTF-8', errors='strict')
            comment_likes = 0
            author_id = str(comment.owner.userid)
            author_name = comment.owner.username
            parent_id = ''
            source_id = 1002
            location = ''
            person_url = 'https://www.instagram.com/' + author_name
            cur.execute(
                'select id from tl_media_comments_inst where comment_id=\'{comment_id}\''.format(comment_id=comment_id))
            one_row = cur.fetchone()
            # print(comments_id_db)
            if one_row is not None:
                print('Updated - ', object_id, ' comment')
                cur.execute(sql_comment_update, (comment_likes, comment_text, comment_id))
            else:
                print('Inserted - ', object_id, ' comment')
                cur.execute(
                    "INSERT INTO tl_media_comments_inst(comment_id, object_id, published_date, comment_likes, comment_text, author_id, author_name, author_url,  parent_id, source_id, location) " + "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (comment_id, c_object_id, c_published_date, comment_likes, comment_text, author_id, author_name,
                     person_url, parent_id, source_id, location))
            conn.commit()

            time.sleep(3)
            gen = comment.answers
            if gen is None:
                time.sleep(3)

            for g in gen:
                sql_reply_update = """ UPDATE tl_media_comments_inst SET comment_likes = %s, comment_text = %s  WHERE comment_id = %s"""
                reply_comment_id = str(g.id)
                reply_object_id = str(post.mediaid)
                reply_published_date = g.created_at_utc
                reply_comment_text = g.text
                # reply_comment_text = reply_comment_text.encode(encoding='UTF-8', errors='strict')
                reply_comment_likes = 0
                reply_author_id = str(g.owner.userid)
                reply_author_name = g.owner.username
                reply_parent_id = str(comment.id)
                reply_source_id = 1002
                reply_location = ''
                reply_person_url = 'https://www.instagram.com/' + reply_author_name
                cur.execute('select id from tl_media_comments_inst where comment_id=\'{comment_id}\''.format(
                    comment_id=reply_comment_id))
                one_row = cur.fetchone()
                # print(comments_id_db)
                if one_row is not None:
                    print('Updated - ', object_id, ' reply')
                    cur.execute(sql_reply_update, (reply_comment_likes, reply_comment_text, reply_comment_id))
                else:
                    print('Inserted - ', object_id, ' reply')
                    cur.execute(
                        "INSERT INTO tl_media_comments_inst(comment_id, object_id, published_date, comment_likes, comment_text, author_id, author_name, author_url, parent_id, source_id, location) " + "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (reply_comment_id, reply_object_id, reply_published_date, reply_comment_likes,
                         reply_comment_text, reply_author_id, reply_author_name, reply_person_url, reply_parent_id,
                         reply_source_id, reply_location))

                conn.commit()
cur.close()
# except:
# 	bot_message = "InstaNegative0 Error " + "  " + str(datetime.datetime.now())
# 	telegram_bot_sendtext(bot_message)
