import time
import datetime
from datetime import datetime as dt
import ibm_db_dbi as db
import pandas
import instaloader
import time
from itertools import dropwhile, takewhile
import requests

def telegram_bot_sendtext(bot_message):
    bot_token = '766790954:AAFgBHzh6FPOpHakusiqNSPQ7z5d_kFJCAg'
    bot_chatID = '91344390'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()

def get_channels():

    connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
    con = db.connect(connection_text, "", "")
    cursor = con.cursor()
    sql = 'select channel_id from tl_media_channels where source_id = 2 order by id limit(25)'
    df = pandas.read_sql(sql, con)

    df['channel_id'] = df['CHANNEL_ID'].str.replace("'", "")
    df1 = df['channel_id'].values.tolist()
    print(df1)
    channels_from_db = []

    for r in df1:
        print(r)
        channels_from_db.append(r)
    return channels_from_db



#username = 'indira_rahimbekova86'
#password = 'r2d2C3po'
# username = 'alina_turdalina'
# password = 'r2d2C3po'

L = instaloader.Instaloader()
#L.login(username, password)

hashtags = {
    #'mynalgys',
    #'thespiritoftengri2019',
    #'spiritoftengri',
    #'thespiritoftengri',
    'salemsocial',
    'назарбаев_выборы_2019',
    'сайлау',
    'sailau',
    'сайлау2019',
}

connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
con = db.connect(connection_text, "", "")
cursor = con.cursor()
try:
    bot_message = "InstaPositiveKeywords Running" +  "  " +  str(datetime.datetime.now())
    telegram_bot_sendtext(bot_message)
    for hashtag in hashtags:

        print('Downloading posts from ', hashtag)
        posts = L.get_hashtag_posts(hashtag=hashtag)

        SINCE = datetime.datetime.now()
        UNTIL = SINCE - datetime.timedelta(days=7)
        print(SINCE, '  ', UNTIL)

        for post in takewhile(lambda p: p.date > UNTIL, dropwhile(lambda p: p.date > SINCE, posts)):
            print(post.date)
            #print('instagram.com/p/'+post.shortcode, ' ', post.owner_id, ' ', post.owner_username)

            object_id = str(post.mediaid)
            published_date = post.date_local
            channel_id = str(post.owner_id)
            likes = post.likes
            comments = post.comments
            views = post.video_view_count or 0
            reposts = 'null'
            caption = 'null'
            text = str(post.caption)
            url_attachment = post.url
            shortcode = post.shortcode

            url_channel = 'https://www.instagram.com/p/' + shortcode
            source_id = 2
            try:
                location = post.location.name
            except:
                location = 'null'
            cursor.execute('select object_id from tl_media_data_inst where object_id = \'{object_id}\''.format(object_id=object_id))
            one_row = cursor.fetchone()
            if one_row is not None:
                print('Update {object_id} object. In '.format(object_id=object_id), datetime.datetime.now())
                inserted_date = datetime.datetime.now()
                sql_update = "update tl_media_data_inst set likes = {likes}, comments = {comments}, views = {views}, location=\'{location}\' where object_id = \'{object_id}\'".format(likes=likes, comments=comments, views=views, caption=caption, location=location, object_id=object_id)
                #print(sql_update)

                sql_history_test = "insert into tl_media_data_inst_history (object_id, inserted_date, published_date, channel_id, likes, comments, caption, text, url_attachment, url_channel, source_id) values (?,?,?,?,?,?,?,?,?,?,?)"
                cursor.execute(sql_history_test, (object_id, inserted_date, published_date, channel_id, likes, comments, caption, text, url_attachment, url_channel, source_id))

                cursor.execute(sql_update)
            else:

                print('Insert ', object_id, 'object.', datetime.datetime.now())
                inserted_date = datetime.datetime.now()
                sql_1_test = "insert into tl_media_data_inst (object_id, published_date, channel_id, likes, comments,views, caption, text, url_attachment, url_channel, source_id, location) values (?,?,?,?,?,?,?,?,?,?,?,?)"
                try:
                    cursor.execute(sql_1_test, (object_id, published_date, channel_id, likes, comments, views, caption, text, url_attachment, url_channel, source_id, location))
                except:
                    print('Missed ', object_id)
            con.commit()
            time.sleep(3)
            comments_t = post.get_comments()
            for comment in comments_t:

                comment_id = str(comment.id)
                c_object_id = str(post.mediaid)
                c_published_date = comment.created_at_utc
                comment_text = comment.text
                comment_likes = 0
                author_id = str(comment.owner.userid)
                author_name = comment.owner.username
                parent_id = ''
                source_id = 2
                location = ''
                person_url = 'https://www.instagram.com/' + author_name


                cursor.execute('select comment_id from tl_media_comments_inst where comment_id = \'{comment_id}\''.format(comment_id=comment_id))
                one_row = cursor.fetchone()

                if one_row is not None:

                    print('Update {comment_id} comment.'.format(comment_id=comment_id), datetime.datetime.now())
                    sql_update = "update tl_media_comments_inst set comment_likes = {likes} where comment_id = \'{comment_id}\'".format(likes=comment_likes, comment_id=comment_id)
                    cursor.execute(sql_update)

                else:

                    print('Insert ', comment_id, 'comment.', datetime.datetime.now())
                    sql_1_test = "insert into tl_media_comments_inst (comment_id, published_date, object_id, comment_likes, comment_text,author_id,author_name, author_url, source_id) values (?,?,?,?,?,?,?,?,?)"
                    try:
                        cursor.execute(sql_1_test, (comment_id, c_published_date, c_object_id, comment_likes, comment_text, author_id, author_name, person_url, source_id))
                    except:
                        print('Missed ', object_id)
                con.commit()

                time.sleep(3)
                gen = comment.answers
                if gen is None:
                    time.sleep(3)

                for g in gen:
                    reply_comment_id = str(g.id)
                    reply_object_id = str(post.mediaid)
                    reply_published_date = g.created_at_utc
                    reply_comment_text = g.text
                    reply_comment_likes = 0
                    reply_author_id = str(g.owner.userid)
                    reply_author_name = g.owner.username
                    reply_parent_id = str(comment.id)
                    reply_source_id = 2
                    reply_location = ''
                    reply_person_url = 'https://www.instagram.com/' + reply_author_name

                    cursor.execute('select comment_id from tl_media_comments_inst where comment_id = \'{comment_id}\''.format(comment_id=reply_comment_id))
                    one_row = cursor.fetchone()

                    if one_row is not None:

                        print('Update reply {comment_id} comment.'.format(comment_id=reply_comment_id),datetime.datetime.now())
                        sql_update = "update tl_media_comments_inst set comment_likes = {likes}, author_url = \'{author_url}\' where comment_id = \'{comment_id}\'".format(likes=reply_comment_likes, author_url=reply_person_url, comment_id=reply_comment_id)
                        cursor.execute(sql_update)

                    else:

                        print('Insert reply', reply_comment_id, 'comment.', datetime.datetime.now())
                        sql_1_test = "insert into tl_media_comments_inst (comment_id, published_date, object_id, comment_likes, comment_text,author_id,author_name, author_url, source_id, parent_id) values (?,?,?,?,?,?,?,?,?,?)"
                        try:
                            cursor.execute(sql_1_test, (reply_comment_id, reply_published_date, reply_object_id, reply_comment_likes, reply_comment_text, reply_author_id, reply_author_name, reply_person_url, reply_source_id, reply_parent_id))
                        except:
                            print('Missed ', object_id)
                    con.commit()

except:
    bot_message = "InstaPositiveKeywords ERROR" +  "  " +  str(datetime.datetime.now())
    telegram_bot_sendtext(bot_message)

cursor.close()
con.close()
