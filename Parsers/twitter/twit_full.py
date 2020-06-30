import ibm_db_dbi as db
import tweepy as tw
import sys
from datetime import datetime, timedelta
import time
import pandas as pd
from twython import Twython
import psycopg2

consumer_key= 'VCwKiz8UHa4GNHrgkHBhPrtL8'
consumer_secret= '32GQN7EFIdaEMuWXZVZX2tsgtgwFjzkOhTbsLYJ01emOa6Zy3n'
access_token= '1126386395709419520-yirS4EPwOnDB7GF3RaCkMF2qtA4gnj'
access_token_secret= 'yHXaTjcKNDORhLpk3Eb06y5PeWx6LgDWgRlAnlIt6uPhk'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)
new_search = "Токаев OR Назарбаев OR Ахметбеков OR Еспаева OR Косанов OR Рахимбеков OR Таспихов OR Тоқаев OR Сади-Бек OR Сәді-Бек  -filter:retweets"

#print(date_since)
#print(current_date.date())

replies = []

non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

conn = psycopg2.connect(dbname='postgres', user='postgres', password='Mukanov13', host='localhost')
cursor = conn.cursor()
search = tw.Cursor(api.search, q=new_search)
items = search.items()
for full_tweets in items:
    query = 'to:' + full_tweets.user.screen_name
    count = 0


    object_id = int(full_tweets.id)
    published_date = full_tweets.created_at.strftime("%Y-%m-%d %H:%M:%S")
    print(published_date)
    channel_id = int(full_tweets.user.id)
    likes = full_tweets.favorite_count
    comments = count
    views = None
    reposts = full_tweets.retweet_count
    caption = None
    text = full_tweets.text.translate(non_bmp_map)
    url_channel = "https://twitter.com/%s/status/%s" % (full_tweets.user.screen_name, full_tweets.id)
    source_id = 4
    img = full_tweets.entities.get('media')
    url_attachment = None

    try:
        url_attachment = img[0]['media_url']
        print('PIKCHAA', url_attachment)
    except:
        url_attachment = 'NULL'
        print('asd')

    print("geo", full_tweets.user.location, "OBJECT_ID", object_id, "PUBLISHED_DATE", published_date, "CHANNEL_ID", channel_id, "LIKES", likes,
          "COMMENTS", count, "VIEWS", "NULL", "REPOSTS", reposts, "CAPTION", "NULL", "TEXT :", text, "URL_ATTACHMENT",
          url_attachment, "URL_CHANNEL", url_channel)

    cursor.execute('select id from tl_media_data_tw3 where object_id = \'{object_id}\''.format(object_id=object_id))
    one_row = cursor.fetchone()

    #print(one_row)
    if one_row is None:
        sql = "insert into tl_media_data_tw3 (object_id, published_date, channel_id, likes, comments, views, reposts, caption, text, url_attachment, url_channel, source_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        cursor.execute(sql, (object_id, published_date, channel_id, likes, comments, views, reposts, caption, text, url_attachment, url_channel, source_id))

    conn.commit()
cursor.close()
conn.close()




