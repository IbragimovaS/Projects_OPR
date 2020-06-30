import ibm_db_dbi as db
import tweepy as tw
import sys
from datetime import datetime, timedelta
import psycopg2

consumer_key= 'VCwKiz8UHa4GNHrgkHBhPrtL8'
consumer_secret= '32GQN7EFIdaEMuWXZVZX2tsgtgwFjzkOhTbsLYJ01emOa6Zy3n'
access_token= '1126386395709419520-yirS4EPwOnDB7GF3RaCkMF2qtA4gnj'
access_token_secret= 'yHXaTjcKNDORhLpk3Eb06y5PeWx6LgDWgRlAnlIt6uPhk'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)
new_search = {
    "АЭС OR ЦИК OR Выборы OR Власть OR Китай OR Россия OR Переименование OR Уйгур OR Малоимущие OR Многодетные -filter:retweets",
    "Оппозиция OR Взятки OR Народ OR Голосовать OR Авторитарный OR интернет-ресурс OR сайт OR задержан -filter:retweets",
    "соцсет OR блокировка OR доступ OR интернет OR сми OR протест OR Коррупция OR Диктатор -filter:retweets",
    "Митинг OR Революция OR Тюльпановая OR Религиозный OR конфликт OR Экстремисты OR Экстремизм OR Террористы OR Несправедливость -filter:retweets"
}
current_date = datetime.now() - timedelta(days=30)

new_date = current_date.date() - timedelta(days=180)

date_since = new_date
#print(date_since)
#print(current_date.date())

replies = []

non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

conn = psycopg2.connect(dbname='postgres', user='postgres', password='Mukanov13', host='localhost')
cursor = conn.cursor()

for search in new_search:
    for full_tweets in tw.Cursor(api.search, q=search, since=date_since, geocode="48.40550,76.55273,639km").items():
        query = 'to:' + full_tweets.user.screen_name
        count = 0

        for twit in tw.Cursor(api.search, q=query,  geocode="48.40550,76.55273,637km").items():
           if hasattr(twit, 'in_reply_to_status_id_str'):
               if (twit.in_reply_to_status_id_str == full_tweets.id_str):
                   replies.append(twit.text)
                   count = count + 1
                   # for elements in replies:
                   #    print("Replies :", elements)
                   # replies.clear()

        object_id = str(full_tweets.id)
        published_date = (full_tweets.created_at+timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")
        print(published_date)
        channel_id = str(full_tweets.user.id)
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
        location = full_tweets.user.location

        try:
            url_attachment = img[0]['media_url']
            # print('PIKCHAA', url_attachment)
        except:
            url_attachment = 'NULL'
            # print('asd')

        print("geo", full_tweets.user.location, "OBJECT_ID", object_id, "PUBLISHED_DATE", published_date, "CHANNEL_ID", channel_id, "LIKES", likes,
              "COMMENTS", count, "VIEWS", "NULL", "REPOSTS", reposts, "CAPTION", "NULL", "TEXT :", text, "URL_ATTACHMENT",
              url_attachment, "URL_CHANNEL", url_channel)

        cursor.execute('select id from tl_media_data_tw3 where object_id = \'{object_id}\''.format(object_id=object_id))
        one_row = cursor.fetchone()

        #print(one_row)
        if one_row is None :
            sql = "insert into tl_media_data_tw3 (object_id, published_date, channel_id, likes, comments, views, reposts, caption, text, url_attachment, url_channel, location, source_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            cursor.execute(sql, (object_id, published_date, channel_id, likes, comments, views, reposts, caption, text, url_attachment, url_channel, location, source_id))

        conn.commit()
cursor.close()
conn.close()





