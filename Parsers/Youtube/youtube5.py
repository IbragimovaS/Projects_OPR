
import os
import sys
import json
import datetime
import pandas as pd
import ibm_db_dbi as db
import time
# this is to import youtube_api from the py directory
sys.path.append(os.path.abspath('../'))
import youtube_api
from youtube_api import youtube_api_utils as utils

connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
con = db.connect(connection_text, "", "")
cursor = con.cursor()

key = 'AIzaSyDT0DXaz02PQBe6QQp5IBCg_2s2i0Mpsxs'
yt = youtube_api.YoutubeDataApi(key)

channel_ids = {
    'UCoVl2rx8GcQIPY3V7o9VIow',
    'UCzvrkx5W8-3JqMsUlpTUGQg',
    'UClmqO1ELiMtzP2YGXhWE_lw',
    'UCBaSaluuKZL9oajCr7iwysg',
    'UCCod_goRPrJmMagRQlqzb-g',
    'UCsp7B7NL6lQc599k8QiNUDA',
    'UCFcUfonLGGT0KZ67zVecxpg',
    'UCYlP4F1zNoliX2oysOJRuYQ',
    'UCM3NppqFwqvmdw9fL_5NDfQ',
    'UCzkSFhAT2mbgFiK0Fn1d7kQ',
    'UCrB1ZCQLUamth2JmBY39pDA',
    'UCMGvY8gyV7S8UuDjQXQzLUA',
    'UCNJgnslpVSGg37EdpoMb6gw',
    'UCfT0yG1oOORVBbfdHxQniyA',
    'UCxgBhtLIInrvEMGB0aHEqjA',
    'UC0ogTjpgVgwtEdb-VgF1Xqw',
    'UCiTE6Eyt-EWAlDhVD51VHdQ',
    'UCPOILb0u86kPJRyxFHXC9Lw',
    'UCkM9lOm_P_1teORlZjiwn6w',
    'UCEU6EyjXF2KCLTHtmOfmXgg',
    'UCSE66snuSP9nZkkdnsmv4Xw',
    'UCBG57608Hukev3d0d-gvLhQ',
    'UCIlLaUQtcaabbw9-uQysRjw',
}
for channel_id in channel_ids:
    channel_meta = yt.get_channel_metadata(channel_id)
    playlist_id = channel_meta['playlist_id_uploads']

    video_ids = yt.get_videos_from_playlist_id(playlist_id, published_after=datetime.datetime.now()-datetime.timedelta(days=1))
    print(video_ids)
    df = pd.DataFrame(video_ids)
    video_ids = df['video_id'].tolist()

    for video_id in video_ids:
        video_meta = yt.get_video_metadata(video_id)

        object_id = video_meta['video_id']
        published_date = datetime.datetime.fromtimestamp(video_meta['video_publish_date'])
        channel_id = video_meta['channel_id']
        likes = video_meta['video_like_count']
        comments = video_meta['video_comment_count']
        views = video_meta['video_view_count']
        text = video_meta['video_description']
        text = text.encode(encoding='UTF-8', errors='strict')
        url_attachment = video_meta['video_thumbnail']
        url_channel = 'https://www.youtube.com/channel/' + video_meta['channel_id']
        source_id = 3


        #print(object_id,'__',published_date,'__',channel_id,'__',likes,'__',comments,'__',views,'__',reposts,'__',caption,'__',text,'__',url_attachment,'__',url_channel,'__',source_id,'__',subtitles_passed)
        cursor.execute('select object_id from tl_media_data_yt where object_id = \'{object_id}\''.format(object_id=object_id))
        one_row = cursor.fetchone()
        if one_row is not None:
            print('Update ', object_id, 'object.', datetime.datetime.now())
            sql_update = 'update tl_media_data_yt set likes = {likes}, comments = {comments}, views = {views} where object_id = \'{object_id}\''.format(likes=likes,comments=comments,views=views,object_id=object_id)
            cursor.execute(sql_update)
        else:
            print('Insert ', object_id, 'object.', datetime.datetime.now())
            inserted_date = datetime.datetime.now()
            sql_1_test = "insert into tl_media_data_yt (object_id,published_date,channel_id,likes,comments,views,text,url_attachment,url_channel,source_id) values (?,?,?,?,?,?,?,?,?,?)"
            cursor.execute(sql_1_test, (object_id,published_date,channel_id,likes,comments,views, text, url_attachment, url_channel, source_id))

        con.commit()
        time.sleep(1)
        try:
            commentslist = yt.get_video_comments(video_id)
            for com in commentslist:
                #print(com)
                comment_id = com['comment_id']
                object_id = object_id
                published_date = datetime.datetime.fromtimestamp(com['comment_publish_date'])
                comment_likes = com['comment_like_count']
                comment_text = com['text']
                comment_text = comment_text.encode(encoding='UTF-8', errors='strict')
                author_id = com['commenter_channel_id']
                author_name = com['commenter_channel_display_name']
                author_name = author_name.encode(encoding='UTF-8', errors='strict')
                author_url = com['commenter_channel_url']
                parent_id = com['comment_parent_id']
                source_id = 3
                print(comment_id,object_id,published_date,comment_likes,comment_text,author_id,author_name,author_url,parent_id,source_id)
                cursor.execute('select object_id from tl_media_comments_yt where comment_id = \'{comment_id}\''.format(comment_id=comment_id))
                one_row = cursor.fetchone()
                if one_row is not None:
                    print('Update ', object_id, 'comment.', datetime.datetime.now())
                    sql_update = 'update tl_media_comments_yt set comment_likes = {comment_likes} where comment_id = \'{comment_id}\''.format(comment_likes=comment_likes, comment_id=comment_id)
                    cursor.execute(sql_update)
                else:
                    print('Insert ', object_id, 'comment.', datetime.datetime.now())
                    inserted_date = datetime.datetime.now()
                    sql_1_test = "insert into tl_media_comments_yt (comment_id, object_id, published_date, comment_likes, comment_text, author_id, author_name, author_url, parent_id, source_id) values (?,?,?,?,?,?,?,?,?,?)"
                    cursor.execute(sql_1_test, (comment_id, object_id, published_date,comment_likes,comment_text,author_id,author_name,author_url,parent_id,source_id))
                time.sleep(0.8)
                con.commit()
        except AttributeError:
            print('Post have ho comments')
cursor.close()
con.close()
cursor.close()
con.close()

